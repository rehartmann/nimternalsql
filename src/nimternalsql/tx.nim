#
# Copyright (c) 2020 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
#
import strutils
import nqcommon
import nqcore
import tables

type
  TxLogEntryKind = enum tleInsert, tleDelete, tleUpdate
  TxLogAssignment = object
    col: Natural
    oldVal: MatValue
    newVal: MatValue
  TxLogEntry = object
    table: BaseTable
    keyRec: Record[MatValue]
    case kind: TxLogEntryKind
      of tleInsert, tleDelete:
        valRec: Record[MatValue]
      of tleUpdate:
        assignments: seq[TxLogAssignment]
  Tx* = ref object
    ## A transaction object which can be used seially to begin and commit
    ## multiple transactions. Transactions are started implicitly.
    log: seq[TxLogEntry]

proc newTx*(): Tx =
  var tx: Tx 
  new(tx)
  tx.log = @[]
  return tx

proc insert*(tx: Tx, table: BaseTable, values: seq[NqValue]) =
  if values.len != table.def.len:
    raiseDbError("invalid # of values")

  let htable = HashBaseTable(table)
  var keyRec: Record[MatValue]
  var valRec: Record[MatValue]
  for i in 0..<values.len:
    if isKey(htable, i):
      keyRec.add(toMatValue(values[i], table.def[i]))
    else:
      valRec.add(toMatValue(values[i], table.def[i]))
  if htable.rows.hasKey(keyRec):
    raiseDbError("duplicate key")
  tx.log.add(TxLogEntry(kind: tleInsert,
                        table: htable,
                        keyRec: keyRec,
                        valRec: valRec))
  htable.rows[keyRec] = valRec

proc delete*(tx: Tx, table: BaseTable, whereExp: Expression, args: openArray[
    string]): int64 =
  var vtable: VTable = BaseTableRef(table: table, rangeVar: "")
  if whereExp == nil:
    result = len(HashBaseTable(table).rows)
    clear(HashBaseTable(table).rows)
  else:
    vtable = WhereTable(child: vtable, whereExp: whereExp)
    let cursor = newCursor(vtable, args)
    var row: InstantRow
    var keys: seq[Record[MatValue]]
    while cursor.next(row):
      keys.add(row.keyRecord)
    for key in keys:
      tx.log.add(TxLogEntry(kind:tleDelete,
                            table: table,
                            keyRec: key,
                            valRec: HashBaseTable(table).rows[key]))
      HashBaseTable(table).rows.del(key)
      result += 1

proc update*(tx: Tx, table: BaseTable, assignments: seq[ColumnAssignment],
             whereExp: Expression, args: openArray[string]): int64 =
  var vtable: VTable = BaseTableRef(table: table, rangeVar: "")
  if whereExp != nil:
    vtable = WhereTable(child: vtable, whereExp: whereExp)
  let cursor = newCursor(vtable, args)
  let evargs = @args
  var row: InstantRow
  var txas: seq[TxLogAssignment]
  if isKeyUpdate(table, assignments):
    var keys: seq[Record[MatValue]]
    while cursor.next(row):
      keys.add(row.keyRecord)
    for key in keys:
      row = InstantRow(table: BaseTableRef(table: table, rangeVar: ""),
                       material: true,
                       keyRecord: key)
      # Update non-key columns
      for assignment in assignments:
        if not isKey(table, assignment.col):
          let val = eval(assignment.src, proc(name: string,
              rangeVar: string): NqValue =
            if name[0] == '$':
              return NqValue(kind: nqkString,
                             strVal: evargs[parseInt(name[1..name.high]) - 1])
            let col = columnNo(table, name)
            if col == -1:
              raiseDbError("column \"" & name & "\" does not exist")
            return columnValueAt(row, col))
          txas.add(TxLogAssignment(col: assignment.col,
              oldVal: toMatValue(columnValueAt(row, assignment.col),
                                 table.def[assignment.col]),
              newVal: toMatValue(val, table.def[assignment.col])))
          setColumnValueAt(row, assignment.col, val)
      let valRec = HashBaseTable(table).rows[key]
      # Update key columns
      for assignment in assignments:
        if isKey(table, assignment.col):
          let val = eval(assignment.src, proc(name: string,
              rangeVar: string): NqValue =
            if name[0] == '$':
              return NqValue(kind: nqkString,
                              strVal: evargs[parseInt(name[1..name.high]) - 1])
            let col = columnNo(table, name)
            if col == -1:
              raiseDbError("column \"" & name & "\" does not exist")
            return columnValueAt(row, col))
          txas.add(TxLogAssignment(col: assignment.col,
              oldVal: toMatValue(columnValueAt(row, assignment.col),
                                 table.def[assignment.col]),
              newVal: toMatValue(val, table.def[assignment.col])))
          setColumnValueAt(row, assignment.col, val)
      # Delete old key and value
      HashBaseTable(table).rows.del(key)
      # Insert new values
      HashBaseTable(table).rows[row.keyRecord] = valRec
      tx.log.add(TxLogEntry(kind:tleUpdate,
                            table: table,
                            keyRec: row.keyRecord,
                            assignments: txas))
      result += 1
  else:
    while cursor.next(row):
      for assignment in assignments:
        let val = eval(assignment.src, proc(name: string,
            rangeVar: string): NqValue =
          if name[0] == '$':
            return NqValue(kind: nqkString,
                              strVal: evargs[parseInt(name[1..name.high]) - 1])
          let col = columnNo(table, name)
          if col == -1:
            raiseDbError("column \"" & name & "\" does not exist")
          return columnValueAt(row, col))
        txas.add(TxLogAssignment(col: assignment.col,
            oldVal: toMatValue(columnValueAt(row, assignment.col),
                               table.def[assignment.col]),
            newVal: toMatValue(val, table.def[assignment.col])))
        setColumnValueAt(row, assignment.col, val)
      tx.log.add(TxLogEntry(kind: tleUpdate,
                            table: table,
                            keyRec: row.keyRecord,
                            assignments: txas))
      result += 1

proc commit*(tx: Tx) =
  tx.log = @[]

func isKeyUpdate(table: BaseTable,
                  assignments: seq[TxLogAssignment]): bool =
  for a in assignments:
    for k in table.primaryKey:
      if a.col == k:
        return true
  result = false

proc rollback*(tx: Tx) =
  for i in countdown(tx.log.len - 1, 0):
    case tx.log[i].kind:
      of tleInsert:
        HashBaseTable(tx.log[i].table).rows.del(tx.log[i].keyRec)
      of tleDelete:
        HashBaseTable(tx.log[i].table).rows[tx.log[i].keyRec] = tx.log[i].valRec
      of tleUpdate:
        if isKeyUpdate(tx.log[i].table, tx.log[i].assignments):
          var newKey = tx.log[i].keyRec
          var vals = HashBaseTable(tx.log[i].table).rows[tx.log[i].keyRec]
          for a in tx.log[i].assignments:
            var ki = keyIndex(tx.log[i].table, a.col)
            if ki != -1:
              newKey[ki] = a.oldVal
            else:
              var vi = 0
              for i in 0..<tx.log[i].table.def.len:
                if not isKey(tx.log[i].table, i):
                  if i == a.col:
                    vals[vi] = a.oldVal
                    break
                  vi += 1
          HashBaseTable(tx.log[i].table).rows.del(tx.log[i].keyRec)
          HashBaseTable(tx.log[i].table).rows[newKey] = vals
        else:
          for a in tx.log[i].assignments:
            setColumnValueAt(HashBaseTable(tx.log[i].table), tx.log[i].keyRec,
                             a.col, a.oldVal)
  tx.log = @[]
