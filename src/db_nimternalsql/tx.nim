#
# Copyright (c) 2020 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
#
import strutils
import nqcommon
import nqcore
import snapshot
import tables
import os
import posix

type
  TxLogEntryKind = enum tleInsert, tleDelete, tleUpdate, tleCreate, tleDrop
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
      of tleCreate, tleDrop:
        discard
  Tx* = ref object
    ## A transaction object which can be used seially to begin and commit
    ## multiple transactions. Transactions are started implicitly.
    log: seq[TxLogEntry]
    logDir*: string
    logFile: File

proc newTx*(logPath: string, file: File): Tx =
  var tx: Tx 
  new(tx)
  tx.logDir = logPath
  tx.log = @[]
  tx.logFile = file
  return tx

proc createBaseTable*(tx: Tx, db: Database, name: string,
                      columns: openArray[ColumnDef],
                      key: seq[string]): BaseTable =
  if columns.len < 1:
    raiseDbError("table must have at least one column")
  result = newHashBaseTable(name, columns, key)
  for i in 0..<result.def.len:
    checkType(result.def[i])

    if i < result.def.len - 1:
      for j in i + 1..<result.def.len:
        if result.def[i].name == result.def[j].name:
          raiseDbError("error: column \"" & result.def[i].name & "\" specified more than once")
    if result.def[i].typ == "TIMESTAMP":
      let tsMaxPrecision = 6
      if result.def[i].precision > tsMaxPrecision:
        raiseDbError("max precision of TIMESTAMP is " & $tsMaxPrecision)
    else:
      if result.def[i].precision > maxPrecision:
        raiseDbError("max precision is " & $maxPrecision)
    if result.def[i].typ == "DECIMAL":
      result.def[i].precision = maxPrecision
    # Check default value
    if columns[i].defaultValue != nil:
      discard toMatValue(eval(columns[i].defaultValue, nil, nil), columns[i])
  for i in result.primaryKey:
    result.def[i].notNull = true
  if db.tables.hasKeyOrPut(name, result):
    raiseDbError("table \"" & name & "\" already exists")
  tx.log.add(TxLogEntry(kind: tleCreate, table: result))

proc dropBaseTable*(tx: Tx, db: Database, name: string) =
  if not db.tables.hasKey(name):
    raiseDbError("table \"" & name & "\" does not exist")
  let rtable = db.tables[name]
  db.tables.del(name)
  tx.log.add(TxLogEntry(kind: tleDrop, table: rtable))

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
  htable.rows[keyRec] = valRec
  tx.log.add(TxLogEntry(kind: tleInsert,
                        table: htable,
                        keyRec: keyRec,
                        valRec: valRec))

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
      tx.log.add(TxLogEntry(kind: tleDelete,
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

proc writeRedo(f: File, logEntry: TxLogEntry) =
  case logEntry.kind:
    of tleInsert:
      write(f, 'I')
      writeName(f, logEntry.table.name)
      writeRecord(f, logEntry.keyRec)
      writeRecord(f, logEntry.valRec)
    of tleDelete:
      write(f, 'D')
      writeName(f, logEntry.table.name)
      writeRecord(f, logEntry.keyRec)
    of tleUpdate:
      write(f, 'U')
      writeName(f, logEntry.table.name)
      writeRecord(f, logEntry.keyRec)
      var intBuf = int32(logEntry.assignments.len)
      if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
        raiseIoError(writeError)
      for a in logEntry.assignments:
        intBuf = int32(a.col)
        if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
          raiseIoError(writeError)
        writeValue(f, a.newVal)
    of tleCreate, tleDrop:
      write(f, if logEntry.kind == tleCreate: 'C' else: 'R')
      writeTableDef(f, logEntry.table)

proc commit*(tx: Tx) =
  if tx.logFile != nil:
    for e in tx.log:
      writeRedo(tx.logFile, e)
    tx.logFile.flushFile
    if fsync(tx.logFile.getOsFileHandle) != 0:
      raiseDbError("fsync() failed")
  tx.log = @[]

func isKeyUpdate(table: BaseTable,
                 assignments: seq[TxLogAssignment]): bool =
  for a in assignments:
    for k in table.primaryKey:
      if a.col == k:
        return true
  result = false

proc rollback*(tx: Tx, db: Database) =
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
      of tleCreate:
        db.tables.del(tx.log[i].table.name)
      of tleDrop:
        db.tables[tx.log[i].table.name] = HashBaseTable(tx.log[i].table)
  tx.log = @[]

proc replayLog(f: File, db: Database) =
  var c: array[0..0, char]
  while readChars(f, c, 0, 1) == 1:
    case c[0]:
      of 'I':
        let tableName = readName(f)
        let keyRec = readRecord(f)
        let valRec = readRecord(f)
        HashBaseTable(getTable(db, tableName)).rows[keyRec] = valRec
      of 'D':
        let tableName = readName(f)
        let keyRec = readRecord(f)
        HashBaseTable(getTable(db, tableName)).rows.del(keyRec)
      of 'U':
        let tableName = readName(f)
        let keyRec = readRecord(f)
        var lenBuf: int32
        if readBuffer(f, addr(lenBuf), sizeof(int32)) < sizeof(int32):
          raiseDbError(readErrorMissingData)
        for i in 0..<lenBuf:
          var colBuf: int32
          if readBuffer(f, addr(colBuf), sizeof(int32)) < sizeof(int32):
            raiseDbError(readErrorMissingData)
          let v = readValue(f)
          HashBaseTable(getTable(db, tableName)).rows[keyRec][colBuf] = v
      of 'C':
        var table: BaseTable
        new(table)
        readTableDef(f, table)
        db.tables[table.name] = HashBaseTable(name: table.name,
                                              def: table.def,
                                              primaryKey: table.primaryKey)
      of 'R':
        var table: BaseTable
        new(table)
        readTableDef(f, table)
        db.tables.del(table.name)
      else:
        raiseDbError("invalid log entry")

proc openLog*(logdir: string, db: Database): File =
  if not dirExists(logDir):
    if fileExists(logDir):
      raiseDbError(logDir & " is not a directory")
    createDir(logDir)
  if fileExists(logdir & DirSep & defaultDumpName):
    restore(db, logdir & DirSep & defaultDumpName)
  let logNo = 0
  var logFile: File
  if open(logFile, logdir & DirSep & $logNo & ".txlog", fmRead):
    replayLog(logFile, db)
    close(logFile)    
  if not open(result, logdir & DirSep & $logNo & ".txlog", fmAppend):
    raiseIoError("cannot open transaction log")

proc closeLog*(tx: Tx) =
  if tx.logFile != nil:
    close(tx.logFile)

proc logIsActive*(tx: Tx): bool =
  result = tx.logFile != nil

proc truncateLog*(tx: Tx) =
  discard ftruncate(tx.logFile.getOsFileHandle, 0)
