import nqcommon
import nqtables
import db_common
import strutils
import tables
import sequtils

type
  JoinTable* = ref object of VTable
    children: array[2, VTable]
    exp*: Expression
    leftOuter*: bool
  JoinTableCursor = ref object of Cursor
    childCursors: array[2, Cursor]
    table: JoinTable
    advanceLeft: bool
    row: InstantRow
    keyCols: seq[tuple[colNo: Natural, exp: Expression]]
    keyTableNo: int   # of child table accessed by key, if keyCols.len > 0
    rightMatch: bool # true if a row from the right was returned
                     # which matched the current row of the left table

func newJoinTable*(lchild: VTable, rchild: VTable,
                    leftOuter: bool = false,
                    exp: Expression = nil): VTable =
  result = JoinTable(children: [lchild, rchild],
                    leftOuter: leftOuter, exp: exp)

method columnCount(table: JoinTable): Natural =
  result = columnCount(table.children[0]) + columnCount(table.children[1])

method columnNo(table: JoinTable, name: string, tableName: string): int =
  let col1 = table.children[0].columnNo(name, tableName)
  let col2 = table.children[1].columnNo(name, tableName);

  if col1 == -1 and col2 == -1:
    return -1;
  if col1 != -1 and col2 != -1:
    raiseDbError("column reference \"" & (if tableName != "": tableName & "." else: "") &
        name & "\" is ambiguous", columnRefAmbiguous)
  result = if col1 != -1: col1 else: columnCount(table.children[0]) + col2

func join(row1: InstantRow, row2: InstantRow, table: VTable): InstantRow =
  var vals = seq[NqValue](@[])
  for i in 0..<columnCount(row1):
    vals.add(columnValueAt(row1, i))
  for i in 0..<columnCount(row2):
    vals.add(columnValueAt(row2, i))
  result = newInstantRow(table, vals)

method newCursor(rtable: JoinTable, args: openArray[string]): Cursor =
  if rtable.exp != nil:
    for i in (if rtable.leftOuter: 1 else: 0)..1:
      if rtable.children[i] of BaseTableRef:
        let keyCols = expKeyCols(rtable.exp, BaseTableRef(rtable.children[i]),
            proc(exp: Expression): bool =
              if isConst(exp):
                return true
              if not (exp of QVarExp):
                return false
              let colRef = QVarExp(exp)
              let thisColNo = columnNo(rtable.children[i], colRef.name, colRef.tableName)
              let otherColNo = columnNo(rtable.children[1 - i], colRef.name, colRef.tableName)
              if otherColNo != -1:
                if thisColNo != -1:
                      raiseDbError("column reference \"" &
                          (if colRef.tableName != "": colRef.tableName & "." else: "") &
                          colRef.name & "\" is ambiguous", columnRefAmbiguous)
                result = true
              else:
                result = false,
            args)
        if keyCols.len == BaseTableRef(rtable.children[i]).table.primaryKey.len:
          return JoinTableCursor(args: @args,
                                  childCursors:
                                      if i == 0: [nil, newCursor(rtable.children[1], args)]
                                      else: [newCursor(rtable.children[0], args), nil],
                                  table: rtable,
                                  advanceLeft: true,
                                  keyCols: keyCols,
                                  keyTableNo: i)
  result = JoinTableCursor(args: @args,
                          childCursors: [newCursor(rtable.children[0], args),
                                         newCursor(rtable.children[1], args)],
                          table: rtable,
                          advanceLeft: true)

func evalJoinCond(cursor: JoinTableCursor, row: InstantRow): bool =
  result = cursor.table.exp == nil or
      eval(cursor.table.exp, proc(name: string, rangeVar: string): NqValue =
          if name[0] == '$':
            return NqValue(kind: nqkString,
                           strVal: cursor.args[parseInt(name[1..name.high]) - 1])
          let col = columnNo(cursor.table, name, rangeVar)
          if col == -1:
            raiseDbError("column " &
                        (if rangeVar != "": rangeVar & "." else: "") & name & " does not exist",
                        undefinedColumnName)
          return columnValueAt(row, col)).boolVal

proc nextByKey(cursor: JoinTableCursor, row: var InstantRow, varResolver: VarResolver): bool =
  while cursor.childCursors[1 - cursor.keyTableNo].next(cursor.row, varResolver):
    var key: Record[MatValue]
    var val: NqValue
    for c in cursor.keyCols:
      if c.exp of QVarExp:
        let colRef = QVarExp(c.exp)
        if colRef.name[0] == '$':
          val = NqValue(kind: nqkString,
              strVal: cursor.args[parseInt(colRef.name[1..colRef.name.high]) - 1])
        else:
          let col = columnNo(cursor.table.children[1 - cursor.keyTableNo],
                             colRef.name, colRef.tableName)
          val = columnValueAt(cursor.row, col)
      else:
        val = eval(c.exp, varResolver, nil)
      key.add(toMatValue(val,
          BaseTableRef(cursor.table.children[cursor.keyTableNo]).table.def[c.colNo]))
    if HashBaseTable(BaseTableRef(cursor.table.children[cursor.keyTableNo]).table)
          .rows.hasKey(key):
      row = if cursor.keyTableNo == 0:
          join(newMatInstantRow(cursor.table.children[cursor.keyTableNo], key),
               cursor.row, cursor.table)
      else:
          join(cursor.row,
               newMatInstantRow(cursor.table.children[cursor.keyTableNo], key),
               cursor.table)
      return true
    elif cursor.table.leftOuter:
      row = join(cursor.row,
                 newInstantRow(cursor.table.children[1], 
                               repeat(NqValue(kind: nqkNull),
                                      cursor.table.children[1].columnCount)),
                 cursor.table)
      return true
  result = false

method next(cursor: JoinTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  if cursor.keyCols.len > 0:
    return nextByKey(cursor, row, varResolver)
  var rrow: InstantRow
  while true:
    if cursor.advanceLeft:
      cursor.advanceLeft = false
      if not cursor.childCursors[0].next(cursor.row, varResolver):
        return false
      cursor.rightMatch = false
    if cursor.childCursors[1].next(rrow):
      row = join(cursor.row, rrow, cursor.table)
      if evalJoinCond(cursor, row):
        cursor.rightMatch = true
        return true
      else:
        continue
    if cursor.table.leftOuter and not cursor.rightMatch:
      row = join(cursor.row,
                 newInstantRow(cursor.table.children[1],
                               repeat(NqValue(kind: nqkNull),
                                      cursor.table.children[1].columnCount)),
                 cursor.table)
      cursor.advanceLeft = true
      return true
    if not cursor.childCursors[0].next(cursor.row, varResolver):
      return false
    cursor.childCursors[1] = newCursor(cursor.table.children[1], cursor.args)
    cursor.rightMatch = false
    if cursor.childCursors[1].next(rrow, varResolver):
      row = join(cursor.row, rrow, cursor.table)
      if evalJoinCond(cursor, row):
        cursor.rightMatch = true
        return true
  result = false

method `$`(vtable: JoinTable): string =
  result = '(' & $vtable.children[0] & ')' & " JOIN " & '(' & $vtable.children[1] & ')'
  if vtable.exp != nil:
    result = result & " ON " & $vtable.exp

method getColumns*(table: JoinTable): DbColumns =
  for i in 0..1:
    var cols = table.children[i].getColumns()
    for j in 0..<cols.len:
      cols[j].primaryKey = false
      result.add(cols[j])

