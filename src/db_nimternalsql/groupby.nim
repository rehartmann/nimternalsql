import nqcommon
import nqtables
import db_common
import tables

type
  GroupTable = ref object of VTable
    child: VTable
    groupBy: seq[QVarExp]
    columns: seq[SelectElement]
  GroupTableCursor = ref object of Cursor
    iter: iterator(groupTable: Table[Record[NqValue], Record[
        NqValue]]): Record[NqValue]
    groupTable: Table[Record[NqValue], Record[NqValue]]
    table: GroupTable
    rowsRead: bool

method columnCount(table: GroupTable): Natural =
  result = table.columns.len

method columnNo(table: GroupTable, name: string, tableName: string): int =
  for i in 0..<table.columns.len:
    if table.columns[i].colName == name:
      return i
  result = -1

method getColumns*(table: GroupTable): DbColumns =
  let childcols = table.child.getColumns()
  for sel in table.columns:
    if sel.exp of QVarExp:
      let i = columnNo(table.child, QVarExp(sel.exp))
      result.add(childcols[i])
    else:
      var coltyp: DbType
      coltyp.kind = dbUnknown
      if sel.exp of ScalarOpExp:
        case ScalarOpExp(sel.exp).opName
          of "COUNT":
            coltyp = DbType(kind: dbInt, notNull: false, name: "INTEGER",
                  size: sizeof(int32), maxReprLen: 10, precision: 9,
                  scale: 0, min: int32.low, max: int32.high, validValues: @[])
            result.add(DbColumn(
                name: sel.colName, tableName: "", typ: coltyp,
                primaryKey: false, foreignKey: false))
          of "AVG":
            coltyp = DbType(kind: dbFloat, notNull: false, name: "REAL",
                  size: sizeof(float64), maxReprLen: 19, precision: 0,
                  scale: 0, min: int64.low, max: int64.high, validValues: @[])
            result.add(DbColumn(
                name: sel.colName, tableName: "", typ: coltyp,
                primaryKey: false, foreignKey: false))
          of "MIN", "MAX", "SUM":
            if ScalarOpExp(sel.exp).args.len == 1 and
               ScalarOpExp(sel.exp).args[0] of QVarExp:
              # i = columnIndex(childcols, QVarExp(ScalarOpExp(sel.exp).args[0]).name)
              let i = columnNo(table.child, QVarExp(ScalarOpExp(sel.exp).args[0]))
              if i >= 0:
                coltyp = childcols[i].typ
                result.add(DbColumn(name: sel.colName, tableName: "", typ: coltyp))
      if coltyp.kind == dbUnknown:
        result.add(DbColumn(
            name: sel.colName, tableName: "",
            typ: DbType(kind: dbUnknown, notNull: false, name: "",
                        size: 0, maxReprLen: 0, precision: 0,
                        scale: 0, min: 0, max: 0, validValues: @[]),
                primaryKey: false, foreignKey: false))

func isAggregate(exp: Expression): bool =
  if exp of ScalarOpExp:
    case ScalarOpExp(exp).opName
      of "COUNT", "AVG", "MAX", "MIN", "SUM":
        result = true
      else:
        result = false
  else:
    result = false


func newGroupTable*(table: VTable, aggrs: seq[Expression], groupBy: seq[
    QVarExp]): VTable =
  for colRef in groupBy:
    if columnNo(table, colRef) == -1:
      raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
          "." else: "") & colRef.name & " does not exist", undefinedColumnName)
  if table of ProjectTable:
    # Each column must either be in GROUP BY or an aggregation
    for col in ProjectTable(table).columns:
      if not isAggregate(col.exp):
        var found = false
        for g in groupBy:
          if g.name == col.colName:
            found = true
            break
        if not found:
          raiseDbError(col.colName & " is not a grouping column", invalidGrouping)    
    result = GroupTable(child: ProjectTable(table).child,
                       groupBy: groupBy, columns: ProjectTable(table).columns)
  else:
    # Each column must be in GROUP BY
    var cols: seq[SelectElement]
    for dbcol in table.getColumns:
      var found = false
      for g in groupBy:
        if g.name == dbcol.name:
          found = true
          break
      if not found:
        raiseDbError(dbcol.name & " is not a grouping column", invalidGrouping)
      cols.add(SelectElement(colName: dbcol.name, exp: newQVarExp(dbcol.name)))
    result = GroupTable(child: table, groupBy: groupBy, columns: cols)

func mapColRef(colRef: QVarExp, columns: seq[SelectElement]): QVarExp =
  for sel in columns:
    if sel.exp of QVarExp and sel.colName == colRef.name and
        (colRef.tableName == "" or colRef.tableName == QVarExp(
            sel.exp).tableName):
      return QVarExp(sel.exp)
  result = colRef

func groupVals(groupBy: seq[QVarExp], columns: seq[SelectElement],
               row: InstantRow): Record[NqValue] =
  for colRef in groupBy:
    result.add(columnValueAt(row, columnNo(row.table, mapColRef(colRef, columns))))

proc aggrCount(table: VTable, groupBy: seq[QVarExp],
               columns: seq[SelectElement],
               groupByVals: Record[NqValue],
               args: openArray[string]): NqValue =
  var cnt = 0
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      cnt += 1
  result = NqValue(kind: nqkNumeric, numericVal: cnt)

proc aggrMax(table: VTable, groupBy: seq[QVarExp],
             columns: seq[SelectElement],
             groupByVals: Record[NqValue],
             colRef: QVarExp,
             args: openArray[string]): NqValue =
  let col = columnNo(table, colRef)
  if col == -1:
    raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
        "." else: "") & colRef.name & " does not exist", undefinedColumnName)
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt and
            val.kind != nqkString:
          raiseDbError("column type is not supported by MAX", undefinedFunction)
        if result.kind == nqkNull:
          result = val
        elif result.kind == nqkInt:
          result = NqValue(kind: nqkInt,
                           intVal: max(result.intVal, val.intVal))
        elif result.kind == nqkNumeric:
          let scale = max(result.scale, val.scale)
          result = NqValue(kind: nqkNumeric,
                           numericVal: max(result.setScale(scale).numericVal,
                                       val.setScale(scale).numericVal))
        elif result.kind == nqkString:
          result = NqValue(kind: nqkString,
                              strVal: max(result.strVal, val.strVal))
        else:
          result = NqValue(kind: nqkFloat, floatVal: max(result.floatVal, val.floatVal))

proc aggrMin(table: VTable, groupBy: seq[QVarExp],
             columns: seq[SelectElement],
             groupByVals: Record[NqValue],
             colRef: QVarExp,
             args: openArray[string]): NqValue =
  let col = columnNo(table, colRef)
  if col == -1:
    raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
        "." else: "") & colRef.name & " does not exist", undefinedColumnName)
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt and
            val.kind != nqkString:
          raiseDbError("column type is not supported by MIN", undefinedFunction)
        if result.kind == nqkNull:
          result = val
        elif result.kind == nqkInt:
          result = NqValue(kind: nqkInt,
                              intVal: max(result.intVal,
                                          val.intVal))
        elif result.kind == nqkNumeric:
          let scale = max(result.scale, val.scale)
          result = NqValue(kind: nqkNumeric,
                              numericVal: min(result.setScale(scale).numericVal,
                                          val.setScale(scale).numericVal))
        elif result.kind == nqkString:
          result = NqValue(kind: nqkString,
                              strVal: min(result.strVal, val.strVal))
        else:
          result = NqValue(kind: nqkFloat, floatVal: max(result.floatVal, val.floatVal))

proc aggrSum(table: VTable, groupBy: seq[QVarExp],
             columns: seq[SelectElement],
             groupByVals: Record[NqValue],
             colRef: QVarExp,
             args: openArray[string]): NqValue =
  let col = columnNo(table, colRef)
  if col == -1:
    raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
        "." else: "") & colRef.name & " does not exist", undefinedColumnName)
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric", undefinedFunction)
        if result.kind == nqkNull:
          result = val
        else:
          result = result + val

proc aggrAvg(table: VTable, groupBy: seq[QVarExp],
             columns: seq[SelectElement],
             groupByVals: Record[NqValue],
             colRef: QVarExp,
             args: openArray[string]): NqValue =
  let col = columnNo(table, colRef)
  if col == -1:
    raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
        "." else: "") & colRef.name & " does not exist", undefinedColumnName)
  var n: int64 = 0
  var avg: float
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric", undefinedFunction)
        n += 1
        avg += (toFloat(val) - avg) / float(n)
  result = if n == 0: NqValue(kind: nqkNull)
           else: NqValue(kind: nqkFloat, floatVal: avg)

iterator groupKeys(groupTable: Table[Record[NqValue], Record[NqValue]]
                   ): Record[NqValue] {.closure.} =
  for k in groupTable.keys:
    yield k

method newCursor(rtable: GroupTable, args: openArray[string]): Cursor =
  var groupTable: Table[Record[NqValue], Record[NqValue]] =
    initTable[Record[NqValue], Record[NqValue]]()
  let cursor = newCursor(rtable.child, args)
  var row: InstantRow
  while cursor.next(row):
    var key: Record[NqValue]
    for colRef in rtable.groupBy:
      let col = columnNo(rtable.child, mapColRef(colRef, rtable.columns))
      if col == -1:
        raiseDbError("column " & (if colRef.tableName != "": colRef.tableName & "." else: "") &
                     colRef.name & " does not exist", undefinedColumnName)
      key.add(columnValueAt(row, col))
    if not groupTable.hasKey(key):
      groupTable[key] = Record[NqValue](@[])
  result = GroupTableCursor(table: rtable, iter: groupKeys,
                            groupTable: groupTable, args: @args)

method next(cursor: GroupTableCursor, row: var InstantRow,
    varResolver: VarResolver = nil): bool =
  let k = cursor.iter(cursor.groupTable)
  if finished(cursor.iter):
    if cursor.rowsRead or cursor.table.groupBy.len > 0:
      return false
  cursor.rowsRead = true
  var vals: seq[NqValue]
  for i in 0..<cursor.table.columns.len:
    var isAggrCol = true
    for j in 0..<cursor.table.groupBy.len:
      if cursor.table.columns[i].exp of QVarExp:
        let colRef = QVarExp(cursor.table.columns[i].exp)
        if cursor.table.columns[i].colName == cursor.table.groupBy[j].name and
            (cursor.table.groupBy[j].tableName == "" or
                cursor.table.groupBy[j].tableName == colRef.tableName):
          vals.add(k[j])
          isAggrCol = false
          break
    if isAggrCol:
      vals.add(eval(cursor.table.columns[i].exp,
          proc(name: string, rangeVar: string): NqValue =
        raiseDbError(name & " does not exist", undefinedColumnName),
          proc(exp: ScalarOpExp): NqValue =
        case exp.opName
          of "COUNT":
            result = aggrCount(cursor.table.child, cursor.table.groupBy,
                               cursor.table.columns, k, cursor.args)
          of "AVG", "MAX", "MIN", "SUM":
            if exp.args.len != 1:
              raiseDbError("1 argument to " & exp.opName & " required", undefinedFunction)
            if not (exp.args[0] of QVarExp):
              raiseDbError("column reference required", syntaxError)
            let colRef = QVarExp(exp.args[0])
            case exp.opName
              of "MAX":
                result = aggrMax(cursor.table.child,
                                 cursor.table.groupBy,
                                 cursor.table.columns, k,
                                 colRef, cursor.args)
              of "MIN":
                result = aggrMin(cursor.table.child,
                                 cursor.table.groupBy,
                                 cursor.table.columns, k,
                                 colRef, cursor.args)
              of "SUM":
                result = aggrSum(cursor.table.child,
                                 cursor.table.groupBy,
                                 cursor.table.columns, k,
                                 colRef, cursor.args)
              of "AVG":
                result = aggrAvg(cursor.table.child,
                                 cursor.table.groupBy,
                                 cursor.table.columns, k,
                                 colRef, cursor.args)
      ))
  row = InstantRow(table: cursor.table, material: false, vals: vals)
  result = true
