import nqcommon
import nqtables
import algorithm
import db_common

type
  SortedTable = ref object of VTable
    child: VTable
    order: seq[tuple[col: Natural, asc: bool]]
    removeDuplicates: bool
  SortedTableCursor = ref object of Cursor
    table: SortedTable
    nextRow: Natural
    rows: seq[seq[NqValue]]

func newSortedTable*(child: VTable, order: seq[tuple[col: Natural, asc: bool]],
                     removeDuplicates: bool = false): VTable =
  result = SortedTable(child: child, order: order, removeDuplicates: removeDuplicates)

method columnCount(table: SortedTable): Natural =
  result = columnCount(table.child)

method columnNo(table: SortedTable, name: string, tableName: string): int =
  return table.child.columnNo(name, tableName)

proc cmp*(v1: NqValue, v2:NqValue): int =
  if v1.kind == nqkNull or v2.kind == nqkNull:
    return ord(v1.kind) - ord(v2.kind)
  elif v1.kind == nqkInt and v2.kind == nqkInt:
    return cmp(v1.intVal, v2.intVal)
  elif v1.kind == nqkFloat or v2.kind == nqkFloat:
    return cmp(toFloat(v1), toFloat(v2))
  elif v1.kind == nqkBool and v2.kind == nqkBool:
    return cmp(v1.boolVal, v2.boolVal)
  elif v1.kind == nqkString and v2.kind == nqkString:
    return cmp(v1.strVal, v2.strVal)
  elif v1.kind == nqkInt or v1.kind == nqkNumeric or
        v2.kind == nqkInt or v2.kind == nqkNumeric:
    var a = toNumeric(v1)
    var b = toNumeric(v2)
    adjustScale(a, b)
    return cmp(a.numericVal, b.numericVal)
  elif v1.kind == nqkBigint and v2.kind == nqkBigint:
    return cmp(v1.bigintVal, v2.bigintVal)
  else:
    raiseDbError("comparing incompatible types", typeMismatch)

method newCursor(table: SortedTable, args: openArray[string]): Cursor =
  let childCursor = newCursor(table.child, args)
  let columnCount = columnCount(table)
  let cursor = SortedTableCursor(table: table, nextRow: 0, rows: seq[seq[NqValue]](@[]))
  var row: InstantRow
  while childCursor.next(row):
    var destRow: seq[NqValue]
    for i in 0..<columnCount:
      destRow.add(columnValueAt(row, i))
    cursor.rows.add(destRow)
  cursor.rows.sort(proc(r1, r2: seq[NqValue]): int =
      for i in 0..<table.order.len:
        let d = cmp(r1[table.order[i].col], r2[table.order[i].col])
        if d != 0:
          return if table.order[i].asc: d else: -d
      result = 0)
  result = cursor

method next(cursor: SortedTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  while true:
    if cursor.nextRow >= cursor.rows.len:
      return false
    row = newInstantRow(cursor.table, cursor.rows[cursor.nextRow])
    cursor.nextRow += 1
    if not cursor.table.removeDuplicates or
       cursor.nextRow < 2 or cursor.rows[cursor.nextRow - 1] != cursor.rows[cursor.nextRow - 2]:
      return true

method getColumns*(table: SortedTable): DbColumns =
  result = table.child.getColumns()
