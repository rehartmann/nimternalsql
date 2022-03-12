import nqtables
import db_common
import sets

type
  DupRemTable* = ref object of VTable
    child*: VTable
  DupRemTableCursor = ref object of Cursor
    table: DupRemTable
    iter: iterator(cursor: DupRemTableCursor): InstantRow
    rows: HashSet[seq[NqValue]]

func newDupRemTable*(child: VTable): VTable =
  result = DupRemTable(child: child)

method columnCount(table: DupRemTable): Natural =
  result = columnCount(table.child)

method columnNo(table: DupRemTable, name: string, tableName: string): int =
  return table.child.columnNo(name, tableName)

iterator instantRows(cursor: DupRemTableCursor): InstantRow {.closure.} =
  for r in cursor.rows.items:
    yield newInstantRow(cursor.table.child, r)

method newCursor(table: DupRemTable, args: openArray[string]): Cursor =
  let childCursor = newCursor(table.child, args)
  let columnCount = columnCount(table)
  var row: InstantRow
  var rows: seq[seq[NqValue]]
  while childCursor.next(row):
    var destRow: seq[NqValue]
    for i in 0..<columnCount:
      destRow.add(columnValueAt(row, i))
    rows.add(destRow)
  result = DupRemTableCursor(table: table, iter: instantRows,
                             rows: toHashSet(rows))

method next(cursor: DupRemTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  row = cursor.iter(cursor)
  result = not finished(cursor.iter)

method getColumns*(table: DupRemTable): DbColumns =
  result = table.child.getColumns()
