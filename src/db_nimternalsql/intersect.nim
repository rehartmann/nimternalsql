import nqcommon
import nqtables
import db_common

type
  IntersectTable = ref object of VTable
    child1: VTable
    child2: VTable
  IntersectTableCursor = ref object of Cursor
    cursor: Cursor
    table: IntersectTable

func newIntersectTable*(child1: VTable, child2: VTable): VTable =
  if columnCount(child1) != columnCount(child2):
    raiseDbError("INTERSECT tables differ in number of columns", syntaxError)
  result = IntersectTable(child1: child1, child2: child2)

method columnCount(table: IntersectTable): Natural =
    result = columnCount(table.child1)

method newCursor(rtable: IntersectTable, args: openArray[string]): Cursor =
  result = IntersectTableCursor(args: @args,
                          cursor: newCursor(rtable.child1, args),
                          table: rtable)

method next(cursor: IntersectTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  while true:
    if not cursor.cursor.next(row, varResolver):
      return false
    if cursor.table.child2.contains(row):
      return true

method columnNo*(table: IntersectTable, name: string, tableName: string): int =
  result = columnNo(table.child1, name, tableName)

method getColumns*(table: IntersectTable): DbColumns =
  result = table.child1.getColumns
