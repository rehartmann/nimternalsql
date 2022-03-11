import nqcommon
import nqtables
import db_common

type
  ExceptTable = ref object of VTable
    leftChild: VTable
    rightChild: VTable
  ExceptTableCursor = ref object of Cursor
    cursor: Cursor
    table: ExceptTable

func newExceptTable*(lchild: VTable, rchild: VTable): VTable =
  if columnCount(lchild) != columnCount(rchild):
    raiseDbError("EXCEPT tables differ in number of columns", syntaxError)
  result = ExceptTable(leftChild: lchild, rightChild: rchild)

method columnCount(table: ExceptTable): Natural =
    result = columnCount(table.leftChild)

method newCursor(rtable: ExceptTable, args: openArray[string]): Cursor =
  result = ExceptTableCursor(args: @args,
                          cursor: newCursor(rtable.leftChild, args),
                          table: rtable)

method next(cursor: ExceptTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  while true:
    if not cursor.cursor.next(row, varResolver):
      return false
    if not cursor.table.rightChild.contains(row):
      return true

method columnNo*(table: ExceptTable, name: string, tableName: string): int =
  result = columnNo(table.leftChild, name, tableName)

method getColumns*(table: ExceptTable): DbColumns =
  result = table.leftChild.getColumns()
