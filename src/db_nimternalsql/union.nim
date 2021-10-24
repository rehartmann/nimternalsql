import nqcommon
import nqcore

type
  UnionTable = ref object of VTable
    leftChild: VTable
    rightChild: VTable
  UnionTableCursor = ref object of Cursor
    cursor: Cursor
    cursorIsLeft: bool
    table: UnionTable

func newUnionTable*(lchild: VTable, rchild: VTable): VTable =
  if columnCount(lchild) != columnCount(rchild):
    raiseDbError("UNION tables differ in number of columns")
  # check types
  result = UnionTable(leftChild: lchild, rightChild: rchild)

method columnCount(table: UnionTable): Natural =
    result = columnCount(table.leftChild)

method newCursor(rtable: UnionTable, args: openArray[string]): Cursor =
  result = UnionTableCursor(args: @args,
                          cursor: newCursor(rtable.leftChild, args),
                          cursorIsLeft: true, table: rtable)

method next(cursor: UnionTableCursor, row: var InstantRow, varResolver: VarResolver = nil): bool =
  if cursor.cursorIsLeft:
    if cursor.cursor.next(row, varResolver):
      return true
    else:
      cursor.cursor = newCursor(cursor.table.rightChild, cursor.args)
      cursor.cursorIsLeft = false
  result = cursor.cursor.next(row, varResolver)

method columnNo*(table: UnionTable, name: string, tableName: string): int =
  let lcol = columnNo(table.leftChild, name, tableName)
  let rcol = columnNo(table.rightChild, name, tableName)

  if lcol == rcol:
    result = lcol
  else:
    result = -1
