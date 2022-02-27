import nqcommon
import nqtables
import db_common

type
  UnionTable = ref object of VTable
    leftChild: VTable
    rightChild: VTable
  UnionTableCursor = ref object of Cursor
    cursor: Cursor
    cursorIsLeft: bool
    table: UnionTable

func isNum(kind: DbTypeKind): bool =
  result = (kind == dbInt) or (kind == dbDecimal) or (kind == dbFloat)

func newUnionTable*(lchild: VTable, rchild: VTable): VTable =
  if columnCount(lchild) != columnCount(rchild):
    raiseDbError("UNION tables differ in number of columns", syntaxError)
  # Type check is currently disabled because it requires full type inference
  # which is not available yet
  when defined(unionColumnTypeCheck):
    let lcols = lchild.getColumns()
    let rcols = rchild.getColumns()
    for i in 0..<lcols.len:
      if isNum(lcols[i].typ.kind) != isNum(rcols[i].typ.kind):
        raiseDbError("Incompatible UNION types", typeMismatch)
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

method getColumns*(table: UnionTable): DbColumns =
  let lcols = table.leftChild.getColumns()
  let rcols = table.rightChild.getColumns()
  for i in 0..<lcols.len:
    let typ = DbType(kind: if lcols[i].typ.kind == rcols[i].typ.kind: lcols[i].typ.kind else: dbUnknown,
            notNull: false,
            name: if lcols[i].typ.name == rcols[i].typ.name: lcols[i].typ.name else: "",
            size: if lcols[i].typ.size == rcols[i].typ.size: lcols[i].typ.size else: 0,
            maxReprLen: if lcols[i].typ.maxReprLen >= rcols[i].typ.maxReprLen: lcols[i].typ.maxReprLen
                        else: rcols[i].typ.maxReprLen,
            precision: if lcols[i].typ.precision >= rcols[i].typ.precision: lcols[i].typ.precision
                       else: rcols[i].typ.precision,
            scale: if lcols[i].typ.scale >= rcols[i].typ.scale: lcols[i].typ.scale
                   else: rcols[i].typ.scale,
            min: if lcols[i].typ.min <= rcols[i].typ.min: lcols[i].typ.min
                   else: rcols[i].typ.min,
            max: if lcols[i].typ.min >= rcols[i].typ.min: lcols[i].typ.max
                   else: rcols[i].typ.max,
            validValues: @[])
    result.add(DbColumn(name: if lcols[i].name == rcols[i].name: lcols[i].name else: "",
                    tableName: "", typ: typ, primaryKey: false, foreignKey: false))
