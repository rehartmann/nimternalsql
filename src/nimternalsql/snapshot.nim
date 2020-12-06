import nqcommon
import nqcore
import tables

const
  magic = 0x3e3db22a
  writeError = "error writing snapshot"
  readErrorMissingData = "error reading snapshot: missing data"
  fileVersion = 1
  tHashBaseTable = 1

proc raiseIoError*(msg: string) {.noreturn.} =
  var
    e: ref IoError
  new(e)
  e.msg = msg
  raise e

proc writeValue(f: File, val: MatValue) =
  var shortBuf = int16(ord(val.kind))
  if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
    raiseIoError(writeError)
  case val.kind:
    of kInt:
      if writeBuffer(f, unsafeAddr(val.intVal), sizeof(int32)) < sizeof(int32):
        raiseIoError(writeError)
    of kNumeric:
      if writeBuffer(f, unsafeAddr(val.numericVal), sizeof(int64)) < sizeof(int64):
        raiseIoError(writeError)
    of kFloat:
      if writeBuffer(f, unsafeAddr(val.floatVal), sizeof(int64)) < sizeof(int64):
        raiseIoError(writeError)
    of kString:
      var intBuf = int32(val.strVal.len)
      if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
        raiseIoError(writeError)
      write(f, val.strVal)
    of kBool:
      var byteBuf = byte(if val.boolVal: 1 else: 0)
      if writeBuffer(f, addr(byteBuf), 1) < 1:
        raiseIoError(writeError)
    of kNull:
      discard

proc writeRecord(f: File, rec: Record) =
  var reclen = int16(rec.len)
  if writeBuffer(f, addr(reclen), sizeof(int16)) < sizeof(int16):
    raiseIoError(writeError)
  for v in rec:
    writeValue(f, v)

proc writeTable(f: File, table: BaseTable) =
  var intBuf = int32(table.name.len)
  if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
    raiseIoError(writeError)
  write(f, table.name)
  
  var shortBuf = tHashBaseTable;
  if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
    raiseIoError(writeError)  

  intBuf = int32(table.def.len)
  if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
    raiseIoError(writeError)
  for colDef in table.def:
    intBuf = int32(colDef.name.len)
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)
    write(f, colDef.name)

    intBuf = int32(colDef.typ.len)
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)
    write(f, colDef.typ)

    intBuf = int32(colDef.size)
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)
    
    shortBuf = int16(colDef.precision)
    if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseIoError(writeError)

    shortBuf = int16(colDef.scale)
    if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseIoError(writeError)

    var byteBuf = byte(if colDef.notNull: 0 else: 1)
    if writeBuffer(f, addr(byteBuf), 1) < 1:
      raiseIoError(writeError)
      
    byteBuf = byte(if colDef.defaultValue == nil: 0 else: 1)
    if writeBuffer(f, addr(byteBuf), 1) < 1:
      raiseIoError(writeError)
    if colDef.defaultValue != nil:
      writeValue(f, toMatValue(eval(colDef.defaultValue, nil, nil), colDef))

  shortBuf = int16(table.primaryKey.len)
  if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
    raiseIoError(writeError)
  for k in table.primaryKey:
    shortBuf = int16(k)
    if writeBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseIoError(writeError)
  
  intBuf = int32(((HashBaseTable)table).rows.len)
  if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
    raiseIoError(writeError)
  for k, v in ((HashBaseTable)table).rows.pairs:
    writeRecord(f, k)
    writeRecord(f, v)


proc save*(db: Database, filename: string) =
  let f = open(filename, fmWrite)

  try:
    var intBuf: int32 = magic
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)

    intBuf = fileVersion
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)
      
    intBuf = int32(db.tables.len)
    if writeBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseIoError(writeError)
    for t in db.tables.values:
      writeTable(f, t)
  finally:
    close(f)

proc readValue(f: File): MatValue =
  var shortBuf: int16
  if readBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
    raiseDbError(readErrorMissingData)
  let k = MatValueKind(shortBuf)
  case k:
    of kInt:
      var intVal: int32
      if readBuffer(f, addr(intVal), sizeof(int32)) < sizeof(int32):
        raiseDbError(readErrorMissingData)
      result = MatValue(kind: kInt, intVal: intVal)
    of kNumeric:
      var nVal: int64
      if readBuffer(f, addr(nVal), sizeof(int64)) < sizeof(int64):
        raiseDbError(readErrorMissingData)
      result = MatValue(kind: kNumeric, numericVal: nVal)
    of kFloat:
      var floatVal: float
      if readBuffer(f, addr(floatVal), sizeof(int64)) < sizeof(int64):
        raiseDbError(readErrorMissingData)
      result = MatValue(kind: kFloat, floatVal: floatVal)
    of kString:
      var len: int32
      if readBuffer(f, addr(len), sizeof(int32)) < sizeof(int32):
        raiseDbError(readErrorMissingData)
      var strVal = newString(len)
      if readChars(f, strVal, 0, len) < len:
        raiseDbError(readErrorMissingData)
      result = MatValue(kind: kString, strVal: strVal)
    of kBool:
      result = MatValue(kind: kBool,
                             boolVal: if readChar(f) == '\0': false else: true)
    of kNull:
      discard  
    
proc readRecord(f: File): Record[MatValue] =
  var reclen: int16
  if readBuffer(f, addr(reclen), sizeof(int16)) < sizeof(int16):
    raiseDbError(readErrorMissingData)
  for i in 0..<reclen:
    result.add(readValue(f))

func toExpr(v: NqValue): Expression =
  case v.kind:
    of tvkInt, tvkNumeric, tvkFloat:
      result = NumericLit(val: $v)
    of tvkString:
      result = StringLit(val: v.strVal)
    of tvkBool:
      result = BoolLit(val: if v.boolVal: "TRUE" else: "FALSE")
    of tvkNull:
      result = NullLit()
    of tvkList:
      raiseDbError("internal error: list value is invalid")

proc readTable(f: File): BaseTable =
  var htable: HashBaseTable
  new(htable)
  result = htable

  var intBuf: int32
  if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
    raiseDbError(readErrorMissingData)
  result.name = newString(intBuf)
  if readChars(f, result.name, 0, intBuf) < intBuf:
    raiseDbError(readErrorMissingData)
  
  var shortBuf: int16
  if readBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
    raiseDbError(readErrorMissingData)
  if shortBuf != tHashBaseTable:
    raiseDbError("invalid result type")

  var colCount: int32
  if readBuffer(f, addr(colCount), sizeof(int32)) < sizeof(int32):
    raiseDbError(readErrorMissingData)
  var colDef: ColumnDef
  for i in 0..<colCount:
    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    colDef.name = newString(intBuf)

    if readChars(f, colDef.name, 0, intBuf) < intBuf:
      raiseDbError(readErrorMissingData)

    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    colDef.typ = newString(intBuf)
    if readChars(f, colDef.typ, 0, intBuf) < intBuf:
      raiseDbError(readErrorMissingData)

    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    colDef.size = intBuf

    if readBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseDbError(readErrorMissingData)
    colDef.precision = shortBuf

    if readBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseDbError(readErrorMissingData)
    colDef.scale = shortBuf

    var byteBuf: byte
    if readBuffer(f, addr(byteBuf), sizeof(byte)) < sizeof(byte):
      raiseDbError(readErrorMissingData)
    case byteBuf:
      of 0:
        colDef.notNull = false
      of 1:
        colDef.notNull = true
      else:
        raiseDbError("invalid notNull value")
    
    if readBuffer(f, addr(byteBuf), sizeof(byte)) < sizeof(byte):
      raiseDbError(readErrorMissingData)
    if byteBuf == 1:
      colDef.defaultValue = toExpr(toNqValue(readValue(f), colDef))
    result.def.add(colDef)

  var keyLen: int16
  if readBuffer(f, addr(keyLen), sizeof(int16)) < sizeof(int16):
    raiseDbError(readErrorMissingData)
  for i in 0..<keyLen:
    if readBuffer(f, addr(shortBuf), sizeof(int16)) < sizeof(int16):
      raiseDbError(readErrorMissingData)
    result.primaryKey.add(shortBuf)

  if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
    raiseDbError(readErrorMissingData)
  for i in 0..<intBuf:
    let k = readRecord(f)
    let v = readRecord(f)
    ((HashBaseTable)result).rows[k] = v

proc restore*(db: var Database, filename: string) =
  let f = open(filename, fmRead)
  
  try:
    var intBuf: int32;
    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    if intBuf != magic:
      raiseDbError("invalid snapshot file")

    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    if intBuf != fileVersion:
      raiseDbError("invalid snapshot file: version mismatch")

    if readBuffer(f, addr(intBuf), sizeof(int32)) < sizeof(int32):
      raiseDbError(readErrorMissingData)
    var tables: seq[BaseTable]
    for i in 0..<intBuf:
      tables.add(readTable(f))
    clear(db.tables)
    for t in tables:
      db.tables[t.name] = t
  finally:
    close(f)
  
