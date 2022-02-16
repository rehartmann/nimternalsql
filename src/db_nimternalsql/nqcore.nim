#
# Copyright (c) 2020 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
# 
import tables
import strutils
import hashes
import sequtils
import math
import times
import strformat
import algorithm
import unicode
import nqcommon
import like

const
  maxPrecision* = 18
  maxNumeric = 999999999999999999

func getMaxByPrecision: array[1..18, int64] =
  var val: int64 = 9
  result[1] = val
  for i in 2..result.high:
    val = val * 10 + 9
    result[i] = val

let
  maxByPrecision = getMaxByPrecision()
  baseDate = dateTime(2000, mJan, 1, zone = utc())

type
  XColumnDef* = object of ColumnDef
    currentAutoincVal*: int64 # for Autoincrement
  BaseTable* = ref object of RootObj
    name*: string
    def*: seq[XColumnDef]
    primaryKey*: seq[Natural]
  Database* = ref object
    tables*: Table[string, BaseTable]
  Record*[T] = seq[T]
#  SqlError = object of DbError
#    sqlState: string

  MatValueKind* = enum kInt, kNumeric, kFloat, kString, kBool, kNull, kBigint,
                       kTime, kDate
  ## A value in a base table.
  ## The scale in the case of kNumeric is taken from the column definition.
  MatValue* = object
    case kind*: MatValueKind
      of kInt:
        intVal*: int32
      of kNumeric, kBigint:
        numericVal*: int64
      of kFloat:
        floatVal*: float
      of kString:
        strVal*: string
      of kBool:
        boolVal*: bool
      of kTime:
        microsecond*: int64
      of kDate:
        year*: int16
        month*: int8
        day*: int8
      of kNull:
        discard
  HashBaseTable* = ref object of BaseTable
    rows*: Table[Record[MatValue], Record[MatValue]]

  NqValueKind* = enum nqkNull, nqkInt, nqkNumeric, nqkFloat, nqkString,
                      nqkBool, nqkList, nqkBigint, nqkTime, nqkDate, nqkTimestamp
  NqValue* = object
    case kind*: NqValueKind
    of nqkNull:
      discard
    of nqkInt:
      intVal*: int32
    of nqkNumeric:
      numericVal*: int64
      scale*: Natural
    of nqkFloat:
      floatVal*: float
    of nqkString:
      strVal*: string
    of nqkBool:
      boolVal*: bool
    of nqkList:
      listVal: seq[NqValue]
    of nqkBigint:
      bigintVal*: int64
    of nqkTime, nqkTimestamp:
      microsecond*: int64
      precision*: Natural
    of nqkDate:
      year*: int
      month*: Month
      day*: MonthdayRange
  VarResolver* = proc(name: string, rangeVar: string): NqValue
  AggrResolver = proc(exp: ScalarOpExp): NqValue

  VTable* = ref object of Expression
  BaseTableRef* = ref object of VTable
    table*: BaseTable
    rangeVar*: string
  WhereTable* = ref object of VTable
    child*: VTable
    whereExp*: Expression
  ProjectTable = ref object of VTable
    child: VTable
    columns: seq[SelectElement]
  GroupTable = ref object of VTable
    child: VTable
    groupBy: seq[QVarExp]
    columns: seq[SelectElement]

  ColumnAssignment* = object
    col*: Natural
    src*: Expression

  InstantRow* = object
    ## Holds a row's column values.
    table*: VTable
    case material*: bool
      of true:
        keyRecord*: Record[MatValue]
      of false:
        vals: seq[NqValue]

  Cursor* = ref object of RootObj
    args*: seq[string]
  TableRefCursor = ref object of Cursor
    iter: iterator(tableRef: BaseTableRef): InstantRow
    tableRef: BaseTableRef
  WhereTableCursor = ref object of Cursor
    cursor: Cursor
    table: WhereTable
    key: Record[MatValue]
    beforeFirstRow: bool
  ProjectTableCursor = ref object of Cursor
    cursor: Cursor
    table: ProjectTable
  GroupTableCursor = ref object of Cursor
    iter: iterator(groupTable: Table[Record[NqValue], Record[
        NqValue]]): Record[NqValue]
    groupTable: Table[Record[NqValue], Record[NqValue]]
    table: GroupTable
    rowsRead: bool

func newInstantRow*(table: VTable, vals: seq[NqValue]): InstantRow =
  result = InstantRow(table: table, material: false, vals: vals)

func newMatInstantRow*(table: VTable, key: Record[MatValue]): InstantRow =
  result = InstantRow(table: table, material: true, keyRecord: key)

func newWhereTable*(child: VTable, whereExp: Expression): VTable =
  result = WhereTable(child: child, whereExp: whereExp)

func newProjectTable*(child: VTable, columns: seq[SelectElement]): VTable =
  result = ProjectTable(child: child, columns: columns)

method columnNo*(rtable: VTable, name: string, tableName: string): int {.base.} =
  raiseDbError("not implemented")

func isQVarExp*(exp: Expression): bool =
  result = exp of QVarExp

func hash(v: MatValue): Hash =
  case v.kind
  of kInt: result = hash(v.intVal)
  of kTime: result = hash(v.microsecond)
  of kDate: result = hash(v.year) xor hash(v.month) xor hash(v.day)
  of kNumeric, kBigint: result = hash(v.numericVal)
  of kFloat: result = hash(v.floatVal)
  of kString: result = hash(v.strVal)
  of kBool: result = hash(v.boolVal)
  of kNull: result = 17

func hash*(v: NqValue): Hash =
  case v.kind
  of nqkInt: result = hash(v.intVal)
  of nqkTime, nqkTimestamp: result = hash(v.microsecond)
  of nqkDate: result = hash(v.year) xor hash(v.month) xor hash(v.day)
  of nqkNumeric: result = hash(v.numericVal)
  of nqkFloat: result = hash(v.floatVal)
  of nqkString: result = hash(v.strVal)
  of nqkBool: result = hash(v.boolVal)
  of nqkNull: result = 17
  of nqkList: result = if v.listVal.len == 0: 13 else: hash(v.listVal[0])
  of nqkBigint: result = hash(v.bigintVal)

func toNum*(v: NqValue): NqValue =
  case v.kind
    of nqkInt, nqkNumeric, nqkFloat, nqkBigint:
      result = v
    of nqkString:
      try:
        if v.strVal.len > maxPrecision + 1:
          result = NqValue(kind: nqkFloat, floatVal: parseFloat(v.strVal))
        else:
          let dotPos = find(v.strVal, ".")
          if dotPos == -1:
            result = NqValue(kind: nqkNumeric, numericVal: parseInt(v.strVal))
          else:
            result = NqValue(kind: nqkNumeric,
                        numericVal: parseInt(v.strVal[0 .. dotPos - 1] &
                            v.strVal[dotPos + 1 .. ^1]),
                        scale: v.strVal.len - 1 - dotPos)
      except ValueError:
        raiseDbError("invalid numeric value: " & v.strVal)
    else: raiseDbError("numeric value required")

func toNumeric*(v: NqValue): NqValue =
  case v.kind
    of nqkInt:
      result = NqValue(kind: nqkNumeric, scale: 0, numericVal: v.intVal)
    of nqkNumeric:
      result = v
    of nqkBigint:
      result = NqValue(kind: nqkNumeric, scale: 0, numericVal: v.bigintVal)
    of nqkFloat:
      result = NqValue(kind: nqkNumeric, scale: 0, numericVal: int64(round(v.floatVal)))
    of nqkString:
      try:
        let dotPos = find(v.strVal, ".")
        var val: BiggestInt
        if dotPos == -1:
          val = parseBiggestInt(v.strVal)
        else:
          val = parseBiggestInt(v.strVal[0 .. dotPos - 1] &
                            v.strVal[dotPos + 1 .. ^1])
        if val > high(int64) or val < low(int64):
          raiseDbError("numeric value too larg: " & v.strVal)
        if dotPos == -1:
          result = NqValue(kind: nqkNumeric, numericVal: int64(val))
        else:
          result = NqValue(kind: nqkNumeric,
                        numericVal: int64(val),
                        scale: v.strVal.len - 1 - dotPos)
      except ValueError:
        raiseDbError("invalid numeric value: " & v.strVal)
    else: raiseDbError("numeric value required")

func toInt64(v: NqValue): int64 =
  case v.kind
    of nqkInt:
      result = v.intVal
    of nqkFloat:
      result = int64(round(v.floatVal))
    of nqkNumeric:
      var scale = v.scale
      var num = v.numericVal
      while scale > 1:
        num = num div 10
        scale -= 1
      if scale == 1:
        num = num div 10 + (if num mod 10 >= 5: 1 else: 0)
      if num > int64(high(int)) or num < int64(low(int)):
        raiseDbError("value out of range")
      result = num
    of nqkBigint:
      result = v.bigintVal
    of nqkString:
      try:
        result = int64(parseBiggestInt(v.strVal))
      except ValueError:
        raiseDbError("invalid integer value: " & v.strVal)
    else: raiseDbError("invalid integer value")

func toFloat*(v: NqValue): float =
  case v.kind
    of nqkInt:
      result = float(v.intVal)
    of nqkNumeric:
      result = float(v.numericVal)
      var scale = v.scale
      while scale > 0:
        result /= 10.0
        scale -= 1
    of nqkBigint:
      result = float(v.bigintVal)
    of nqkFloat:
      result = v.floatVal
    of nqkString:
      try:
        result = parseFloat(v.strVal)
      except ValueError:
        raiseDbError("invalid float value: " & v.strVal)
    else: raiseDbError("invalid float value")

func toBool(v: NqValue): bool =
  case v.kind
    of nqkBool: result = v.boolVal
    of nqkString:
      if toUpperAscii(v.strVal) == "TRUE":
        result = true
      elif toUpperAscii(v.strVal) == "FALSE":
        result = false
      else:
        raiseDbError("invalid boolean value: " & v.strVal)
    else: raiseDbError("invalid boolean value")

proc adjustScaleFirstGreater(a: var NqValue, b: var NqValue) =
  # Increase scale of b if possible
  while b.numericVal < maxNumeric div 10 and b.scale < a.scale:
    b.numericVal *= 10
    b.scale += 1
  # Decrease scale of a
  while a.scale > b.scale:
    a.numericVal = a.numericVal div 10
    a.scale -= 1

proc adjustScale*(a: var NqValue, b: var NqValue) =
  if a.scale > b.scale:
    adjustScaleFirstGreater(a, b)
  elif a.scale < b.scale:
    adjustScaleFirstGreater(b, a)

proc precisionMicroseconds(ms: int64, precision: int): int64 =
  result = ms
  for i in 0..5 - precision:
    result = result div 10
  for i in 0..5 - precision:
    result = result * 10

proc toTime(s: string): NqValue =
  let t = parse(s.substr(0, 7), "hh:mm:ss")
  var ms = 0
  if s.len > 8 and s[8] == '.':
    let frac = s.substr(9)
    ms = frac.parseInt
    for i in 0..5 - frac.len:
      ms *= 10
  result = NqValue(kind: nqkTime,
                   microsecond: int64(t.hour * 3600 + t.minute * 60 +
                                      t.second) * 1000000 + ms,
                   precision: 6)

proc toTimestamp(s: string): NqValue =
  var ms = 0
  let dt = parse(s.substr(0, 18), "yyyy-MM-dd HH:mm:ss", utc())
  if s.len > 19 and s[19] == '.':
    let frac = s.substr(20)
    ms = frac.parseInt
    for i in 0..5 - frac.len:
      ms *= 10
  result = NqValue(kind: nqkTimestamp, 
                   microsecond: (dt - baseDate).inMicroseconds + ms,
                   precision: 6)

proc toDate(s: string): NqValue =
  let t = parse(s, "yyyy-MM-dd")
  result = NqValue(kind: nqkDate, year: int16(t.year), month: Month(t.month),
                   day: int8(t.monthday))

proc `==`(v1: NqValue, v2: NqValue): bool =
  if v1.kind == nqkInt and v2.kind == nqkInt:
    result = v1.intVal == v2.intVal
  elif v1.kind == nqkFloat or v2.kind == nqkFloat:
    result = toFloat(v1) == toFloat(v2)
  elif v1.kind == nqkBool and v2.kind == nqkBool:
    result = v1.boolVal == v2.boolVal
  elif v1.kind == nqkString and v2.kind == nqkString:
    return v1.strVal == v2.strVal
  elif v1.kind == nqkInt or v1.kind == nqkNumeric or v1.kind == nqkBigint or
        v2.kind == nqkInt or v2.kind == nqkNumeric or v2.kind == nqkBigint:
    var a = toNumeric(v1)
    var b = toNumeric(v2)
    adjustScale(a, b)
    result = a.numericVal == b.numericVal
  elif v1.kind == nqkTime and v2.kind == nqkTime:
    result = v1.microsecond == v2.microsecond
  elif v1.kind == nqkTime and v2.kind == nqkString:
    result = v1 == toTime(v2.strVal)
  elif v1.kind == nqkString and v2.kind == nqkTime:
    result = toTime(v1.strVal) == v2
  elif v1.kind == nqkDate and v2.kind == nqkDate:
    result = (v1.year == v2.year) and (v1.month == v2.month) and (v1.day == v2.day)
  elif v1.kind == nqkDate and v2.kind == nqkString:
    result = v1 == toDate(v2.strVal)
  elif v1.kind == nqkString and v2.kind == nqkDate:
    result = toDate(v1.strVal) == v2
  elif v1.kind == nqkTimestamp and v2.kind == nqkTimestamp:
    result = v1.microsecond == v2.microsecond
  elif v1.kind == nqkTimestamp and v2.kind == nqkString:
    result = v1 == toTimestamp(v2.strVal)
  elif v1.kind == nqkString and v2.kind == nqkTimestamp:
    result = toTimestamp(v1.strVal) == v2
  else:
    raiseDbError("comparing incompatible types")

proc `!=`(f1: NqValue, f2: NqValue): bool =
  result = not (f1 == f2)

func compareDate(v1: NqValue, v2: NqValue): int =
  var d = v1.year - v2.year
  if d != 0:
    return d
  d = ord(v1.month) - ord(v2.month)
  if d != 0:
    return d
  return v1.day - v2.day

proc `<=`(v1: NqValue, v2: NqValue): bool =
  if v1.kind == nqkInt and v2.kind == nqkInt:
    result = v1.intVal <= v2.intVal
  elif v1.kind == nqkFloat or v2.kind == nqkFloat:
    result = toFloat(v1) <= toFloat(v2)
  elif v1.kind == nqkBool and v2.kind == nqkBool:
    result = v1.boolVal <= v2.boolVal
  elif v1.kind == nqkString and v2.kind == nqkString:
    result = v1.strVal <= v2.strVal
  elif v1.kind == nqkInt or v1.kind == nqkNumeric or v1.kind == nqkBigint or
        v2.kind == nqkInt or v2.kind == nqkNumeric or v2.kind == nqkBigint:
    var a = toNumeric(v1)
    var b = toNumeric(v2)
    adjustScale(a, b)
    result = a.numericVal <= b.numericVal
  elif v1.kind == nqkTime and v2.kind == nqkTime:
    result = v1.microsecond <= v2.microsecond
  elif v1.kind == nqkTime and v2.kind == nqkString:
    result = v1 <= toTime(v2.strVal)
  elif v1.kind == nqkString and v2.kind == nqkTime:
    result = toTime(v1.strVal) <= v2
  elif v1.kind == nqkDate and v2.kind == nqkDate:
    result = compareDate(v1, v2) <= 0
  elif v1.kind == nqkDate and v2.kind == nqkString:
    result = compareDate(v1, toDate(v2.strVal)) <= 0
  elif v1.kind == nqkString and v2.kind == nqkDate:
    result = compareDate(toDate(v1.strVal), v2) <= 0
  elif v1.kind == nqkTimestamp and v2.kind == nqkTimestamp:
    result = v1.microsecond <= v2.microsecond
  elif v1.kind == nqkTimestamp and v2.kind == nqkString:
    result = v1 <= toTimestamp(v2.strVal)
  elif v1.kind == nqkString and v2.kind == nqkTimestamp:
    result = toTimestamp(v1.strVal) <= v2
  else:
    raiseDbError("comparing incompatible types")

proc `>=`(f1: NqValue, f2: NqValue): bool =
  result = f2 <= f1

proc `<`(f1: NqValue, f2: NqValue): bool =
  result = not (f1 >= f2)

proc `>`(f1: NqValue, f2: NqValue): bool =
  result = not (f1 <= f2)

func toNqValue*(v: MatValue, colDef: ColumnDef): NqValue =
  case v.kind:
    of kInt: return NqValue(kind: nqkInt, intVal: v.intVal)
    of kNumeric: return NqValue(kind: nqkNumeric, numericVal: v.numericVal,
        scale: colDef.scale)
    of kFloat: return NqValue(kind: nqkFloat, floatVal: v.floatVal)
    of kString: return NqValue(kind: nqkString, strVal: v.strVal)
    of kBool: return NqValue(kind: nqkBool, boolVal: v.boolVal)
    of kNull: return NqValue(kind: nqkNull)
    of kBigint: return NqValue(kind: nqkBigint, bigintVal: v.numericVal) 
    of kTime:
      return if colDef.typ == "TIMESTAMP": NqValue(kind: nqkTimestamp,
                                                   microsecond: v.microsecond,
                                                   precision: colDef.precision)
             else: NqValue(kind: nqkTime, microsecond: v.microsecond,
                            precision: colDef.precision)
    of kDate: return NqValue(kind: nqkDate, year: v.year, month: Month(v.month), day: v.day)

func typeKind(typ: string): MatValueKind =
  case toUpperAscii(typ):
    of "INTEGER", "INT":
      return kInt
    of "NUMERIC", "DECIMAL":
      return kNumeric
    of "BIGINT":
      return kBigint
    of "REAL":
      return kFloat
    of "TEXT", "CHAR", "VARCHAR", "BINARY", "VARBINARY", "LONGVARBINARY",
       "RAW", "BYTEA":
      return kString
    of "BOOLEAN":
      return kBool
    of "TIME", "TIMESTAMP":
      return kTime
    of "DATE":
      return kDate
    else:
      raiseDbError("Unsupported type definition, type: " & typ)

func typeKind(def: ColumnDef): MatValueKind =
  result = typeKind(def.typ)

proc checkType*(def: ColumnDef) =
  discard typeKind(def)

func setScale(v: NqValue, scale: Natural): NqValue =
  result = v
  while result.scale < scale:
    result.numericVal *= 10
    result.scale += 1
  while result.scale > scale:
    let incr = if result.numericVal mod 10 < 5: 0 else: 1
    result.numericVal = result.numericVal div 10 + incr
    result.scale -= 1

proc withPrecision(tsMicroseconds: int64, precision: int): int64 =
  if precision < 6:
    var dt = baseDate + initDuration(nanoseconds = tsMicroseconds * 1000)
    let frac = dt.nanosecond div 1000
    result = tsMicroseconds - frac + precisionMicroseconds(frac, precision)
  else:
    result = tsMicroseconds

proc toMatValue*(v: NqValue, colDef: ColumnDef): MatValue =
  ## Converts a NqValue into a MatValue.

  if v.kind == nqkNull:
    if colDef.notNull:
      raiseDbError("destination column " & colDef.name & " is not nullable")
    return MatValue(kind: kNull)
  var val: int64
  case typeKind(colDef):
    of kInt:
      val = toInt64(v)
      if val > high(int32) or val < low(int32):
        raiseDBError("value out of range for int")
      result = MatValue(kind: kInt, intVal: int32(val))
    of kBigint:
      result = MatValue(kind: kBigint, numericVal: toInt64(v))
    of kNumeric:
      case v.kind:
        of nqkInt:
          val = toInt64(v)
        of nqkNumeric:
          val = v.setScale(colDef.scale).numericVal
        of nqkFloat:
          var v = v.floatVal
          var scale = colDef.scale
          while scale > 0:
            v *= 10
            scale -= 1
          val = int64(v)
        of nqkString:
          val = parseInt(v.strVal)
        else:
          raiseDbError("invalid value for type " & colDef.typ)
      if colDef.precision > 0 and val > maxByPrecision[colDef.precision]:
        raiseDbError("value is too large for precision " & $colDef.precision &
                     ", scale " & $colDef.scale)
      result = MatValue(kind: kNumeric, numericVal: val)
    of kFloat:
      var val: float
      case v.kind:
        of nqkNumeric:
          val = float(v.numericVal)
          var scale = v.scale
          while scale > 0:
            val /= 10.0
            scale -= 1
        of nqkFloat:
          val = v.floatVal
        of nqkString:
          val = parseFloat(v.strVal)
        else:
          raiseDbError("invalid value for type " & colDef.typ)
      result = MatValue(kind: kFloat, floatVal: val)
    of kString:
      if v.kind != nqkString:
        raiseDbError("invalid value for type " & colDef.typ & $ord(v.kind))
      var s = v.strVal
      if colDef.typ == "CHAR" or colDef.typ == "VARCHAR":
        if s.len > colDef.size:
          for i in colDef.size..s.high:
            if s[i] != ' ':
              raiseDbError("value too long: '" & s & "'")
          s = substr(s, 0, colDef.size)
      if colDef.typ == "CHAR":
        while s.len < colDef.size:
          s = s & " "
      result = MatValue(kind: kString, strVal: s)
    of kBool:
      var val: bool
      case v.kind:
        of nqkBool:
          val = v.boolVal
        of nqkString:
          if toUpperAscii(v.strVal) == "TRUE":
            val = true
          elif toUpperAscii(v.strVal) == "FALSE":
            val = false
          else:
            raiseDbError("invalid value for type " & colDef.typ)
        else:
          raiseDbError("invalid value for type " & colDef.typ)
      result = MatValue(kind: kBool, boolVal: val)
    of kTime:
      case v.kind:
        of nqkTime:
          if colDef.typ != "TIME":
            raiseDbError("invalid time value")
          result = MatValue(kind: kTime,
                            microsecond: precisionMicroseconds(v.microsecond, colDef.precision))
        of nqkTimestamp:
          if colDef.typ != "TIMESTAMP":
            raiseDbError("invalid timestamp value")
          result = MatValue(kind: kTime,
                            microsecond: withPrecision(v.microsecond, colDef.precision))
        of nqkString:
          var ms = 0
          if colDef.typ == "TIMESTAMP":
            if v.strVal.len > 19 and v.strVal[19] == '.':
              let frac = v.strVal.substr(20, 19 + colDef.precision)
              ms = frac.parseInt
              for i in 0..5 - frac.len:
                ms *= 10
            result = toMatValue(toTimestamp(v.strVal), colDef)
          else:
            result = toMatValue(toTime(v.strVal), colDef)
        else:
          raiseDbError("invalid value for type " & colDef.typ)
    of kDate:
      case v.kind:
        of nqkDate:
          result = MatValue(kind: kDate, year: int16(v.year), month: int8(v.month), day: int8(v.day))
        of nqkString:
          result = toMatValue(toDate(v.strVal), colDef)
        else:
          raiseDbError("invalid value for type " & colDef.typ)
    of kNull:
      raiseDbError("internal error: invalid column definition")

func `==`(v1: MatValue, v2: MatValue): bool =
  if v1.kind != v2.kind:
    return false
  case v1.kind:
    of kInt:
      result = v1.intVal == v2.intVal
    of kDate:
      result = (v1.year == v2.year) and (v1.month == v2.month) and
               (v1.day == v2.day)
    of kNumeric, kBigint:
      result = v1.numericVal == v2.numericVal
    of kFloat:
      result = v1.floatVal == v2.floatVal
    of kString:
      result = v1.strVal == v2.strVal
    of kBool:
      result = v1.boolVal == v2.boolVal
    of kTime:
      result = v1.microsecond == v2.microsecond
    of kNull:
      result = true

func hash[T](r: seq[T]): Hash =
  result = 0
  for v in r:
    result = result !& hash(v)
  result = !$result

proc `==`*(r1: seq, r2: seq): bool =
  if r1.len() != r2.len():
    return false
  for i in 0..<r1.len():
    if r1[i] != r2[i]:
      return false
  result = true

func findColumnName(columns: openArray[XColumnDef],
                    name: string): int =
  for i in 0..<columns.len():
    if columns[i].name == name:
      return i
  result = -1

proc newHashBaseTable*(name: string, columns: openArray[ColumnDef],
    key: seq[string]): HashBaseTable =
  var rtable: HashBaseTable
  new(rtable)
  rtable.name = name
  newSeq(rtable.def, columns.len)
  for i in 0..<columns.len:
    rtable.def[i].name = columns[i].name
    rtable.def[i].typ = columns[i].typ
    rtable.def[i].size = columns[i].size
    rtable.def[i].precision = columns[i].precision
    rtable.def[i].scale = columns[i].scale
    rtable.def[i].notNull = columns[i].notNull
    rtable.def[i].defaultValue = columns[i].defaultValue
    rtable.def[i].autoincrement = columns[i].autoincrement
    if columns[i].autoincrement:
      rtable.def[i].currentAutoincVal = 0
  newSeq(rtable.primaryKey, key.len)
  for i, keycol in key:
    let n = findColumnName(rtable.def, keycol)
    if n == -1:
      raiseDbError("invalid key column" & keycol)
    rtable.primaryKey[i] = n
  rtable.rows = initTable[Record[MatValue], Record[MatValue]]()
  result = rtable

func newDatabase*(): Database =
  result = Database(tables: initTable[string, BaseTable]())

func keyIndex*(table: BaseTable, col: int): int =
  for i in 0..<table.primaryKey.len:
    if table.primaryKey[i] == col:
      return i
  result = -1

func isKey*(table: BaseTable, col: int): bool =
  result = keyIndex(table, col) != -1

func getTable*(db: Database, tableName: string): BaseTable =
  try:
    return db.tables[tableName]
  except KeyError:
    raiseDbError("table " & tableName & " does not exist")

method eval*(exp: Expression, varResolver: VarResolver,
             aggrResolver: AggrResolver = proc(exp: ScalarOpExp): NqValue =
                raiseDbError(exp.opName & " not supported")
             ): NqValue {.base.} =
  ## Evaluates an expression using varResolver to resolve variable references.
  ## varResolver may raise KeyError to signal that a variable could not be resolved.
  raiseDbError("internal error: not implemented")

method eval*(exp: StringLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = NqValue(kind: nqkString, strVal: exp.val)

method eval*(exp: NumericLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  if contains(exp.val, "E"):
    return NqValue(kind: nqkFloat,
                      floatVal: parseFloat(exp.val))
  let dotPos = find(exp.val, ".")
  if dotPos >= 0:
    result = NqValue(kind: nqkNumeric,
                        numericVal: parseInt(exp.val[0 .. dotPos - 1] & exp.val[
                            dotPos + 1 .. ^1]),
                        scale: exp.val.len - 1 - dotPos)
  else:
    result = NqValue(kind: nqkNumeric, numericVal: parseInt(exp.val))

method eval*(exp: TimeLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = toTime(exp.val)

method eval*(exp: TimestampLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = toTimestamp(exp.val)

method eval*(exp: DateLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = toDate(exp.val)

method eval*(exp: BoolLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = NqValue(kind: nqkBool, boolVal: if exp.val ==
      "TRUE": true else: false)

method eval*(exp: NullLit, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  result = NqValue(kind: nqkNull)

proc `+`(a: NqValue, b: NqValue): NqValue =
  if a.kind == nqkInt and b.kind == nqkInt:
    result = NqValue(kind: nqkInt, intVal: a.intVal + b.intVal)
  if a.kind == nqkFloat or b.kind == nqkFloat:
    result = NqValue(kind: nqkFloat, floatVal: a.toFloat() + b.toFloat())
  else:
    var adja = toNumeric(a)
    var adjb = toNumeric(b)
    adjustScale(adja, adjb)
    result = NqValue(kind: nqkNumeric,
                        numericVal: adja.numericVal + adjb.numericVal,
                        scale: adja.scale)

proc `-`(a: NqValue, b: NqValue): NqValue =
  if a.kind == nqkInt and b.kind == nqkInt:
    result = NqValue(kind: nqkInt, intVal: a.intVal - b.intVal)
  if a.kind == nqkFloat or b.kind == nqkFloat:
    result = NqValue(kind: nqkFloat, floatVal: a.toFloat() - b.toFloat())
  else:
    var adja = toNumeric(a)
    var adjb = toNumeric(b)
    adjustScale(adja, adjb)
    result = NqValue(kind: nqkNumeric,
                        numericVal: adja.numericVal - adjb.numericVal,
                        scale: adja.scale)

method newCursor*(table: VTable, args: openArray[string]): Cursor {.base.} =
  raiseDbError("not supported")

method next*(cursor: Cursor, row: var InstantRow,
    varResolver: VarResolver = nil): bool {.base.} =
  ## Reads the next row from the cursor.
  ## If a row could be read, true is returned and row is set to the row read.
  ## If there are no more rows, false is returned and the value of row is undefined.
  raiseDbError("not supported")

iterator instantRows*(rtable: VTable, args: varargs[string]): InstantRow =
  let cursor = rtable.newCursor(args)
  var row: InstantRow
  while true:
    if not next(cursor, row):
      break
    yield row

iterator instantRows(rtable: VTable, varResolver: VarResolver): InstantRow =
  let cursor = rtable.newCursor([])
  var row: InstantRow
  while true:
    if not next(cursor, row, varResolver):
      break
    yield row

func columnValueAt*(row: InstantRow; col: Natural): NqValue

method columnCount*(table: VTable): Natural {.base.} =
  raiseDbError("internal error: not implemented")

func columnCount*(row: InstantRow): int =
  result = columnCount(row.table)

func isAllOrAny(exp: Expression): bool =
  result = (exp of ScalarOpExp) and (((ScalarOpExp)exp).opName == "ALL" or
                                     ((ScalarOpExp)exp).opName == "ANY")

func evalCmpAllOrAny(arg0: NqValue, exp: ScalarOpExp,
                     cmp: proc(v1: NqValue, v2: NqValue): bool,
                     varResolver: VarResolver): NqValue =
  if columnCount(VTable(exp.args[0])) != 1:
    raiseDbError("subquery must have exactly one column")
  if exp.opName == "ALL":
    result = NqValue(kind: nqkBool, boolVal: true)
    for row in instantRows(VTable(exp.args[0]), varResolver):
      if not cmp(arg0, row.columnValueAt(0)):
        return NqValue(kind: nqkBool, boolVal: false)
  else:
    result = NqValue(kind: nqkBool, boolVal: false)
    for row in instantRows(VTable(exp.args[0]), varResolver):
      if cmp(arg0, row.columnValueAt(0)):
        return NqValue(kind: nqkBool, boolVal: true)

proc toInt(val: NqValue): int =
  let v = toInt64(val)
  if v > int.high or v < int.low:
    raiseDbError("value out of range")
  result = int(v)

method eval*(exp: ScalarOpExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  case exp.opName:
    of "COUNT", "AVG", "MAX", "MIN", "SUM":
      return aggrResolver(exp)
    of "EXISTS":
      if not (exp.args[0] of VTable):
        raiseDbError("argument of EXISTS must be a table expression")
      for row in instantRows(VTable(exp.args[0]), varResolver):
        return NqValue(kind: nqkBool, boolVal: true)
      return NqValue(kind: nqkBool, boolVal: false)
  let arg0 = eval(exp.args[0], varResolver, aggrResolver)
  if exp.opName == "isNull":
    return NqValue(kind: nqkBool, boolVal: arg0.kind == nqkNull)
  if arg0.kind == nqkNull:
    return NqValue(kind: nqkNull)
  case exp.opName:
    of "AND":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver)
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      result = NqValue(kind: nqkBool,
          boolVal: arg0.toBool and arg1.toBool)
    of "OR":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver)
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      result = NqValue(kind: nqkBool,
          boolVal: arg0.toBool or arg1.toBool)
    of "NOT":
      result = NqValue(kind: nqkBool, boolVal: not arg0.toBool)
    of "=":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `==`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool, boolVal: arg0 == arg1)
    of "<>":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `!=`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool, boolVal: arg0 != arg1)
    of "<":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `<`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool, boolVal: arg0 < arg1)
    of "<=":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `<=`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool, boolVal: arg0 <= arg1)
    of ">":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `>`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool, boolVal: arg0 > arg1)
    of ">=":
      if isAllOrAny(exp.args[1]):
        result = evalCmpAllOrAny(arg0, (ScalarOpExp)exp.args[1], `>=`, varResolver)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver)
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = NqValue(kind: nqkBool,
            boolVal: arg0 >= arg1)
    of "IN":
      if exp.args[1] of VTable:
        if columnCount(VTable(exp.args[1])) != 1:
          raiseDbError("subquery must have exactly one column")
        for row in instantRows(VTable(exp.args[1]), varResolver):
          if row.columnValueAt(0) == arg0:
            return NqValue(kind: nqkBool, boolVal: true)
        return NqValue(kind: nqkBool, boolVal: false)
      let lv = eval(exp.args[1], varResolver, aggrResolver)
      if lv.kind == nqkNull:
        return NqValue(kind: nqkNull)
      if lv.kind == nqkList:
        result = NqValue(kind: nqkBool,
            boolVal: any(lv.listVal, proc(v: NqValue): bool = return v == arg0))
      else:
        raiseDbError("Invalid argument to IN")
    of "+":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver).toNum()
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      result = arg0 + arg1
    of "-":
      if exp.args.len == 1:
        if arg0.kind == nqkNumeric:
          result = NqValue(kind: nqkNumeric, numericVal: -arg0.numericVal)
        else:
          result = NqValue(kind: nqkFloat, floatVal: -arg0.floatVal)
      else:
        let arg1 = eval(exp.args[1], varResolver, aggrResolver).toNum()
        if arg1.kind == nqkNull:
          return NqValue(kind: nqkNull)
        result = arg0 - arg1
    of "*":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver).toNum()
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)

      if arg0.kind == nqkNumeric and arg1.kind == nqkNumeric and
          arg0.scale + arg1.scale <= maxPrecision and
          arg0.numericVal <= high(int64) div arg1.numericVal:
        result = NqValue(kind: nqkNumeric, numericVal: arg0.numericVal * arg1.numericVal,
                            scale: arg0.scale + arg1.scale)
      else:
        result = NqValue(kind: nqkFloat, floatVal: toFloat(arg0) * toFloat(arg1))
    of "/":
      let farg0 = toFloat(arg0)
      let arg1 = eval(exp.args[1], varResolver, aggrResolver).toNum()
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      let farg1 = toFloat(arg1)
      result = NqValue(kind: nqkFloat, floatVal: farg0 / farg1)
    of "||":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver)
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      if arg0.kind != nqkString or arg1.kind != nqkString:
        raiseDbError("arguments of || must be of a character type")
      result = NqValue(kind: nqkString,
          strVal: arg0.strVal & arg1.strVal)
    of "LOWER":
      result = NqValue(kind: nqkString, strVal: toLowerAscii(arg0.strVal))
    of "UPPER":
      result = NqValue(kind: nqkString, strVal: toUpperAscii(arg0.strVal))
    of "LIKE":
      let arg1 = eval(exp.args[1], varResolver, aggrResolver)
      if arg1.kind == nqkNull:
        return NqValue(kind: nqkNull)
      result = NqValue(kind: nqkBool,
          boolVal: matchesLike(arg0.strVal, arg1.strVal))
    of "OCTET_LENGTH":
      result = NqValue(kind: nqkInt, intVal: int32(arg0.strVal.len))
    of "CHAR_LENGTH", "LENGTH":
      result = NqValue(kind: nqkInt, intVal: int32(runeLen(arg0.strVal)))
    of "SUBSTR":
      if exp.args.len < 2:
        raiseDbError("missing SUBSTR argument(s)")
      if exp.args.len > 3:
        raiseDbError("too many SUBSTR arguments")
      if arg0.kind != nqkString:
        raiseDbError("Character argument expected")
      var pos = toInt(eval(exp.args[1], varResolver, aggrResolver))
      if pos > 0:
        pos -= 1
      if exp.args.len == 2:
        result = NqValue(kind: nqkString, strVal: runeSubStr(arg0.strVal, pos))
      else:
        let len = toInt(eval(exp.args[2], varResolver, aggrResolver))
        # Workaround for a bug in unicode.runeSubStr
        if pos < 0 and len >= -pos:
          result = NqValue(kind: nqkString,
                           strVal: runeSubStr(arg0.strVal, pos))
        else:
          result = NqValue(kind: nqkString,
                           strVal: runeSubStr(arg0.strVal, pos, len))
    of "POSITION":
      if exp.args.len != 2:
        raiseDbError("Invalid number of arguments")
      if arg0.kind != nqkString:
        raiseDbError("Character argument expected")
      let str = eval(exp.args[1], varResolver, aggrResolver)
      if str.kind != nqkString:
        raiseDbError("Character argument expected")
      let pos = find(str.strVal, arg0.strVal)
      if pos < 0:
        result = NqValue(kind: nqkInt, intVal: 0)
      else:
        result = NqValue(kind: nqkInt,
                         intVal: int32(runeLen(str.strVal[0..<pos]) + 1))
    else:
      raiseDbError("Unknown operator: " & exp.opName)

method getAggrs(exp: Expression): seq[Expression] {.base.} =
  result = @[]

method getAggrs(exp: ScalarOpExp): seq[Expression] =
  if exp.opName == "COUNT" or exp.opName == "AVG" or exp.opName == "MAX" or
      exp.opName == "MIN" or exp.opName == "SUM":
    return @[Expression(exp)]
  for arg in exp.args:
    result &= getAggrs(arg)

func getAggrs*(sels: seq[SelectElement]): seq[Expression] =
  for sel in sels:
    if sel.exp != nil:
      result &= sel.exp.getAggrs()

method eval*(exp: QVarExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  try:
    if exp.name == "CURRENT_TIME":
      let t = now().utc
      return NqValue(kind: nqkTime,
                     microsecond: int64(t.hour * 3600 + t.minute * 60 + t.second) * 1000000,
                     precision: 0)
    if exp.name == "CURRENT_DATE":
      let t = now()
      return NqValue(kind: nqkDate, year: t.year, month: t.month, day: t.monthday)
    if exp.name == "CURRENT_TIMESTAMP":
      return NqValue(kind: nqkTimestamp,
                     microsecond: (now().utc - baseDate).inMicroseconds,
                     precision: 6)
    result = varResolver(exp.name, exp.tableName)
  except KeyError:
    raiseDbError("Not found: " & exp.name)

method eval*(exp: ListExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  var vals: seq[NqValue]
  for e in exp.exps:
    vals.add(eval(e, varResolver))
  return NqValue(kind: nqkList, listVal: vals)

method eval*(exp: VTable, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  if columnCount(exp) != 1:
    raiseDbError("subquery has too many columns")
  var count = 0
  for row in instantRows(exp, varResolver):
    if count > 0:
      raiseDbError("more than one row returned by a subquery used as an expression")
    result = columnValueAt(row, 0)
    count += 1
  if count == 0:
    result = NqValue(kind: nqkNull)

method eval*(exp: CaseExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  if exp.exp != nil:
    let val = eval(exp.exp, varResolver, aggrResolver)
    for i in 0..<exp.whens.len:
      let whenVal = eval(exp.whens[i].cond, varResolver, aggrResolver)
      if whenVal == val:
        return eval(exp.whens[i].exp, varResolver, aggrResolver)
  else:
    for i in 0..<exp.whens.len:
      let cond = eval(exp.whens[i].cond, varResolver, aggrResolver)
      if cond.kind != nqkBool:
        raiseDbError("WHEN expression must be of type BOOLEAN")
      if cond.boolVal:
        return eval(exp.whens[i].exp, varResolver, aggrResolver)
  result = if exp.elseExp != nil: eval(exp.elseExp, varResolver, aggrResolver)
           else: NqValue(kind: nqkNull)

method eval*(exp: TrimExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  let src = eval(exp.src, varResolver, aggrResolver)
  if exp.char != nil:
    let ch = eval(exp.char, varResolver, aggrResolver)
    if ch.kind != nqkString or ch.strVal.runeLen != 1:
      raiseDbError("single character expected")
    result = NqValue(kind: nqkString,
                     strVal: unicode.strip(src.strVal, exp.leading, exp.trailing, [runeAt(ch.strVal, 0)]))
  else:
    result = NqValue(kind: nqkString,
                     strVal: unicode.strip(src.strVal, exp.leading, exp.trailing))

proc `$`*(val: NqValue): string =
  case val.kind
    of nqkInt:
      result = $val.intVal
    of nqkNumeric:
      result = $val.numericVal
      if val.scale > 0:
        result = result[0 .. result.len - 1 - val.scale] & "." &
            result[result.len - val.scale .. ^1]
    of nqkBigint:
      result = $val.bigintVal
    of nqkFloat: result = $val.floatVal
    of nqkString: result = $val.strVal
    of nqkBool: result = if val.boolVal: "TRUE" else: "FALSE"
    of nqkTime:
      result = &"{val.microsecond div 3600000000:02}" & ":" &
               &"{(val.microsecond mod 3600000000) div 60000000:02}" & ":" &
               &"{val.microsecond mod 60000000 div 1000000:02}"
      if val.precision > 0:
        let frac = &"{val.microsecond mod 1000000:06}"
        result &= "."
        result &= frac.substr(0, val.precision - 1)
    of nqkDate: result = &"{val.year:04}" & "-" &
                         &"{ord(val.month):02}" & "-" &
                         &"{val.day:02}"
    of nqkTimestamp:
      var dt = baseDate + initDuration(nanoseconds = val.microsecond * 1000)
      result = dt.format("yyyy-MM-dd HH:mm:ss")
      if val.precision > 0:
        let frac = &"{dt.nanosecond mod 1000000000:06}"
        result &= "."
        result &= frac.substr(0, val.precision - 1)
    of nqkNull: result = ""
    of nqkList: raiseDbError("conversion of list to string not supported")

method eval*(exp: CastExp, varResolver: VarResolver,
    aggrResolver: AggrResolver): NqValue =
  let arg = eval(exp.exp, varResolver, aggrResolver)
  var coldef: ColumnDef
  case typeKind(exp.typeDef.typ)
    of kInt:
      let v = toInt64(arg)
      if v > high(int32) or v < low(int32):
        raiseDBError("value out of range for int")
      result = NqValue(kind: nqkInt, intVal: int32(v))
    of kNumeric:
      result = toNumeric(arg)

      # Check if result is compatible with target type
      coldef.typ = exp.typeDef.typ
      coldef.precision = exp.typeDef.precision
      coldef.scale = exp.typeDef.scale
      discard toMatValue(result, coldef)
    of kFloat:
      result = NqValue(kind: nqkFloat, floatVal: toFloat(arg))
    of kString:
      result = NqValue(kind: nqkString, strVal: $arg)
      coldef.typ = exp.typeDef.typ
      coldef.size = exp.typeDef.size
      discard toMatValue(result, coldef)
    of kBool:
      result = NqValue(kind: nqkBool, boolVal: toBool(arg))
    of kNull:
      raiseDbError("internal error")
    of kBigint:
      result = NqValue(kind: nqkBigint, bigintVal: toInt64(arg))
    of kTime:
      if exp.typeDef.typ == "TIMESTAMP":
        let t = toTimestamp($arg)
        result = NqValue(kind: nqkTimestamp,
                         microsecond: withPrecision(t.microsecond, exp.typeDef.precision),
                         precision: exp.typeDef.precision)
      else:
        let t = toTime($arg)
        result = NqValue(kind: nqkTime,
                         microsecond: precisionMicroseconds(t.microsecond, exp.typeDef.precision),
                         precision: exp.typeDef.precision)
    of kDate: result = toDate($arg)

method columnCount(table: BaseTableRef): Natural =
  result = HashBaseTable(table.table).def.len

method columnCount(table: WhereTable): Natural =
  result = columnCount(table.child)

method columnCount(table: ProjectTable): Natural =
  result = table.columns.len

method columnCount(table: GroupTable): Natural =
  result = table.columns.len

method columnNo*(rtable: BaseTable, name: string): int {.base.} =
  raiseDbError("not implemented")

method columnNo(rtable: HashBaseTable, name: string): int =
  for i in 0..<rtable.def.len:
    if rtable.def[i].name == name:
      return i
  return -1

method columnNo(tableRef: BaseTableRef, name: string, tableName: string): int =
  if tableName != "" and tableRef.rangeVar != tableName:
    return -1
  result = HashBaseTable(tableRef.table).columnNo(name)

method columnNo(rtable: WhereTable, name: string, tableName: string): int =
  return rtable.child.columnNo(name, tableName)

method columnNo(rtable: ProjectTable, name: string, tableName: string): int =
  for i in 0..<rtable.columns.len:
    if rtable.columns[i].colName == name:
      return i
  result = -1

method columnNo(table: GroupTable, name: string, tableName: string): int =
  for i in 0..<table.columns.len:
    if table.columns[i].colName == name:
      return i
  result = -1

func columnValueAt(table: HashBaseTable, keyRecord: Record, col: Natural): NqValue =
  let ki = keyIndex(table, col)
  if ki != -1:
    return toNqValue(keyRecord[ki], table.def[col])
  var vi = 0
  for i in 0..<table.def.len:
    if not isKey(table, i):
      if i == col:
        return toNqValue(table.rows[keyRecord][vi], table.def[col])
      vi = vi + 1
  result = NqValue(kind: nqkNull)

func columnValueAt*(row: InstantRow; col: Natural): NqValue =
  if row.material:
    result = columnValueAt(HashBaseTable(BaseTableRef(row.table).table), row.keyRecord, col)
  else:
    result = row.vals[col]

func columnNo(table: VTable, colRef: QVarExp): int =
  result = columnNo(table, colRef.name, colRef.tableName)

func newGroupTable*(table: VTable, aggrs: seq[Expression], groupBy: seq[
    QVarExp]): VTable =
  for colRef in groupBy:
    if columnNo(table, colRef) == -1:
      raiseDbError("column " & (if colRef.tableName != "": colRef.tableName &
          "." else: "") & colRef.name & " does not exist")
  if table of ProjectTable:
    result = GroupTable(child: ProjectTable(table).child,
                       groupBy: groupBy, columns: ProjectTable(table).columns)
  else:
    result = GroupTable(child: table, groupBy: groupBy, columns: @[])

proc setColumnValueAt*(table: HashBaseTable; keyRecord: var Record[MatValue],
    col: Natural, val: MatValue) =
  let ki = keyIndex(table, col)
  if ki != -1:
    keyRecord[ki] = val
    return
  var vi = 0
  for i in 0..<table.def.len:
    if not isKey(table, i):
      if i == col:
        table.rows[keyRecord][vi] = val
        return
      vi = vi + 1

proc setColumnValueAt(table: HashBaseTable; keyRecord: var Record[MatValue],
    col: Natural, val: NqValue) =
  setColumnValueAt(table, keyRecord, col, toMatValue(val, table.def[col]))

proc setColumnValueAt*(row: var InstantRow; col: Natural, val: NqValue) =
  setColumnValueAt(HashBaseTable(BaseTableRef(row.table).table), row.keyRecord,
                   col, val)

iterator instantRows*(tableRef: BaseTableRef): InstantRow {.closure.} =
  for k in HashBaseTable(tableRef.table).rows.keys():
    yield InstantRow(table: tableRef, material: true, keyRecord: k)

method newCursor(rtable: BaseTableRef, args: openArray[string]): Cursor =
  result = TableRefCursor(args: @args, iter: instantRows, tableRef: rtable)

method next(cursor: TableRefCursor, row: var InstantRow,
    varResolver: VarResolver = nil): bool =
  row = cursor.iter(cursor.tableRef)
  result = not finished(cursor.iter)

func isConst*(exp: Expression): bool =
  result = (exp of ScalarLit) or
      (exp of QVarExp) and (QVarExp(exp).name[0] == '$')

proc isKeyColumn(tableRef: BaseTableRef, exp: Expression): bool =
  if exp of QVarExp:
    let colRef = QVarExp(exp)
    let col = findColumnName(tableRef.table.def, colRef.name)
    if col == -1:
      return false
    if not isKey(tableRef.table, col):
      return false
    if colRef.tableName == "":
      return true
    if tableRef.rangeVar == "":
      result = colRef.tableName == tableRef.table.name
    else:
      result = colRef.tableName == tableRef.rangeVar

func cmpCol(a, b: tuple[colNo: Natural, exp: Expression]): int =
  result = a.colNo - b.colNo

proc expKeyCols*(exp: Expression, tableRef: BaseTableRef, isConstProc: proc(exp: Expression): bool,
                args: openArray[string]): seq[tuple[colNo: Natural,
                    exp: Expression]] =
  # If exp is of the form (col1 = val1 and col2 = val2 and ...) with col1, col2 ... being primaryKey columns,
  # return the column/value pairs, sorted by column number, otherwise an empty sequence
  if exp of ScalarOpExp:
    let opexp = ScalarOpExp(exp)
    if opexp.opName == "=" and
        ((isConstProc(opexp.args[0]) and isKeyColumn(tableRef, opexp.args[1]) or
         isKeyColumn(tableRef, opexp.args[0]) and isConstProc(opexp.args[1]))):
      var colRef: QVarExp
      var argExp: Expression
      if isConstProc(opexp.args[1]):
        colRef = QVarExp(opexp.args[0])
        argExp = opexp.args[1]
      else:
        colRef = QVarExp(opexp.args[1])
        argExp = opexp.args[0]
      let colNo = columnNo(tableRef.table, colRef.name)
      result.add((colNo: Natural(colNo), exp: argExp))
    elif opexp.opName == "AND":
      result = expKeyCols(opexp.args[0], tableRef, isConstProc, args) &
          expKeyCols(opexp.args[1], tableRef, isConstProc, args)
  result.sort(cmpCol)
  # Check for duplicate column
  for i in 0..<result.len - 1:
    if result[i].colNo == result[i + 1].colNo:
      return @[]

proc expKeyColVals(exp: Expression, tableRef: BaseTableRef, isConstProc: proc(exp: Expression): bool,
                args: openArray[string]): seq[tuple[colNo: Natural,
                    val: MatValue]] =
  let cols = expKeyCols(exp, tableRef, isConstProc, args)
  var val: NqValue
  for c in cols:
    if c.exp of QVarExp:
      let arg = QVarExp(c.exp)
      if arg.name[0] != '$':
        raiseDbError("column " & arg.name & " does not exist")
      val = NqValue(kind: nqkString,
                        strVal: args[parseInt(arg.name[1..arg.name.high]) - 1])
    else:
      val = eval(c.exp, nil, nil)
    result.add((colNo: c.colNo, val: toMatValue(val, tableRef.table.def[c.colNo])))

proc whereTableKey(table: WhereTable, args: openArray[string]): Record[MatValue] =
  if not (table.child of BaseTableRef): # or ProjectTable of BaseTable
    return
  let baseTable = BaseTableRef(table.child)
  var keyCols = expKeyColVals(table.whereExp, baseTable, isConst, args)
  if keyCols.len == baseTable.table.primaryKey.len:
    for c in keyCols:
      result.add(c.val)

method newCursor(rtable: WhereTable, args: openArray[string]): Cursor =
  let key = whereTableKey(rtable, args)
  result = if key.len > 0:
      WhereTableCursor(args: @args,
                       table: rtable,
                       key: key,
                       beforeFirstRow: true)
    else:
      WhereTableCursor(args: @args,
                       cursor: newCursor(rtable.child, args),
                       table: rtable,
                       beforeFirstRow: true)

method next(cursor: WhereTableCursor, row: var InstantRow,
    varResolver: VarResolver = nil): bool =
  if cursor.key.len > 0:
    if cursor.beforeFirstRow:
      row = InstantRow(table: cursor.table.child,
                       material: true,
                       keyRecord: cursor.key)
      cursor.beforeFirstRow = false
      let rowCopy = row
      let val = eval(cursor.table.whereExp, proc(name: string,
          rangeVar: string): NqValue =
        if name[0] == '$':
          return NqValue(kind: nqkString,
                          strVal: cursor.args[parseInt(name[1..name.high]) - 1])
        let col = columnNo(cursor.table, name, rangeVar)
        if col == -1:
          if varResolver != nil:
            return varResolver(name, rangeVar)
          raiseDbError("column " & (if rangeVar != "": rangeVar &
              "." else: "") & name & " does not exist")
        return columnValueAt(rowCopy, col))
      return val.kind != nqkNull and val.boolVal
    else:
      return false
  if (not cursor.cursor.next(row, varResolver)):
    return false
  while true:
    let rowCopy = row
    let val = eval(cursor.table.whereExp, proc(name: string,
        rangeVar: string): NqValue =
      if name[0] == '$':
        return NqValue(kind: nqkString,
                         strVal: cursor.args[parseInt(name[1..name.high]) - 1])
      let col = columnNo(cursor.table, name, rangeVar)
      if col == -1:
        if varResolver != nil:
          return varResolver(name, rangeVar)
        raiseDbError("column " & (if rangeVar != "": rangeVar & "." else: "") &
            name & " does not exist")
      return columnValueAt(rowCopy, col))
    if val.kind != nqkNull and val.boolVal:
      return true
    if not cursor.cursor.next(row):
      return false

method newCursor(rtable: ProjectTable, args: openArray[string]): Cursor =
  result = ProjectTableCursor(args: @args, cursor: newCursor(rtable.child,
      args), table: rtable)

method next(cursor: ProjectTableCursor, row: var InstantRow,
    varResolver: VarResolver = nil): bool =
  var baseRow: InstantRow
  if (not cursor.cursor.next(baseRow, varResolver)):
    return false
  var vals = seq[NqValue](@[])
  for col in cursor.table.columns:
    if getAggrs(col.exp).len == 0:
      vals.add(eval(col.exp, proc(name: string, rangeVar: string): NqValue =
        if name[0] == '$':
          return NqValue(kind: nqkString,
                           strVal: cursor.args[parseInt(name[1..name.high]) - 1])
        let col = columnNo(cursor.table.child, name, rangeVar)
        if col == -1:
          raiseDbError("column " & (if rangeVar != "": rangeVar &
              "." else: "") & name & " does not exist")
        return columnValueAt(baseRow, col)))
    else:
      vals.add(NqValue(kind: nqkNull))
  row = InstantRow(table: cursor.table, material: false, vals: vals)
  result = true

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
        "." else: "") & colRef.name & " does not exist")
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric")
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
        "." else: "") & colRef.name & " does not exist")
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric")
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
        "." else: "") & colRef.name & " does not exist")
  result = NqValue(kind: nqkNull)
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric")
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
        "." else: "") & colRef.name & " does not exist")
  var n: int64 = 0
  var avg: float
  for row in instantRows(table, args):
    if groupByVals == groupVals(groupBy, columns, row):
      let val = columnValueAt(row, col)
      if val.kind != nqkNull:
        if val.kind != nqkNumeric and val.kind != nqkFloat and val.kind != nqkInt:
          raiseDbError("column is not numeric")
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
                     colRef.name & " does not exist")
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
        raiseDbError(name & " does not exist"),
          proc(exp: ScalarOpExp): NqValue =
        case exp.opName
          of "COUNT":
            result = aggrCount(cursor.table.child, cursor.table.groupBy,
                               cursor.table.columns, k, cursor.args)
          of "AVG", "MAX", "MIN", "SUM":
            if exp.args.len != 1:
              raiseDbError("1 argument to " & exp.opName & " required")
            if not (exp.args[0] of QVarExp):
              raiseDbError("column reference required")
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

func isKeyUpdate*(table: BaseTable, assignments: seq[ColumnAssignment]): bool =
  for a in assignments:
    for k in table.primaryKey:
      if a.col == k:
        return true
  result = false

method `$`(vtable: VTable): string = nil

method `$`(vtable: BaseTableRef): string =
  result = vtable.table.name
  if vtable.rangeVar != "":
    result = result & " " & vtable.rangeVar

method `$`(vtable: WhereTable): string =
  result = $vtable.child
  if vtable.whereExp != nil:
    result = result & ' ' & $vtable.whereExp
