#
# Copyright (c) 2020, 2022 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
# 
import nqcommon
import sqlscanner
import strutils

type
  SqlTableRefKind* = enum trkSimpleTableRef, trkRelOp
  SqlTableRef* = ref object
    case kind*: SqlTableRefKind
      of trkSimpleTableRef:
        name*: string
        rangeVar*: string
      of trkRelOp:
        tableRef1*, tableRef2*: SqlTableRef
        onExp*: Expression
        leftOuter*: bool

  SqlStatement* = ref object of RootObj
  SqlSelect* = ref object
    columns*: seq[SelectElement]
    tables*: seq[SqlTableRef]
    whereExp*: Expression
    groupBy*: seq[QVarExp]
    allowDuplicates*: bool
  SqlCreateTable* = ref object of SqlStatement
    tableName*: string
    columns*: seq[ColumnDef]
    primaryKey*: seq[string]
  SqlDropTable* = ref object of SqlStatement
    tableName*: string
    ifExists*: bool
  SqlInsertKind* = enum ikValues, ikSelect
  SqlInsert* = ref object of SqlStatement
    tableName*: string
    columns*: seq[string]
    case kind*: SqlInsertKind
      of ikValues:
        values*: seq[Expression]
      of ikSelect:
        select*: TableExp
  UpdateAssignment* = object
    column*: string
    src*: Expression
  SqlUpdate* = ref object of SqlStatement
    tableName*: string
    updateAssignments*: seq[UpdateAssignment]
    whereExp*: Expression
  SqlDelete* = ref object of SqlStatement
    tableName*: string
    whereExp*: Expression
  SqlCommit* = ref object of SqlStatement
  SqlRollback* = ref object of SqlStatement
  OrderByElement* = object
    name*: string
    tableName*: string
    asc*: bool
  TableExpKind* = enum tekSelect, tekUnion, tekExcept, tekIntersect
  TableExp* {.acyclic.} = ref object of Expression
    case kind*: TableExpKind
      of tekSelect:
        select*: SqlSelect
      of tekUnion, tekExcept, tekIntersect:
        exp1*, exp2*: TableExp
        allowDuplicates*: bool
  NamedTableExp* = ref object
      name*: string
      exp*: TableExp
  QueryExp* = ref object of SqlStatement
    withExps*: seq[NamedTableExp]
    tableExp*: TableExp
    orderBy*: seq[OrderByElement]

proc parseExpression(scanner: Scanner, argCount: var int, before: bool = true): Expression

proc parseTableExp(scanner: Scanner, argCount: var int): TableExp

proc parseType(scanner: Scanner): TypeDef =
  let typeTok = nextToken(scanner)
  var typ: string
  var precision = 0
  var scale = 0
  var size = 0
  case typeTok.kind
    of tokIdentifier:
      typ = typeTok.identifier
      discard nextToken(scanner)
    of tokNumeric, tokDecimal:
      typ = if typeTok.kind == tokNumeric: "NUMERIC" else: "DECIMAL"
      var t = nextToken(scanner)
      if t.kind == tokLeftParen:
        t = nextToken(scanner)
        if t.kind != tokInt:
          raiseDbError("number expected", syntaxError)
        precision = parseInt(t.value)
        t = nextToken(scanner)
        if t.kind == tokComma:
          t = nextToken(scanner)
          if t.kind != tokInt:
            raiseDbError("number expected", syntaxError)
          scale = parseInt(t.value)
          t = nextToken(scanner)
        if t.kind != tokRightParen:
          raiseDbError("\")\" expected", syntaxError)
        discard nextToken(scanner)
    of tokChar:
      typ = "CHAR"
      var t = nextToken(scanner)
      if t.kind == tokLeftParen:
        t = nextToken(scanner)
        if t.kind != tokInt:
          raiseDbError("number expected", syntaxError)
        size = parseInt(t.value)
        t = nextToken(scanner)
        if t.kind != tokRightParen:
          raiseDbError("\")\" expected", syntaxError)
        discard nextToken(scanner)
      else:
        size = 1
    of tokVarchar:
      typ = "VARCHAR"
      var t = nextToken(scanner)
      if t.kind != tokLeftParen:
        raiseDbError("\")\" expected", syntaxError)
      t = nextToken(scanner)
      if t.kind != tokInt:
        raiseDbError("number expected", syntaxError)
      size = parseInt(t.value)
      t = nextToken(scanner)
      if t.kind != tokRightParen:
        raiseDbError("\")\" expected", syntaxError)
      discard nextToken(scanner)
    of tokDouble:
      if nextToken(scanner).kind != tokPrecision:
        raiseDbError("PRECISION expected", syntaxError)
      typ = "REAL"
      discard nextToken(scanner)
    of tokTime:
      var t = nextToken(scanner)
      if t.kind == tokLeftParen:
        t = nextToken(scanner)
        if t.kind != tokInt:
          raiseDbError("number expected", syntaxError)
        precision = parseInt(t.value)
        t = nextToken(scanner)
        if t.kind != tokRightParen:
          raiseDbError("\")\" expected", syntaxError)
        discard nextToken(scanner)
      else:
        precision = 0
      typ = "TIME"
    of tokTimestamp:
      var t = nextToken(scanner)
      if t.kind == tokLeftParen:
        t = nextToken(scanner)
        if t.kind != tokInt:
          raiseDbError("number expected", syntaxError)
        precision = parseInt(t.value)
        t = nextToken(scanner)
        if t.kind != tokRightParen:
          raiseDbError("\")\" expected", syntaxError)
        discard nextToken(scanner)
      else:
        precision = 6
      typ = "TIMESTAMP"
    else:
      raiseDbError("type expected", syntaxError)
  return TypeDef(typ: typ, size: size, precision: precision, scale: scale)

proc parseSum(scanner: Scanner, argCount: var int): Expression

proc parseOperand(scanner: Scanner, argCount: var int): Expression =
  var t = currentToken(scanner)
  var sign = 0
  if t.kind == tokPlus or t.kind == tokMinus:
    sign = if t.kind == tokPlus: 1 else: -1
    t = nextToken(scanner)
  case t.kind:
    of tokString:
      if sign != 0:
        raiseDbError("invalid " & (if sign == 1: "+" else: "-"), syntaxError)
      result = newStringLit(t.value)
    of tokInt, tokRat:
      result = newNumericLit(t.value)
    of tokCount:
      t = nextToken(scanner)
      if t.kind != tokLeftParen:
        raiseDbError("\"(\" expected", syntaxError)
      t = nextToken(scanner)
      if t.kind == tokAsterisk:
        result = newScalarOpExp("COUNT")
        discard nextToken(scanner)
      else:
        result = newScalarOpExp("COUNT", parseExpression(scanner, argCount, false))
      if currentToken(scanner).kind != tokRightParen:
        raiseDbError("\")\" expected", syntaxError)
    of tokCast:
      t = nextToken(scanner)
      if t.kind != tokLeftParen:
        raiseDbError("\"(\" expected", syntaxError)
      let exp = parseExpression(scanner, argCount, true)
      if currentToken(scanner).kind != tokAs:
        raiseDbError("AS expected", syntaxError)
      result = newCastExp(exp, parseType(scanner))
      if currentToken(scanner).kind != tokRightParen:
        raiseDbError("\")\" expected", syntaxError)
    of tokIdentifier:
      if t.identifier == "DATE":
        t = nextToken(scanner)
        if t.kind != tokString or not isValidDate(t.value):
          raiseDbError("invalid date", invalidDatetimeValue)
        result = newDateLit(t.value)
      else:
        let t2 = nextToken(scanner)
        if t2.kind == tokLeftParen:
          let opname = t.identifier
          var exps: seq[Expression]
          while true:
            exps.add(parseExpression(scanner, argCount))
            t = currentToken(scanner)
            if t.kind == tokRightParen:
              break
            if t.kind != tokComma:
              raiseDbError("\")\" or comma expected", syntaxError)
          result = newScalarOpExp(opname, exps)
        elif t2.kind == tokDot:
          let colToken = nextToken(scanner)
          if colToken.kind != tokIdentifier:
            raiseDbError("identifier expected", syntaxError)
          result = newQVarExp(colToken.identifier, t.identifier)
        else:
          result = newQVarExp(t.identifier)
          if sign == -1:
            result = newScalarOpExp("-", result)
          return result
    of tokLeftParen:
      if nextToken(scanner).kind == tokSelect:
        result = parseTableExp(scanner, argCount)
      else:
        result = parseExpression(scanner, argCount, false)
      if currentToken(scanner).kind != tokRightParen:
        raiseDbError("\")\" expected", syntaxError)
    of tokPlaceholder:
      argCount = argCount + 1
      discard nextToken(scanner)
      return newQVarExp("$" & $argCount)
    of tokNumPlaceholder:
      t = nextToken(scanner)
      if t.kind != tokInt:
        raiseDbError("number expected after \"$\"", syntaxError)
      discard nextToken(scanner)
      return newQVarExp("$" & t.value)
    of tokTrue:
      result = newBoolLit(true)
    of tokFalse:
      result = newBoolLit(false)
    of tokNull:
      result = newNullLit()
    of tokTime:
      t = nextToken(scanner)
      if t.kind != tokString or not isValidTime(t.value):
        raiseDbError("invalid time", invalidDatetimeValue)
      result = newTimeLit(t.value)
    of tokTimestamp:
      t = nextToken(scanner)
      if t.kind != tokString or not isValidTimestamp(t.value):
        raiseDbError("invalid timestamp", invalidDatetimeValue)
      result = newTimestampLit(t.value)
    of tokCase:
      var exp, elseExp: Expression
      var whens: seq[tuple[cond: Expression, exp: Expression]]
      var t = nextToken(scanner)
      if t.kind != tokWhen:
        exp = parseExpression(scanner, argCount, false)
        t = currentToken(scanner)
      while t.kind == tokWhen:
        let whenExp = parseExpression(scanner, argCount)
        if currentToken(scanner).kind != tokThen:
          raiseDbError("THEN expected", syntaxError)
        let exp = parseExpression(scanner, argCount)
        whens.add((cond: whenExp, exp: exp))
        t = currentToken(scanner)
      if t.kind == tokElse:
        elseExp = parseExpression(scanner, argCount)
      elif t.kind != tokEnd: 
        raiseDbError("WHEN, ELSE, or END expected", syntaxError)
      if currentToken(scanner).kind != tokEnd:
        raiseDbError("END expected", syntaxError)
      result = newCaseExp(exp, whens, elseExp)
    of tokTrim:
      var exp: Expression
      t = nextToken(scanner)
      if t.kind != tokLeftParen:
        raiseDbError("\"(\" expected", syntaxError)
      t = nextToken(scanner)
      case t.kind:
        of tokLeading, tokTrailing, tokBoth:
          var leading: bool
          var trailing: bool
          case t.kind:
            of tokLeading:
              leading = true
              trailing = false
            of tokTrailing:
              leading = false
              trailing = true
            of tokBoth:
              leading = true
              trailing = true
            else:
              discard # Cannot be reached
          t = nextToken(scanner)
          if t.kind == tokFrom:
            result = newScalarOpExp("TRIM", parseExpression(scanner, argCount),
                                    newBoolLit(leading), newBoolLit(trailing), newNullLit())
          else:
            exp = parseExpression(scanner, argCount, false)
            if currentToken(scanner).kind != tokFrom:
              raiseDbError("FROM expected", syntaxError)
            result = newScalarOpExp("TRIM", parseExpression(scanner, argCount),
                                    newBoolLit(leading), newBoolLit(trailing), exp)
        else:
          exp = parseExpression(scanner, argCount, false)
          if currentToken(scanner).kind == tokFrom:
            result = newScalarOpExp("TRIM", parseExpression(scanner, argCount),
                                    newBoolLit(true), newBoolLit(true), exp)
          else:
            result = newScalarOpExp("TRIM", exp, newBoolLit(true), newBoolLit(true), newNullLit())
      if currentToken(scanner).kind != tokRightParen:
        raiseDbError(") expected", syntaxError)
    of tokPosition:
      t = nextToken(scanner)
      if t.kind != tokLeftParen:
        raiseDbError("\"(\" expected", syntaxError)
      discard nextToken(scanner)
      let subs = parseSum(scanner, argCount)
      t = currentToken(scanner)
      if t.kind != tokIn:
        raiseDbError("IN expected, found " & $t, syntaxError)
      result = newScalarOpExp("POSITION", [subs, parseExpression(scanner, argCount)])
      if currentToken(scanner).kind != tokRightParen:
        raiseDbError(") expected", syntaxError)
    else:
      raiseDbError("unsupported primitive: " & $t.kind, syntaxError)
  discard nextToken(scanner) 
  if sign == -1:
     result = newScalarOpExp("-", result)

proc parseTerm(scanner: Scanner, argCount: var int): Expression =
  var exp = parseOperand(scanner, argCount)
  var t = currentToken(scanner)
  while t.kind == tokAsterisk or t.kind == tokDiv:
    discard nextToken(scanner)
    let exp2 = parseOperand(scanner, argCount)
    exp = newScalarOpExp(if t.kind == tokAsterisk: "*" else: "/", exp, exp2)
    t = currentToken(scanner)
  result = exp

proc parseSum(scanner: Scanner, argCount: var int): Expression =
  var exp = parseTerm(scanner, argCount)
  var t = currentToken(scanner)
  while t.kind == tokPlus or t.kind == tokMinus or t.kind == tokConcat:
    discard nextToken(scanner)
    let exp2 = parseOperand(scanner, argCount)
    case t.kind:
      of tokPlus:
        exp = newScalarOpExp("+", exp, exp2)
      of tokMinus:
        exp = newScalarOpExp("-", exp, exp2)
      of tokConcat:
        exp = newScalarOpExp("||", exp, exp2)
      else:
        raiseDbError("internal error: invalid operator " & $t, internalError)
    t = currentToken(scanner)
  result = exp

proc parseExpListOrTableExp(scanner: Scanner, argCount: var int): Expression =
  if currentToken(scanner).kind != tokLeftParen:
    raiseDbError("\"(\" expected", syntaxError)
  if nextToken(scanner).kind == tokSelect:
    result = parseTableExp(scanner, argCount)
    if currentToken(scanner).kind != tokRightParen:
      raiseDbError("\")\" expected", syntaxError)
    discard nextToken(scanner)
  else:
    var exps = @[parseExpression(scanner, argCount, false)]
    while currentToken(scanner).kind == tokComma:
      exps.add(parseExpression(scanner, argCount))
    if currentToken(scanner).kind != tokRightParen:
      raiseDbError("\")\" expected", syntaxError)
    discard nextToken(scanner)
    result = newListExp(exps)

proc parseSumOrAllOrAny(scanner: Scanner, argCount: var int): Expression =
  let t = currentToken(scanner)
  if t.kind == tokAll or t.kind == tokAny:
    if nextToken(scanner).kind != tokLeftParen:
      raiseDbError("\"(\" expected", syntaxError)
    if nextToken(scanner).kind != tokSelect:
      raiseDbError("SELECT expected", syntaxError)
    result = newScalarOpExp(if t.kind == tokAll: "ALL" else: "ANY",
                            parseTableExp(scanner, argCount))
    if currentToken(scanner).kind != tokRightParen:
      raiseDbError("\")\" expected", syntaxError)
  else:
    result = parseSum(scanner, argCount)

proc parseCondPrimitive(scanner: Scanner, argCount: var int): Expression =
  # The first token has been read
  if currentToken(scanner).kind == tokExists:
    if nextToken(scanner).kind != tokLeftParen:
      raiseDbError("\"(\" expected", syntaxError)
    if nextToken(scanner).kind != tokSelect:
      raiseDbError("SELECT expected", syntaxError)
    result = newScalarOpExp("EXISTS", parseTableExp(scanner, argCount))
    if currentToken(scanner).kind != tokRightParen:
      raiseDbError("\")\" expected", syntaxError)
    discard nextToken(scanner)
    return
  var exp = parseSum(scanner, argCount)
  case currentToken(scanner).kind:
    of tokEq:
      discard nextToken(scanner)
      return newScalarOpExp("=", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokNeq:
      discard nextToken(scanner)
      return newScalarOpExp("<>", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokLt:
      discard nextToken(scanner)
      return newScalarOpExp("<", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokLe:
      discard nextToken(scanner)
      return newScalarOpExp("<=", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokGt:
      discard nextToken(scanner)
      return newScalarOpExp(">", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokGe:
      discard nextToken(scanner)
      return newScalarOpExp(">=", exp, parseSumOrAllOrAny(scanner, argCount))
    of tokLike:
      discard nextToken(scanner)
      return newScalarOpExp("LIKE", exp, parseSum(scanner, argCount))
    of tokIn:
      discard nextToken(scanner)
      return newScalarOpExp("IN", exp, parseExpListOrTableExp(scanner, argCount))
    of tokNot:
      if nextToken(scanner).kind != tokIn:
        raiseDbError("IN expected", syntaxError)
      discard nextToken(scanner)      
      return newScalarOpExp("NOT",
                 newScalarOpExp("IN", exp,
                                parseExpListOrTableExp(scanner, argCount)))
    of tokIs:
      var t = nextToken(scanner)
      var notNull = false
      if t.kind == tokNot:
        notNull = true
        t = nextToken(scanner)
      if t.kind != tokNull:
        raiseDbError("NULL expected", syntaxError)
      discard nextToken(scanner)
      result = newScalarOpExp("isNull", exp)
      if notNull:
        result = newScalarOpExp("NOT", result)
    else:
      result = exp

proc parseCondOperand(scanner: Scanner, argCount: var int): Expression =
  let t = currentToken(scanner)
  if t.kind == tokNot:
    discard nextToken(scanner)
    let exp = parseCondPrimitive(scanner, argCount)
    return newScalarOpExp("NOT", exp)
  return parseCondPrimitive(scanner, argCount)

proc parseCondTerm(scanner: Scanner, argCount: var int): Expression =
  var exp = parseCondOperand(scanner, argCount)
  var t = currentToken(scanner)
  while t.kind == tokAnd:
    discard nextToken(scanner)
    let exp2 = parseCondOperand(scanner, argCount)
    exp = newScalarOpExp("AND", exp, exp2)
    t = currentToken(scanner)
  return exp

proc parseExpression(scanner: Scanner, argCount: var int, before: bool = true):
    Expression =
  if before:
    discard nextToken(scanner)
  var exp = parseCondTerm(scanner, argCount)
  var t = currentToken(scanner)
  while t.kind == tokOr:
    discard nextToken(scanner)
    let exp2 = parseCondTerm(scanner, argCount)
    exp = newScalarOpExp("OR", exp, exp2)
    t = currentToken(scanner)
  result = exp

func litTokToValue(lit: Token): Expression =
  case lit.kind:
    of tokString:
      result = StringLit(val: lit.value)
    of tokRat, tokInt:
      result = NumericLit(val: lit.value)
    of tokFalse:
      result = BoolLit(val: "FALSE")
    of tokTrue:
      result = BoolLit(val: "TRUE")
    of tokNull:
      result = NullLit()
    else:
      raiseDbError("invalid value", syntaxError)

proc parseColumn(scanner: Scanner): ColumnDef =
  ## Parses a column definition, including the trailing comma or paren
  let nameTok = currentToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("column name expected", syntaxError)
  var defValue: Expression = nil
  var pk = false;
  var notNull = false;
  var autoinc = false;
  let typdef = parseType(scanner)
  var t = currentToken(scanner)
  if t.kind == tokDefault:
    t = nextToken(scanner)
    defValue = litTokToValue(t)
    t = nextToken(scanner)
  while t.kind == tokPrimary or t.kind == tokNot or t.kind == tokAutoincrement:
    case t.kind:
    of tokPrimary:
      t = nextToken(scanner)
      if t.kind != tokKey:
        raiseDbError("KEY expected", syntaxError)
      pk = true
    of tokNot:
      t = nextToken(scanner)
      if t.kind != tokNull:
        raiseDbError("NULL expected", syntaxError)
      notNull = true
    of tokAutoincrement:
      autoinc = true
    else: discard
    t = nextToken(scanner)
  return ColumnDef(name: nameTok.identifier, typ: typdef.typ, 
                   size: typdef.size, precision: typdef.precision,
                   scale: typdef.scale,
                   notNull: notNull, defaultValue: defValue,
                   primaryKey: pk, autoincrement: autoinc)

proc parseTablePrimaryKey(scanner: Scanner): seq[string] =
  if nextToken(scanner).kind != tokKey:
    raiseDbError("KEY expected", syntaxError)
  if nextToken(scanner).kind != tokLeftParen:
    raiseDbError("( expected", syntaxError)
  var colTok = nextToken(scanner)
  if colTok.kind != tokIdentifier:
    raiseDbError("Identifier expected", syntaxError)
  result = @[colTok.identifier]
  var t = nextToken(scanner)
  while t.kind == tokComma:
    colTok = nextToken(scanner)
    if colTok.kind != tokIdentifier:
      raiseDbError("Identifier expected", syntaxError)
    result.add(colTok.identifier)
    t = nextToken(scanner)
  if t.kind != tokRightParen:
    if currentToken(scanner).kind != tokRightParen:
      raiseDbError(") expected", syntaxError)
  discard nextToken(scanner)

proc parseCreate(scanner: Scanner): SqlStatement =
  var pKey: seq[string]
  var t = nextToken(scanner)
  if t.kind != tokTable:
    raiseDbError("TABLE expected", syntaxError)
  let nameTok = nextToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  t = nextToken(scanner)
  if t.kind != tokLeftParen:
    raiseDbError("( expected", syntaxError)
  var cols: seq[ColumnDef]
  if nextToken(scanner).kind == tokPrimary:
    if pKey.len > 0:
      raiseDbError("multiple primary keys are not allowed", syntaxError)
    pKey = parseTablePrimaryKey(scanner)
  else:
    cols.add(parseColumn(scanner))
  while currentToken(scanner).kind == tokComma:
    if nextToken(scanner).kind == tokPrimary:
      if pKey.len > 0:
        raiseDbError("multiple primary keys are not allowed", syntaxError)
      pKey = parseTablePrimaryKey(scanner)
    else:
      cols.add(parseColumn(scanner))
  if currentToken(scanner).kind != tokRightParen:
    raiseDbError(") expected", syntaxError)
  result = SqlCreateTable(tableName: nameTok.identifier, columns: cols,
                          primaryKey: pkey)

proc parseDrop(scanner: Scanner): SqlStatement =
  var ifExists = false
  var t = nextToken(scanner)
  if t.kind != tokTable:
    raiseDbError("TABLE expected", syntaxError)
  t = nextToken(scanner)
  if t.kind == tokIf:
    if nextToken(scanner).kind != tokExists:
      raiseDbError("EXISTS expected", syntaxError)
    ifExists = true
    t = nextToken(scanner)
    if t.kind != tokIdentifier:
      raiseDbError("identifier expected", syntaxError)
  elif t.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  result = SqlDropTable(tableName: t.identifier, ifExists: ifExists)

proc parseInsert(scanner: Scanner): SqlStatement =
  var t = nextToken(scanner)
  if t.kind != tokInto:
    raiseDbError("INTO expected", syntaxError)
  let nameTok = nextToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  var columns: seq[string]
  t = nextToken(scanner)
  if t.kind == tokDefault:
    if nextToken(scanner).kind == tokValues:
      return SqlInsert(tableName: nameTok.identifier, columns: @[], kind: ikValues, values: @[])
    raiseDbError("VALUES expected", syntaxError)
  elif t.kind == tokLeftParen:
    t = nextToken(scanner)
    if t.kind != tokIdentifier:
      raiseDbError("identifier expected", syntaxError)
    columns.add(t.identifier)
    t = nextToken(scanner)
    while t.kind == tokComma:
      t = nextToken(scanner)
      if t.kind != tokIdentifier:
        raiseDbError("identifier expected", syntaxError)
      columns.add(t.identifier)
      t = nextToken(scanner)
    if t.kind != tokRightParen:
      raiseDbError("\")\" expected", syntaxError)
    t = nextToken(scanner)
  var argCount = 0
  if t.kind == tokSelect:
    let tableExp = parseTableExp(scanner, argCount)
    return SqlInsert(tableName: nameTok.identifier, columns: columns, kind: ikSelect, select: tableExp)
  if t.kind != tokValues:
    raiseDbError("VALUES or SELECT expected", syntaxError)
  t = nextToken(scanner)
  if t.kind != tokLeftParen:
    raiseDbError("( expected", syntaxError)
  var vals: seq[Expression]
  vals.add(parseExpression(scanner, argCount))
  while currentToken(scanner).kind == tokComma:
    vals.add(parseExpression(scanner, argCount))
  if currentToken(scanner).kind != tokRightParen:
    raiseDbError(") expected", syntaxError)
  result = SqlInsert(tableName: nameTok.identifier, columns: columns, kind: ikValues, values: vals)

proc parseSelectElement(scanner: Scanner, argCount: var int): SelectElement =
  var t = currentToken(scanner)
  if t.kind == tokAsterisk:
    result = SelectElement(colName: "*")
    discard nextToken(scanner)
  else:
    let exp = parseExpression(scanner, argCount, false)
    var colName: string
    t = currentToken(scanner)
    if t.kind == tokAs:
      t = nextToken(scanner)
      if t.kind != tokIdentifier:
        raiseDbError("identifier expected", syntaxError)
    if t.kind == tokIdentifier:
      colName = t.identifier
      discard nextToken(scanner)
    result = SelectElement(colName: colName, exp: exp)

proc parseSelectElements(scanner: Scanner, argCount: var int): seq[SelectElement] =
  result = @[parseSelectElement(scanner, argCount)]
  while currentToken(scanner).kind == tokComma:
    discard nextToken(scanner)
    result.add(parseSelectElement(scanner, argCount))

proc parseTableRef(scanner: Scanner, argCount: var int): SqlTableRef =
  var crossJoin = false
  var leftOuter = false
  while true:
    var nameTok = nextToken(scanner)
    var rangeVar: string
    var t = nextToken(scanner)
    if t.kind == tokAs:
      t = nextToken(scanner)
      if t.kind != tokIdentifier:
        raiseDbError("identifier expected", syntaxError)
      rangeVar = t.identifier
      t = nextToken(scanner)
    elif t.kind == tokIdentifier:
      rangeVar = t.identifier
      t = nextToken(scanner)
    else:
      rangeVar = nameTok.identifier
    let tref = SqlTableRef(kind: trkSimpleTableRef, name: nameTok.identifier, rangeVar: rangeVar)
    if result == nil:
      result = tref
    else:
      if t.kind == tokOn:
        if crossJoin:
          raiseDbError("ON is not allowed with CROSS JOIN", syntaxError)
        result = SqlTableRef(kind: trkRelOp, tableRef1: result, tableRef2: tref,
                             onExp: parseExpression(scanner, argCount),
                             leftOuter: leftOuter)
      else:
        if not crossJoin:
          raiseDbError("ON is required with JOIN", syntaxError)
        result = SqlTableRef(kind: trkRelOp, tableRef1: result, tableRef2: tref,
                             leftOuter: false)
    case t.kind:
      of tokCross:
        if nextToken(scanner).kind != tokJoin:
          raiseDbError("JOIN expected", syntaxError)
        crossJoin = true
        leftOuter = false
      of tokJoin:
        crossJoin = false
        leftOuter = false
      of tokInner:
        if nextToken(scanner).kind != tokJoin:
          raiseDbError("JOIN expected", syntaxError)
        crossJoin = false
        leftOuter = false
      of tokLeft:
        var tk = nextToken(scanner).kind
        if tk == tokOuter:
          tk = nextToken(scanner).kind
        if tk != tokJoin:
          raiseDbError("JOIN expected", syntaxError)
        crossJoin = false
        leftOuter = true
      else:
        return

proc parseTableRefs(scanner: Scanner, argCount: var int): seq[SqlTableRef] =
  result.add(parseTableRef(scanner, argCount))
  while currentToken(scanner).kind == tokComma:
    result.add(parseTableRef(scanner, argCount))

proc parseOrderByElement(scanner: Scanner): OrderByElement =
  var colName: string
  var tableName: string
  var asc: bool
  let t = nextToken(scanner)
  if t.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  let t2 = nextToken(scanner)
  if t2.kind == tokDot:
    let tokCol = nextToken(scanner)
    if tokCol.kind != tokIdentifier:
      raiseDbError("identifier expected", syntaxError)
    discard nextToken(scanner)
    colName = tokCol.identifier
    tableName = t.identifier
  else:
    colName = t.identifier
  let tokDir = currentToken(scanner)
  if tokDir.kind == tokAsc:
    asc = true
    discard nextToken(scanner)
  elif tokDir.kind == tokDesc:
    asc = false
    discard nextToken(scanner)
  else:
    asc = true
  result = OrderByElement(name: colName, tableName: tableName, asc: asc)
    
proc parseOrderBy(scanner: Scanner): seq[OrderByElement] =
  let t = nextToken(scanner)
  if t.kind != tokBy:
    raiseDbError("BY expected", syntaxError)
  result = @[parseOrderByElement(scanner)]
  while currentToken(scanner).kind == tokComma:
    result.add(parseOrderByElement(scanner))

proc parseColRef(scanner: Scanner): QVarExp =
  let t = nextToken(scanner)
  if t.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  let t2 = nextToken(scanner)
  if t2.kind == tokDot:
    let colToken = nextToken(scanner)
    if (colToken.kind != tokIdentifier):
      raiseDbError("identifier expected", syntaxError)
    result = newQVarExp(colToken.identifier, t.identifier)
    discard nextToken(scanner)
  else:
    result = newQVarExp(t.identifier, "")

proc parseGroupBy(scanner: Scanner): seq[QVarExp] =
  var t = nextToken(scanner)
  if t.kind != tokBy:
    raiseDbError("BY expected", syntaxError)
  result.add(parseColRef(scanner))
  t = currentToken(scanner)
  while t.kind == tokComma:
    result.add(parseColRef(scanner))
    t = currentToken(scanner)

proc parseSelect(scanner: Scanner, argCount: var int): SqlSelect =
  var allowDuplicates = true
  var t = nextToken(scanner)
  if t.kind == tokDistinct:
    allowDuplicates = false
    discard nextToken(scanner)
  elif t.kind == tokAll:
    discard nextToken(scanner)
  let selectElements = parseSelectElements(scanner, argCount)
  t = currentToken(scanner)
  if t.kind != tokFrom:
    raiseDbError("FROM expected", syntaxError)
  let tableRefs = parseTableRefs(scanner, argCount)
  var whereExp: Expression = nil
  t = currentToken(scanner)
  if t.kind == tokWhere:
    whereExp = parseExpression(scanner, argCount, true)
  var groupBy = seq[QVarExp](@[])
  t = currentToken(scanner)
  if t.kind == tokGroup:
    groupBy = parseGroupBy(scanner)
  return SqlSelect(columns: selectElements, tables: tableRefs, whereExp: whereExp,
                   groupBy: groupBy, allowDuplicates: allowDuplicates)

proc parseTableTerm(scanner: Scanner, argCount: var int): TableExp =
  var argCount = 0
  var allowDuplicates = true
  while true:
    let sel = parseSelect(scanner, argCount)
    if result == nil:
      result = TableExp(kind: tekSelect, select: sel)
    else:
      result = TableExp(kind: tekIntersect,
                        exp1: result,
                        exp2: TableExp(kind: tekSelect, select: sel),
                        allowDuplicates: allowDuplicates)
    if currentToken(scanner).kind != tokIntersect:
      break
    if nextToken(scanner).kind == tokAll:
      allowDuplicates = true
      discard nextToken(scanner)
    else:
      allowDuplicates = false
    if currentToken(scanner).kind != tokSelect:
      raiseDbError("SELECT expected", syntaxError)

proc parseTableExp(scanner: Scanner, argCount: var int): TableExp =
  var argCount = 0
  var allowDuplicates = true
  var kind: TableExpKind
  while true:
    let sel = parseTableTerm(scanner, argCount)
    if result == nil:
      result = sel
    elif kind == tekUnion:
      result = TableExp(kind: tekUnion,
                        exp1: result,
                        exp2: sel,
                        allowDuplicates: allowDuplicates)
    else:
      result = TableExp(kind: tekExcept,
                        exp1: result,
                        exp2: sel,
                        allowDuplicates: allowDuplicates)
    case currentToken(scanner).kind
      of tokUnion:
        kind = tekUnion
      of tokExcept:
        kind = tekExcept
      else:
        break
    if nextToken(scanner).kind == tokAll:
      allowDuplicates = true
      discard nextToken(scanner)
    else:
      allowDuplicates = false
    if currentToken(scanner).kind != tokSelect:
      raiseDbError("SELECT expected", syntaxError)

proc parseNamedTableExp(scanner: Scanner, argcount: var int): NamedTableExp =
  let nameTok = nextToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  if nextToken(scanner).kind != tokAs:
    raiseDbError("AS expected", syntaxError)
  if nextToken(scanner).kind != tokLeftParen:
    raiseDbError("( expected", syntaxError)
  if nextToken(scanner).kind != tokSelect:
    raiseDbError("SELECT expected", syntaxError)
  let tableExp = parseTableExp(scanner, argCount)
  if currentToken(scanner).kind != tokRightParen:
    raiseDbError(") expected", syntaxError)
  result = NamedTableExp(name: nameTok.identifier, exp: tableExp)

proc parseQueryExp(scanner: Scanner): QueryExp =
  var argCount = 0

  var withTableExps: seq[NamedTableExp]
  while currentToken(scanner).kind == tokWith or
        currentToken(scanner).kind == tokComma:
    withTableExps.add(parseNamedTableExp(scanner, argcount))
    discard nextToken(scanner)
  result = QueryExp(tableExp: parseTableExp(scanner, argCount), withExps: withTableExps)
  if currentToken(scanner).kind == tokOrder:
    result.orderBy = parseOrderBy(scanner)
  else:
    result.orderBy = seq[OrderByElement](@[])

proc parseAssignment(scanner: Scanner, argCount: var int): UpdateAssignment =
  let colTok = nextToken(scanner)
  if colTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  let t = nextToken(scanner)
  if t.kind != tokEq:
    raiseDbError("\"=\" expected", syntaxError)
  let exp = parseExpression(scanner, argCount, true)
  result = UpdateAssignment(column: colTok.identifier, src: exp)

proc parseUpdate(scanner: Scanner): SqlStatement =
  var argCount = 0
  let nameTok = nextToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  var t = nextToken(scanner)
  if t.kind != tokSet:
    raiseDbError("SET expected", syntaxError)
  var assignments: seq[UpdateAssignment]
  assignments.add(parseAssignment(scanner, argCount))
  t = currentToken(scanner)
  while t.kind == tokComma:
    assignments.add(parseAssignment(scanner, argCount))
    t = currentToken(scanner)
  var whereExp: Expression
  if t.kind == tokWhere:
    whereExp = parseExpression(scanner, argCount, true)
  result = SqlUpdate(tableName: nameTok.identifier, updateAssignments: assignments,
                     whereExp: whereExp)

proc parseDelete(scanner: Scanner): SqlStatement =
  var argCount = 0
  var t = nextToken(scanner)
  if t.kind != tokFrom:
    raiseDbError("FROM expected", syntaxError)
  let nameTok = nextToken(scanner)
  if nameTok.kind != tokIdentifier:
    raiseDbError("identifier expected", syntaxError)
  var whereExp: Expression
  t = nextToken(scanner)
  if t.kind == tokWhere:
    whereExp = parseExpression(scanner, argCount, true)
  result = SqlDelete(tableName: nameTok.identifier, whereExp: whereExp)

proc parseCommit(scanner: Scanner): SqlStatement =
  result = SqlCommit()

proc parseRollback(scanner: Scanner): SqlStatement =
  result = SqlRollback()

proc parseStatement*(reader: Reader): SqlStatement =
  let scanner = newScanner(reader)
  let t = nextToken(scanner)
  case t.kind:
    of tokCreate:
      result = parseCreate(scanner)
    of tokDrop:
      result = parseDrop(scanner)
    of tokInsert:
      result = parseInsert(scanner)
    of tokUpdate:
      result = parseUpdate(scanner)
    of tokDelete:
      result = parseDelete(scanner)
    of tokSelect, tokWith:
      result = parseQueryExp(scanner)
    of tokCommit:
      result = parseCommit(scanner);
    of tokRollback:
      result = parseRollback(scanner);
    else:
      raiseDbError("invalid statement", syntaxError)
  if nextToken(scanner).kind != tokEndOfInput:
    raiseDbError("unexpected input near " & $currentToken(scanner), syntaxError)
