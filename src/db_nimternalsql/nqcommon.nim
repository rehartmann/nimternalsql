#
# Copyright (c) 2020, 2022 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
# 
import db_common

const
  defaultDumpName* = "dump.ndb"

  tooManyRowsReturnedBySubquery* = "21000"
  stringTooLong* = "22001"
  valueOutOfRange* = "22003"
  invalidDatetimeValue* = "22007"
  invalidParameterValue* = "22023"
  columnNotNullable* = "23502"
  uniqueConstraintViolation* = "23505"
  syntaxError* = "42601"
  columnRefAmbiguous* = "42702"
  undefinedColumnName* = "42703"
  undefinedObjectName* = "42704"
  invalidGrouping* = "42803"
  typeMismatch* = "42804"
  undefinedFunction* = "42883"
  tableExists* = "42N01"
  generalError* = "HY000"
  internalError* = "N0000"
  fileIoError* = "N0001"
  fileNotValid* = "N0002"
  notADirectory* = "N0003"
  restoreNotSupported* = "NS004"

type
  ColumnDef* = object of RootObj
    name*: string
    typ*: string
    size*: Natural
    precision*: Natural
    scale*: Natural
    notNull*: bool
    defaultValue*: Expression
    primaryKey*: bool # unused in BaseTable
    autoincrement*: bool
  TypeDef* = object
    typ*: string
    size*: Natural
    precision*: Natural
    scale*: Natural

  Expression* = ref object of RootObj
  ScalarLit* = ref object of Expression
    val*: string
  StringLit* {.final.} = ref object of ScalarLit
  NumericLit* {.final.} = ref object of ScalarLit
  BoolLit* {.final.} = ref object of ScalarLit
  TimeLit* {.final.} = ref object of ScalarLit
  TimestampLit* {.final.} = ref object of ScalarLit
  DateLit* {.final.} = ref object of ScalarLit
  NullLit* {.final.} = ref object of Expression
  ScalarOpExp* {.acyclic.} = ref object of Expression
    opName*: string
    args*: seq[Expression]
  QVarExp* = ref object of Expression
    name*: string
    tableName*: string
  ListExp* {.acyclic.} = ref object of Expression
    exps*: seq[Expression]
  CaseExp* {.acyclic.} = ref object of Expression
    exp*: Expression
    whens*: seq[tuple[cond: Expression, exp: Expression]]
    elseExp*: Expression
  CastExp* {.acyclic.} = ref object of Expression
    exp*: Expression
    typeDef*: TypeDef
  SelectElement* {.acyclic.} = object
    colName*: string
    exp*: Expression

  SqlError* = object of DbError
    sqlState*: string

proc raiseDbError*(msg: string, sqlstate: string) {.noreturn.} =
  var
    e: ref SqlError
  new(e)
  e.msg = msg
  e.sqlState = sqlstate
  raise e

func newStringLit*(v: string): Expression =
  result = StringLit(val: v)

func newNumericLit*(v: string): Expression =
  result = NumericLit(val: v)

func newTimeLit*(v: string): Expression =
  result = TimeLit(val: v)

func newTimestampLit*(v: string): Expression =
  result = TimestampLit(val: v)

func newDateLit*(v: string): Expression =
  result = DateLit(val: v)

func newBoolLit*(v: bool): Expression =
  result = BoolLit(val: if v: "TRUE" else: "FALSE")

func newNullLit*(): Expression =
  result = NullLit()

func newListExp*(v: seq[Expression]): Expression =
  result = ListExp(exps: v)

func newScalarOpExp*(name: string, args: varargs[Expression]): Expression =
  result = ScalarOpExp(opName: name, args: @args)

func newQVarExp*(name: string, tableName: string = ""): QVarExp =
  result = QVarExp(name: name, tableName: tableName)

func newCaseExp*(exp: Expression,
                 whens: seq[tuple[cond: Expression, exp: Expression]],
                 elseExp: Expression): Expression =
  result = CaseExp(exp: exp, whens: whens, elseExp: elseExp)

func newCastExp*(exp: Expression, typ: TypeDef): Expression =
  result = CastExp(exp: exp, typedef: typ)

method `$`*(exp: Expression): string {.base.} = nil

method `$`(exp: ScalarLit): string =
  result = exp.val

method `$`(exp: ScalarOpExp): string =
  result = exp.opName & '('
  for i in 0..<exp.args.len:
    if i != 0:
      result &= ','
    result &= $ exp.args[i]
  result &= ')'

method `$`(exp: CaseExp): string =
  result = "CASE "
  if exp.exp != nil:
    result &= $exp.exp
  for w in exp.whens:
    result &= " WHEN " & $w.cond & " THEN " & $w.exp
  if exp.elseExp != nil:
    result &= " ELSE " & $exp.elseExp

method `$`(exp: QVarExp): string =
  if exp.tableName != "":
    result = exp.tableName & '.'
  result &= exp.name

method `$`(exp: ListExp): string =
  result = "("
  for i in 0..<exp.exps.len:
    if i != 0:
      result &= ','
    result &= $ exp.exps[i]
  result &= ')'
  
