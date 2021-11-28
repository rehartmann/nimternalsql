#
# Copyright (c) 2020 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
# 
import db_common
import tables

const
  defaultDumpName* = "dump.ndb"

type
  ColumnDef* = object
    name*: string
    typ*: string
    size*: Natural
    precision*: Natural
    scale*: Natural
    notNull*: bool
    defaultValue*: Expression
    primaryKey*: bool # unused in BaseTable
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
  TrimExp* {.acyclic.} = ref object of Expression
    src*: Expression
    leading*: bool
    trailing*: bool
    char*: Expression
  SelectElement* {.acyclic.} = object
    colName*: string
    exp*: Expression

  Record*[T] = seq[T]
  BaseTable* = ref object of RootObj
    name*: string
    def*: seq[ColumnDef]
    primaryKey*: seq[Natural]
  Database* = ref object
    tables*: Table[string, BaseTable]
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

proc raiseDbError*(msg: string) {.noreturn.} =
  var
    e: ref DbError
  new(e)
  e.msg = msg
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

func newTrimExp*(src: Expression, leading: bool, trailing: bool, char: Expression = nil):
     Expression =
  result = TrimExp(src: src, leading: leading, trailing: trailing, char: char)
     
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
  
