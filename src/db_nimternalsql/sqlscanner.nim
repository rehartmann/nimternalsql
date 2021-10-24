import strutils
import nqcommon

type
  TokenKind* = enum
    tokAll, tokAnd, tokAny, tokAs, tokAsc, tokBy, tokChar, tokDefault, tokDrop,
    tokVarchar, tokNumeric, tokCase, tokWhen, tokThen, tokElse, tokEnd,
    tokCommit, tokRollback, tokCreate, tokCross, tokCount, tokDecimal,
    tokDelete, tokDesc, tokDistinct, tokDouble, tokPrecision, tokExists,
    tokFrom, tokGroup, tokIf, tokInsert, tokUpdate, tokSet, tokIn, tokIs,
    tokInto, tokJoin, tokKey, tokLike, tokNot, tokOn, tokOr, tokOrder,
    tokPrimary, tokSelect, tokTable, tokUnion, tokValues, tokWhere,
    tokLeft, tokInner, tokOuter,
    tokAsterisk, tokDiv, tokPlus, tokNull, tokMinus, tokRightParen,
    tokLeftParen, tokComma, tokDot, tokEq, tokNeq, tokLt, tokLe, tokGt, tokGe,
    tokConcat, tokIdentifier, tokString, tokInt, tokRat, tokTrue, tokFalse,
    tokPlaceholder, tokNumPlaceholder, tokEndOfInput
  Token* = object
    case kind*: TokenKind
    of tokIdentifier:
      identifier*: string
    of tokString, tokInt, tokRat:
      value*: string
    else: discard

  Reader* = ref object of RootObj
  FileReader* = ref object of Reader
    file: File
    nextChar: char
    eof: bool
  StringReader* = ref object of Reader
    buf: string
    current: int

  Scanner* = ref object
    reader: Reader
    current: Token

method read(reader: Reader): char {.base.} = nil
method peek(reader: Reader): char {.base.} = nil

proc newStringReader*(s: string): StringReader =
  result = StringReader(buf: s, current: 0)

method read(reader: StringReader): char =
  if reader.current >= reader.buf.len():
    raise newException(EOFError, "end of input string")
  result = reader.buf[reader.current]
  reader.current += 1

method peek(reader: StringReader): char =
  if reader.current < reader.buf.len():
    result = reader.buf[reader.current]
  else:
    result = '\0'

proc newFileReader*(f: File): FileReader =
  result = FileReader(file: f, eof: false)
  try:
    result.nextChar = f.readChar()
  except EOFError:
    result.eof = true

method read(reader: FileReader): char =
  if reader.eof:
    raise newException(EOFError, "end of input file")
  result = reader.nextChar
  try:
    reader.nextChar = reader.file.readChar()
  except EOFError:
    reader.eof = true

method peek(reader: FileReader): char =
  if reader.eof:
    result = '\0'
  else:
    result = reader.nextChar

proc newScanner*(r: Reader): Scanner =
  result = Scanner(reader: r)

proc toToken(s: string): Token =
  case toUpperAscii(s):
    of "ALL":
      return Token(kind: tokAll)
    of "AND":
      return Token(kind: tokAnd)
    of "ANY":
      return Token(kind: tokAny)
    of "AS":
      return Token(kind: tokAs)
    of "ASC":
      return Token(kind: tokAsc)
    of "BY":
      return Token(kind: tokBy)
    of "CASE":
      return Token(kind: tokCase)
    of "COMMIT":
      return Token(kind: tokCommit)
    of "COUNT":
      return Token(kind: tokCount)
    of "CHARACTER", "CHAR":
      return Token(kind: tokChar)
    of "CREATE":
      return Token(kind: tokCreate)
    of "CROSS":
      return Token(kind: tokCross)
    of "DEC", "DECIMAL":
      return Token(kind: tokDecimal)
    of "DELETE":
      return Token(kind: tokDelete)
    of "DEFAULT":
      return Token(kind: tokDefault)
    of "DESC":
      return Token(kind: tokDesc)
    of "DISTINCT":
      return Token(kind: tokDistinct)
    of "DOUBLE":
      return Token(kind: tokDouble)
    of "DROP":
      return Token(kind: tokDrop)
    of "ELSE":
      return Token(kind: tokElse)
    of "END":
      return Token(kind: tokEnd)
    of "EXISTS":
      return Token(kind: tokExists)
    of "FALSE":
      return Token(kind: tokFalse)
    of "GROUP":
      return Token(kind: tokGroup)
    of "THEN":
      return Token(kind: tokThen)
    of "TRUE":
      return Token(kind: tokTrue)
    of "FROM":
      return Token(kind: tokFrom)
    of "IF":
      return Token(kind: tokIf)
    of "INNER":
      return Token(kind: tokInner)
    of "INSERT":
      return Token(kind: tokInsert)
    of "IN":
      return Token(kind: tokIn)
    of "INTO":
      return Token(kind: tokInto)
    of "IS":
      return Token(kind: tokIs)
    of "JOIN":
      return Token(kind: tokJoin)
    of "KEY":
      return Token(kind: tokKey)
    of "LEFT":
      return Token(kind: tokLeft)
    of "LIKE":
      return Token(kind: tokLike)
    of "NOT":
      return Token(kind: tokNot)
    of "NULL":
      return Token(kind: tokNull)
    of "NUMERIC":
      return Token(kind: tokNumeric)
    of "ON":
      return Token(kind: tokOn)
    of "OR":
      return Token(kind: tokOr)
    of "ORDER":
      return Token(kind: tokOrder)
    of "OUTER":
      return Token(kind: tokOuter)
    of "PRIMARY":
      return Token(kind: tokPrimary)
    of "PRECISION":
      return Token(kind: tokPrecision)
    of "TABLE":
      return Token(kind: tokTable)
    of "ROLLBACK":
      return Token(kind: tokRollback)
    of "SELECT":
      return Token(kind: tokSelect)
    of "SET":
      return Token(kind: tokSet)
    of "SOME":
      return Token(kind: tokAny)
    of "UNION":
      return Token(kind: tokUnion)
    of "UPDATE":
      return Token(kind: tokUpdate)
    of "VALUES":
      return Token(kind: tokValues)
    of "VARCHAR":
      return Token(kind: tokVarchar)
    of "WHEN":
      return Token(kind: tokWhen)
    of "WHERE":
      return Token(kind: tokWhere)
  result = Token(kind: tokIdentifier, identifier: toUpperAscii(s))

proc currentToken*(s: Scanner): Token =
  result = s.current

func isAlphaNumericUnderscore(c: char): bool =
  result = isAlphaNumeric(c) or c == '_'

proc readNextToken(s: Scanner): Token =
  var c: char
  try:
    c = s.reader.read()
    while isSpaceAscii(c):
      c = s.reader.read()
  except EOFError:
    return Token(kind: tokEndOfInput)
  if isAlphaAscii(c):
    var id = $c
    try:
      while isAlphaNumericUnderscore(s.reader.peek()):
        id &= s.reader.read()
    except EOFError:
      discard
    return toToken(id)
  if isDigit(c):
    var numStr = $c
    try:
      while isDigit(s.reader.peek()):
        numStr &= s.reader.read()
      if s.reader.peek() == '.':
        numStr &= "."
        discard s.reader.read()
        while isDigit(s.reader.peek()):
          numStr &= s.reader.read()
        if toUpperAscii(s.reader.peek()) == 'E':
          numStr &= 'E'
          discard s.reader.read()
        let sc = s.reader.peek()
        if sc == '+' or sc == '-':
          numStr &= sc
          discard s.reader.read()
        while isDigit(s.reader.peek()):
          numStr &= s.reader.read()        
    except EOFError:
      discard
    return if find(numStr, ".") >= 0: Token(kind: tokRat, value: numStr)
           else: Token(kind: tokInt, value: numStr)
  case c:
    of '*':
      return Token(kind: tokAsterisk)
    of '/':
      return Token(kind: tokDiv)
    of '+':
      return Token(kind: tokPlus)
    of '-':
      return Token(kind: tokMinus)
    of '(':
      return Token(kind: tokLeftParen)
    of ')':
      return Token(kind: tokRightParen)
    of ',':
      return Token(kind: tokComma)
    of '.':
      return Token(kind: tokDot)
    of '=':
      return Token(kind: tokEq)
    of '<':
      if s.reader.peek() == '>':
        discard s.reader.read()
        return Token(kind: tokNeq)
      elif s.reader.peek() == '=':
        discard s.reader.read()
        return Token(kind: tokLe)
      return Token(kind: tokLt)
    of '>':
      if s.reader.peek() == '=':
        discard s.reader.read()
        return Token(kind: tokGe)
      return Token(kind: tokGt)
    of '|':
      if s.reader.read() != '|':
        raiseDbError("invalid operator: |")
      return Token(kind: tokConcat)
    of '\'':
      var str = ""
      while true:
        c = s.reader.read()
        if c == '\'':
          if s.reader.peek() == '\'':
            discard s.reader.read()
            str &= '\''
          else:
            break
        else:
          str &= c
      return Token(kind: tokString, value: str)
    of '?':
      return Token(kind: tokPlaceholder)
    of '$':
      return Token(kind: tokNumPlaceholder)
    else:
      raiseDbError("Invalid character: " & c)

proc nextToken*(s: Scanner): Token =
  s.current = readNextToken(s)
  result = s.current