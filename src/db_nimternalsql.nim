#
# Copyright (c) 2020, 2022 Rene Hartmann
#
# See the file LICENSE for details about the copyright.
# 

## An in-memory SQL database library.
## NimternalSQL uses the same interface as the Nim ``db_*.nim`` database wrappers.
##
## Examples
## ========
##
## Opening a connection to a database
## ----------------------------------
##
## .. code-block:: Nim
##     import db_nimternalsql
##     let db = open("", "", "", "")
##     db.close()
##
## Creating a table
## ----------------
##
## .. code-block:: Nim
##      db.exec(sql"DROP TABLE IF EXISTS my_table")
##      db.exec(sql"""CREATE TABLE my_table (
##                       id integer PRIMARY KEY,
##                       name varchar(50) not null)""")
##
## Because tables are implemented using hash tables,
## specifying a primary key is required.
##
## Inserting data
## --------------
##
## .. code-block:: Nim
##     db.exec(sql"INSERT INTO my_table (id, name) VALUES (0, ?)",
##             "Nicolas")
import db_common
import db_nimternalsql/nqcommon
import db_nimternalsql/nqtables
import db_nimternalsql/sqlscanner
import db_nimternalsql/sqlparser
import db_nimternalsql/join
import db_nimternalsql/groupby
import db_nimternalsql/duprem
import db_nimternalsql/union
import db_nimternalsql/nqexcept
import db_nimternalsql/intersect
import db_nimternalsql/sorter
import db_nimternalsql/snapshot
import db_nimternalsql/tx
import strutils
import tables
import os

export db_common.sql
export db_common.DbError
export db_common.DbColumns
export nqtables.InstantRow

type
  DbConn* = ref object
    ## A NimternalSQL SQL database.
    db: Database
    errorMsg: string
    tx: Tx
    autocommit: bool
    logFile: File

  Row* = seq[string] ## \
  ## A row of a dataset. NULL database values will be converted to empty strings.

  SqlPrepared = distinct SqlStatement

proc open*(connection, user, password, database: string): DbConn =
  ## Opens a database, creating a new database in memory.
  ## The user, password, and database arguments are ignored.
  ## For compatibility with future versions, empty strings should be passed.
  ##
  ## If the connection argument is not an empty string, it is the name of a directory
  ## where the transaction log file is kept.
  ## If open() finds a file "dump.ndb" this directory, it restores the database from this file.
  ## Afterwards, if open() finds a transaction log file, it reads the file to restore
  ## the last state of the database.
  let db = newDatabase()
  let tx = newTx(connection, if connection != "": openLog(connection, db) else: nil)
  return DbConn(db: db, errorMsg: "", tx: tx, autocommit: true)

proc close*(db: DbConn) =
  ## Closes the database.
  closeLog(db.tx)
  db.db = nil

proc setAutocommit*(db: DbConn, ac: bool) =
  ## Sets the auto-commit mode of the connection given by `db` to `ac`.
  db.autocommit = ac

proc dbError*(db: DbConn) {.noreturn.} =
  ## Raises a DbError exception.
  var e: ref SqlError
  new(e)
  e.msg = db.errorMsg
  e.sqlState = generalError
  raise e

proc sqlState*(err: ref DbError): string =
  ## Extracts an SQLSTATE value from a DbError instance.
  if err of ref SqlError:
    result = (ref SqlError)(err).sqlState
  else:
    result = ""

proc getKey(stmt: SqlCreateTable): seq[string] =
  var keyFound = false
  var pkey: string
  for col in stmt.columns:
    if col.primaryKey:
      if keyFound:
        raiseDbError("multiple primary keys are not allowed", syntaxError)
      else:
        keyFound = true
        pkey = col.name
  if not keyFound:
    if stmt.primaryKey.len == 0:
      raiseDbError("primary key required (for now)", syntaxError)
    return stmt.primaryKey
  if stmt.primaryKey.len > 0:
    raiseDbError("multiple primary keys are not allowed", syntaxError)
  return @[pkey]

method execute(stmt: SqlStatement, db: Database, tx: Tx, args: varargs[
    string]): int64 {.base.} = nil

method execute(stmt: SqlCreateTable, db: Database, tx: Tx, args: varargs[
    string]): int64 =
  discard createBaseTable(tx, db, stmt.tableName, stmt.columns, getKey(stmt))
  result = 0

method execute(stmt: SqlDropTable, db: Database, tx: Tx, args: varargs[string]): int64 =
  try:
    dropBaseTable(tx, db, stmt.tableName)
  except DbError:
    if not stmt.ifExists:
      raise getCurrentException()
  result = 0

method execute(stmt: SqlInsert, db: Database, tx: Tx, args: varargs[string]): int64 =
  if stmt.columns.len > 0:
    if stmt.columns.len != stmt.values.len:
      raiseDbError("number of expressions differs from number of target columns", syntaxError)
  var vals: seq[NqValue]
  var argv: seq[string] = @args
  for val in stmt.values:
    vals.add(val.eval(proc (name: string, rangeVar: string): NqValue =
      if name[0] == '$':
        return NqValue(kind: nqkString, strVal: argv[parseInt(name[
            1..name.high]) - 1])
      raise newException(KeyError, "variables not supported")))
  let table = getTable(db, stmt.tableName)
  if stmt.columns.len == 0 and stmt.values.len > 0:
    insert(tx, table, vals)
  else:
    var insvals: seq[NqValue]
    var valSet: seq[bool]
    newSeq(insvals, table.def.len)
    newSeq(valSet, table.def.len)
    var i = 0
    for colName in stmt.columns:
      let col = columnNo(table, colName)
      if col == -1:
        raiseDbError("column \"" & colName & "\" does not exist", undefinedColumnName)
      insvals[col] = vals[i]
      valSet[col] = true
      i += 1
    for i in 0..<table.def.len:
      if not valSet[i]:
        if table.def[i].defaultValue != nil:
          insvals[i] = eval(table.def[i].defaultValue, nil, nil)
        elif table.def[i].autoincrement:
          if table.def[i].typ == "INTEGER" or table.def[i].typ == "INT":
            if table.def[i].currentAutoincVal >= high(int32):
              raiseDbError("AUTOINCREMENT reached highest possible value on column " &
                          table.def[i].name, valueOutOfRange)
            table.def[i].currentAutoincVal += 1
            insvals[i] = NqValue(kind: nqkInt,
                                intVal: int32(table.def[i].currentAutoincVal))
          else:
            if table.def[i].currentAutoincVal == high(int64):
              raiseDbError("AUTOINCREMENT reached highest possible value on column " &
                          table.def[i].name, valueOutOfRange)
            table.def[i].currentAutoincVal += 1
            insvals[i] = NqValue(kind: nqkBigint,
                                bigintVal: table.def[i].currentAutoincVal)
        else:
          insvals[i] = NqValue(kind: nqkNull)
    insert(tx, table, insvals)
  result = 1

method execute(stmt: SqlUpdate, db: Database, tx: Tx, args: varargs[string]): int64 =
  let table = getTable(db, stmt.tableName)
  var assignments: seq[ColumnAssignment]
  for a in stmt.updateAssignments:
    let col = columnNo(table, a.column)
    assignments.add(ColumnAssignment(col: col, src: a.src))
  result = update(tx, table, assignments, stmt.whereExp, args)

method execute(stmt: SqlDelete, db: Database, tx: Tx, args: varargs[string]): int64 =
  result = delete(tx, getTable(db, stmt.tableName), stmt.whereExp, args)

method execute(stmt: SqlCommit, db: Database, tx: Tx, args: varargs[string]): int64 =
  tx.commit()

method execute(stmt: SqlRollback, db: Database, tx: Tx, args: varargs[string]): int64 =
  tx.rollback(db)

proc exec*(conn: DbConn; sql: SqlQuery; args: varargs[string, `$`]) =
  ## Executes the query and raises DbError if not successful.
  let stmt = parseStatement(newStringReader(string(sql)))
  if conn.autocommit:
    try:
      discard stmt.execute(conn.db, conn.tx, args)
      conn.tx.commit()
    except:
      conn.tx.rollback(conn.db)
      raise
  else:
    discard stmt.execute(conn.db, conn.tx, args)

proc exec*(conn: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]) =
  ## Executes the prepared query and raises DbError if not successful.
  if conn.autocommit:
    try:
      discard SqlStatement(stmt).execute(conn.db, conn.tx, args)
      conn.tx.commit()
    except:
      conn.tx.rollback(conn.db)
      raise
  else:
    discard SqlStatement(stmt).execute(conn.db, conn.tx, args)

proc tryExec*(db: DbConn; query: SqlQuery; args: varargs[string, `$`]): bool =
  ## Tries to execute the prepared query and returns true if successful, false otherwise.
  try:
    exec(db, query, args)
    return true
  except DbError:
    db.errorMsg = getCurrentExceptionMsg()
    return false

proc tryExec*(db: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): bool =
  ## Tries to execute the prepared query and returns true if successful, false otherwise.
  try:
    exec(db, stmt, args)
    return true
  except DbError:
    db.errorMsg = getCurrentExceptionMsg()
    return false

proc execAffectedRows*(conn: DbConn; sql: SqlQuery; args: varargs[string, `$`]): int64 =
  ## Executes the query (typically "UPDATE") and returns the number of affected rows.
  let stmt = parseStatement(newStringReader(string(sql)))
  if conn.autocommit:
    try:
      result = stmt.execute(conn.db, conn.tx, args)
      conn.tx.commit()
    except:
      conn.tx.rollback(conn.db)
      raise
  else:
    result = stmt.execute(conn.db, conn.tx, args)

proc execAffectedRows*(conn: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): int64 =
  ## Executes the prepared query (typically "UPDATE") and returns the number of affected rows.
  if conn.autocommit:
    try:
      result = SqlStatement(stmt).execute(conn.db, conn.tx, args)
      conn.tx.commit()
    except:
      conn.tx.rollback(conn.db)
      raise
  else:
    result = SqlStatement(stmt).execute(conn.db, conn.tx, args)

func toVTable(tableRef: SqlTableRef, withTables: Table[string, VTable],
              db: Database): VTable =
  case tableRef.kind:
    of trkSimpleTableRef:
      if withTables.hasKey(tableRef.name):
        return withTables[tableRef.name]
      result = BaseTableRef(table: getTable(db, tableRef.name),
                         rangeVar: tableRef.rangeVar)
    of trkRelOp:
      result = newJoinTable(toVTable(tableRef.tableRef1, withTables, db),
                             toVTable(tableRef.tableRef2, withTables, db),
                             tableRef.leftOuter,
                             tableRef.onExp)

proc toVTable(tableExp: TableExp, withTables: Table[string, VTable], db: Database): VTable

method transform(exp: Expression, withTables: Table[string, VTable],
                 db: Database): Expression {.base.} =
  result = exp

method transform(exp: ScalarOpExp, withTables: Table[string, VTable],
                 db: Database): Expression =
  var dstargs: seq[Expression]
  for arg in exp.args:
    dstargs.add(transform(arg, withTables, db))
  result = ScalarOpExp(opName: exp.opName, args: dstargs)

method transform(exp: CastExp, withTables: Table[string, VTable],
                 db: Database): Expression =
  result = newCastExp(transform(exp.exp, withTables, db), exp.typeDef)

method transform(exp: TableExp, withTables: Table[string, VTable],
                 db: Database): Expression =
  result = toVTable(exp, withTables, db)

method transform(exp: CaseExp, withTables: Table[string, VTable],
                 db: Database): Expression =
  var whens: seq[tuple[cond: Expression, exp: Expression]]
  for w in exp.whens:
    whens.add((cond: transform(w.cond, withTables, db),
                    exp: transform(w.exp, withTables, db)))
  result = newCaseExp(if exp.exp != nil: transform(exp.exp, withTables, db) else: nil,
                      whens,
                      if exp.elseExp != nil: transform(exp.elseExp, withTables, db) else: nil)

proc toVTable(tableExp: TableExp, withTables: Table[string, VTable],
              db: Database): VTable =
  case tableExp.kind:
    of tekSelect:
      result = toVTable(tableExp.select.tables[0], withTables, db)
      for i in 1..<tableExp.select.tables.len:
        result = newJoinTable(result,
            BaseTableRef(table: getTable(db, tableExp.select.tables[i].name),
                      rangeVar: tableExp.select.tables[i].rangeVar))
      if tableExp.select.whereExp != nil:
        let joinExp = transform(tableExp.select.whereExp, withTables, db)
        if (result of JoinTable) and ((JoinTable)result).exp == nil:
          ((JoinTable)result).exp = joinExp
        else:
          result = newWhereTable(result, transform(tableExp.select.whereExp, withTables, db))
      if tableExp.select.columns.len != 1 or
          tableExp.select.columns[0].colName != "*":
        for i in 0..<tableExp.select.columns.len:
          if tableExp.select.columns[i].colName == "" and
              tableExp.select.columns[i].exp of QVarExp:
            tableExp.select.columns[i].colName = QVarExp(
                tableExp.select.columns[i].exp).name
        var cols: seq[SelectElement]
        for col in tableExp.select.columns:
          cols.add(SelectElement(colName: col.colName, exp: transform(col.exp, withTables, db)))
        result = newProjectTable(result, cols)
      let aggrs = getAggrs(tableExp.select.columns)
      if aggrs.len > 0 or tableExp.select.groupBy.len > 0:
        result = newGroupTable(result, aggrs, tableExp.select.groupBy)
      if not tableExp.select.allowDuplicates:
        result = newDupRemTable(result)
    of tekUnion:
      result = newUnionTable(toVTable(tableExp.exp1, withTables, db),
                             toVTable(tableExp.exp2, withTables, db))
      if not tableExp.allowDuplicates:
        result = newDupRemTable(result)
    of tekExcept:
      let leftChild = toVTable(tableExp.exp1, withTables, db)
      result = newExceptTable(leftChild,
                              toVTable(tableExp.exp2, withTables, db))
      if (not tableExp.allowDuplicates) and not (leftChild of BaseTableRef):
        result = newDupRemTable(result)
    of tekIntersect:
      let leftChild = toVTable(tableExp.exp1, withTables, db)
      let rightChild = toVTable(tableExp.exp2, withTables, db)
      if (rightChild of BaseTableRef) or not (leftChild of BaseTableRef):
        result = newIntersectTable(leftChild, rightChild)
        if (not tableExp.allowDuplicates) and not (leftChild of BaseTableRef):
          result = newDupRemTable(result)
      else:
        result = newIntersectTable(rightChild, leftChild)
        if (not tableExp.allowDuplicates) and not (rightChild of BaseTableRef):
          result = newDupRemTable(result)

proc toVTable(stmt: SqlStatement, db: Database): VTable =
  if not (stmt of QueryExp):
    raiseDbError("statement has no result", syntaxError)
  let queryExp = QueryExp(stmt)
  var withTables: Table[string, VTable]
  for withExp in queryExp.withExps:
    let wtable = toVTable(withExp.exp, withTables, db)
    wtable.name = withExp.name
    withTables[withExp.name] = wtable
  result = toVTable(queryExp.tableExp, withTables, db)
  if queryExp.orderBy.len > 0:
    var order: seq[tuple[col: Natural, asc: bool]]
    for orderElement in queryExp.orderBy:
      let col = columnNo(result, orderElement.name, orderElement.tableName)
      if col == -1:
        raiseDbError("column " &
            (if orderElement.tableName != "": orderElement.tableName &
                "." else: "") &
            orderElement.name & " does not exist", undefinedColumnName)
      order.add((col: Natural(col), asc: orderElement.asc))
    result = newSortedTable(if result of DupremTable: DupremTable(result).child
                            else: result,
                            order,
                            result of DupremTable)

iterator instantRows*(conn: DbConn; sql: SqlQuery; args: varargs[string,
    `$`]): InstantRow =
  ## Executes the query and iterates over the result dataset.
  ## The InstantRows instance returned is not guaranteed to be valid
  ## outside the iterator body.
  let stmt = parseStatement(newStringReader(string(sql)))
  for r in instantRows(toVTable(stmt, conn.db), args, nil):
    yield r

iterator instantRows*(conn: DbConn; columns: var DbColumns; sql: SqlQuery;
                     args: varargs[string, `$`]): InstantRow =
  let stmt = parseStatement(newStringReader(string(sql)))
  let table = toVTable(stmt, conn.db)
  columns = table.getColumns()
  for r in instantRows(table, args, nil):
    yield r

proc prepare*(conn: DbConn; sql: SqlQuery): SqlPrepared =
  ## Creates a new ``SqlPrepared`` statement. Parameter substitution is done
  ## via ``$1``, ``$2``, ``$3``, etc.
  result = SqlPrepared(parseStatement(newStringReader(string(sql))))

iterator instantRows*(conn: DbConn; sql: SqlPrepared; args: varargs[string,
    `$`]): InstantRow =
  ## Executes the prepared query and iterates over the result dataset.
  for r in instantRows(toVTable(SqlStatement(sql), conn.db), args, nil):
    yield r

proc `[]`*(row: InstantRow; col: int): string =
  ## Returns text for given column of the row.
  result = $columnValueAt(row, col)

func len*(row: InstantRow): int =
  ## Returns number of columns in a row.
  result = columnCount(row)

iterator rows*(db: DbConn; query: SqlQuery; args: varargs[string, `$`]): Row =
  ## Executes the query and iterates over the result dataset.
  for ir in instantRows(db, query, args):
    var row: Row
    for i in 0..<ir.len:
      row.add(ir[i])
    yield row

iterator rows*(db: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): Row =
  ## Executes the query and iterates over the result dataset.
  for ir in instantRows(db, stmt, args):
    var row: Row
    for i in 0..<ir.len:
      row.add(ir[i])
    yield row

proc getAllRows*(db: DbConn; query: SqlQuery; args: varargs[string, `$`]): seq[Row] =
  ## Executes the query and returns the whole result dataset.
  for r in instantRows(db, query, args):
    var resultRow: Row = @[]
    for i in 0..<r.len:
      resultRow.add(r[i])
    result.add(resultRow)

proc getAllRows*(db: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): seq[Row] =
  ## Executes the prepared query and returns the whole result dataset.
  for r in instantRows(db, stmt, args):
    var resultRow: Row = @[]
    for i in 0..<r.len:
      resultRow.add(r[i])
    result.add(resultRow)

proc getRow*(conn: DbConn; query: SqlQuery; args: varargs[string, `$`]): Row =
  ## Retrieves a single row. If the query doesn't return any rows,
  ## this proc will return a Row with empty strings for each column.
  let stmt = parseStatement(newStringReader(string(query)))
  let vtable = toVTable(stmt, conn.db)
  for r in instantRows(vtable, args, nil):
    for i in 0..<r.len:
      result.add(r[i])
    return
  for i in 0..<vtable.columnCount():
    result.add("")

proc getRow*(conn: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): Row =
  let vtable = toVTable(SqlStatement(stmt), conn.db)
  for r in instantRows(vtable, args, nil):
    for i in 0..<r.len:
      result.add(r[i])
    return
  for i in 0..<vtable.columnCount():
    result.add("")

proc getValue*(conn: DbConn; query: SqlQuery; args: varargs[string,
    `$`]): string =
  ## Executes the query and returns the first column of the first row of the result dataset.
  ## Returns "" if the dataset contains no rows or the database value is NULL.
  result = getRow(conn, query, args)[0]

proc getValue*(conn: DbConn; stmt: SqlPrepared; args: varargs[string,
    `$`]): string =
  ## Executes the prepared query and returns the first column of the first row of the result dataset.
  ## Returns "" if the dataset contains no rows or the database value is NULL.
  result = getRow(conn, stmt, args)[0]

proc save*(conn: DbConn, filename: string) =
  ## Saves a snapshot of the database to a file named `filename`.
  save(conn.db, filename)

proc save*(conn: DbConn) =
  ## If transaction logging is active, a snapshot file "dump.ndb" is saved
  ## in the directory that was passed to open() and the transaction log is truncated.
  ## Otherwise, a snapshot file "dump.ndb" is saved in the current directory.
  if conn.tx.logIsActive:
    save(conn.db, conn.tx.logDir & DirSep & defaultDumpName)
    truncateLog(conn.tx)
  else:
    save(conn.db, defaultDumpName)

proc restore*(conn: DbConn, filename: string) =
  ## Restores a previously saved database snapshot from a file named `filename`.
  if conn.tx.logIsActive:
    raiseDbError("explicitly restoring snapshots is not supported in transaction log mode",
                 restoreNotSupported)
  restore(conn.db, filename)
