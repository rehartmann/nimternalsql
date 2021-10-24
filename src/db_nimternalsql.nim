#
# Copyright (c) 2020 Rene Hartmann
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
import db_nimternalsql/nqcore
import db_nimternalsql/sqlscanner
import db_nimternalsql/sqlparser
import db_nimternalsql/join
import db_nimternalsql/duprem
import db_nimternalsql/union
import db_nimternalsql/sorter
import db_nimternalsql/snapshot
import db_nimternalsql/tx
import strutils
import os

export db_common.sql
export db_common.DbError
export nqcore.InstantRow

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
  var e: ref DbError
  new(e)
  e.msg = db.errorMsg
  raise e

proc getKey(stmt: SqlCreateTable): seq[string] =
  var keyFound = false
  var pkey: string
  for col in stmt.columns:
    if col.primaryKey:
      if keyFound:
        raiseDbError("multiple primary keys are not allowed")
      else:
        keyFound = true
        pkey = col.name
  if not keyFound:
    if stmt.primaryKey.len == 0:
      raiseDbError("primary key required (for now)")
    return stmt.primaryKey
  if stmt.primaryKey.len > 0:
    raiseDbError("multiple primary keys are not allowed")
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
      raiseDbError("number of expressions differs from number of target columns")
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
        raiseDbError("column \"" & colName & "\" does not exist")
      insvals[col] = vals[i]
      valSet[col] = true
      i += 1
    for i in 0..<table.def.len:
      if not valSet[i]:
        if table.def[i].defaultValue != nil:
          insvals[i] = eval(table.def[i].defaultValue, nil, nil)
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

func toVTable(tableRef: SqlTableRef, db: Database): VTable =
  case tableRef.kind:
    of trkSimpleTableRef:
      result = BaseTableRef(table: getTable(db, tableRef.name),
                         rangeVar: tableRef.rangeVar)
    of trkRelOp:
      result = newJoinTable(toVTable(tableRef.tableRef1, db),
                             toVTable(tableRef.tableRef2, db),
                             tableRef.leftOuter,
                             tableRef.onExp)

proc toVTable(tableExp: TableExp, db: Database): VTable

method transform(exp: Expression, db: Database): Expression {.base.} =
  result = exp

method transform(exp: ScalarOpExp, db: Database): Expression =
  var dstargs: seq[Expression]
  for arg in exp.args:
    dstargs.add(transform(arg, db))
  result = ScalarOpExp(opName: exp.opName, args: dstargs)

method transform(exp: TableExp, db: Database): Expression =
  result = toVTable(exp, db)

proc toVTable(tableExp: TableExp, db: Database): VTable =
  case tableExp.kind:
    of tekSelect:
      result = toVTable(tableExp.select.tables[0], db)
      for i in 1..<tableExp.select.tables.len:
        result = newJoinTable(result,
            BaseTableRef(table: getTable(db, tableExp.select.tables[i].name),
                      rangeVar: tableExp.select.tables[i].rangeVar))
      if tableExp.select.whereExp != nil:
        let joinExp = transform(tableExp.select.whereExp, db)
        if (result of JoinTable) and ((JoinTable)result).exp == nil:
          ((JoinTable)result).exp = joinExp
        else:
          result = newWhereTable(result, transform(tableExp.select.whereExp, db))
      if tableExp.select.columns.len != 1 or tableExp.select.columns[
          0].colName != "*":
        for i in 0..<tableExp.select.columns.len:
          if tableExp.select.columns[i].colName == "" and
              tableExp.select.columns[i].exp of QVarExp:
            tableExp.select.columns[i].colName = QVarExp(
                tableExp.select.columns[i].exp).name
        result = newProjectTable(result, tableExp.select.columns)
      let aggrs = getAggrs(tableExp.select.columns)
      if aggrs.len > 0 or tableExp.select.groupBy.len > 0:
        result = newGroupTable(result, aggrs, tableExp.select.groupBy)
      if not tableExp.select.allowDuplicates:
        result = newDupRemTable(result)
    of tekUnion:
      result = newUnionTable(toVTable(tableExp.exp1, db),
                             toVTable(tableExp.exp2, db))
      if not tableExp.allowDuplicates:
        result = newDupRemTable(result)

proc toVTable(stmt: SqlStatement, db: Database): VTable =
  if not (stmt of QueryExp):
    raiseDbError("statement has no result")
  let queryExp = QueryExp(stmt)
  result = toVTable(queryExp.tableExp, db)
  if queryExp.orderBy.len > 0:
    var order: seq[tuple[col: Natural, asc: bool]]
    for orderElement in queryExp.orderBy:
      let col = columnNo(result, orderElement.name, orderElement.tableName)
      if col == -1:
        raiseDbError("column " &
            (if orderElement.tableName != "": orderElement.tableName &
                "." else: "") &
            orderElement.name & " does not exist")
      order.add((col: Natural(col), asc: orderElement.asc))
    result = newSortedTable(result, order)

iterator instantRows*(conn: DbConn; sql: SqlQuery; args: varargs[string,
    `$`]): InstantRow =
  ## Executes the query and iterates over the result dataset.
  ## The InstantRows instance returned is not guaranteed to be valid
  ## outside the iterator body.
  let stmt = parseStatement(newStringReader(string(sql)))
  for r in instantRows(toVTable(stmt, conn.db), args):
    yield r

proc prepare*(conn: DbConn; sql: SqlQuery): SqlPrepared =
  ## Creates a new ``SqlPrepared`` statement. Parameter substitution is done
  ## via ``$1``, ``$2``, ``$3``, etc.
  result = SqlPrepared(parseStatement(newStringReader(string(sql))))

iterator instantRows*(conn: DbConn; sql: SqlPrepared; args: varargs[string,
    `$`]): InstantRow =
  ## Executes the prepared query and iterates over the result dataset.
  for r in instantRows(toVTable(SqlStatement(sql), conn.db), args):
    yield r

func `[]`*(row: InstantRow; col: int): string =
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
  for r in instantRows(vtable, args):
    for i in 0..<r.len:
      result.add(r[i])
    return
  for i in 0..<vtable.columnCount():
    result.add("")

proc getRow*(conn: DbConn; stmt: SqlPrepared; args: varargs[string, `$`]): Row =
  let vtable = toVTable(SqlStatement(stmt), conn.db)
  for r in instantRows(vtable, args):
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
    raiseDbError("restoring shnapshots is not supported in transaction log mode")
  restore(conn.db, filename)
