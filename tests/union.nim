import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text)")
exec(db, sql"CREATE TABLE tst2 (a int primary key, b text)")
exec(db, sql"INSERT INTO tst VALUES (1, 'ace')")
exec(db, sql"INSERT INTO tst VALUES (2, 'king')")
exec(db, sql"INSERT INTO tst2 VALUES (2, 'king')")
exec(db, sql"INSERT INTO tst2 VALUES (3, 'queen')")
exec(db, sql"INSERT INTO tst2 VALUES (4, 'jack')")

var rows: seq[seq[string]]
var cols: DbColumns
for r in instantRows(db, cols,
                     sql"""SELECT * FROM tst
                            UNION
                            SELECT * FROM tst2
                            ORDER BY a"""):
  rows.add(@[r[0], r[1]])

doAssert cols.len == 2
doAssert cols[0].name == "A"
doAssert cols[0].typ.kind == dbInt
doAssert cols[1].name == "B"
doAssert cols[1].typ.kind == dbVarchar

doAssert rows.len == 4
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "king"
doAssert rows[2][0] == "3"
doAssert rows[2][1] == "queen"
doAssert rows[3][0] == "4"
doAssert rows[3][1] == "jack"

rows = getAllRows(db,
                  sql"""SELECT * FROM tst
                        UNION ALL
                        SELECT * FROM tst2
                        ORDER BY a""")
doAssert rows.len == 5
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "king"
doAssert rows[2][0] == "2"
doAssert rows[2][1] == "king"
doAssert rows[3][0] == "3"
doAssert rows[3][1] == "queen"
doAssert rows[4][0] == "4"
doAssert rows[4][1] == "jack"

exec(db, sql"CREATE TABLE tst3 (a int primary key, n int)")
exec(db, sql"INSERT INTO tst3 VALUES (3, 11)")

rows = @[]
for r in instantRows(db, cols,
                     sql"""SELECT a, b FROM tst
                           UNION
                           SELECT a, CAST(n AS text) FROM tst3
                           ORDER BY a"""):
  rows.add(@[r[0], r[1]])
doAssert cols.len == 2
doAssert cols[0].name == "A"
doAssert cols[0].typ.kind == dbInt
doAssert cols[1].typ.kind == dbVarchar

doAssert rows.len == 3
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "king"
doAssert rows[2][0] == "3"
doAssert rows[2][1] == "11"
