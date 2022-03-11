import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int, b text, c int primary key)")
exec(db, sql"CREATE TABLE tst2 (a int, b text, c int, primary key(c, b))")
exec(db, sql"INSERT INTO tst VALUES (1, 'ace', 1)")
exec(db, sql"INSERT INTO tst VALUES (2, 'king', 2)")
exec(db, sql"INSERT INTO tst VALUES (2, 'jack', 3)")
exec(db, sql"INSERT INTO tst2 VALUES (2, 'king', 2)")
exec(db, sql"INSERT INTO tst2 VALUES (3, 'queen', 2)")
exec(db, sql"INSERT INTO tst2 VALUES (4, 'jack', 4)")

var rows: seq[seq[string]]
var cols: DbColumns

for r in instantRows(db, cols,
                     sql"""SELECT * FROM tst
                           INTERSECT
                           SELECT * FROM tst2
                           ORDER BY c"""):
  rows.add(@[r[0], r[1], r[2]])

doAssert rows.len == 1
doAssert rows[0][0] == "2"
doAssert rows[0][1] == "king"
doAssert rows[0][2] == "2"

exec(db, sql"CREATE TABLE tst3 (a int, b text primary key)")
exec(db, sql"INSERT INTO tst3 VALUES (2, 'king')")
exec(db, sql"INSERT INTO tst3 VALUES (2, 'jack')")
exec(db, sql"INSERT INTO tst3 VALUES (3, 'ace')")

rows = getAllRows(db, sql"""SELECT a, b FROM tst
                            INTERSECT
                            SELECT * FROM tst3
                            ORDER BY b DESC""")

doAssert rows.len == 2
doAssert rows[0][0] == "2"
doAssert rows[0][1] == "king"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "jack"

rows = getAllRows(db, sql"""SELECT * FROM tst3
                            INTERSECT
                            SELECT a, b FROM tst
                            ORDER BY b DESC""")

doAssert rows.len == 2
doAssert rows[0][0] == "2"
doAssert rows[0][1] == "king"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "jack"

rows = getAllRows(db, sql"""SELECT c FROM tst2
                            INTERSECT
                            SELECT c * 2 FROM tst
                            WHERE c = 2
                            ORDER BY c""")

doAssert rows.len == 1
doAssert rows[0][0] == "4"

rows = getAllRows(db, sql"""SELECT a FROM tst
                            INTERSECT ALL
                            SELECT 2 AS a FROM tst""")

doAssert rows.len == 2
doAssert rows[0][0] == "2"
doAssert rows[0][0] == "2"

close(db)
