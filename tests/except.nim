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
                           EXCEPT
                           SELECT * FROM tst2
                           ORDER BY c"""):
  rows.add(@[r[0], r[1], r[2]])

doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[0][2] == "1"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "jack"
doAssert rows[1][2] == "3"

rows = getAllRows(db, sql"""SELECT * FROM tst2
                            EXCEPT
                            SELECT * FROM tst
                            ORDER BY a""")

doAssert rows.len == 2
doAssert rows[0][0] == "3"
doAssert rows[0][1] == "queen"
doAssert rows[1][0] == "4"
doAssert rows[1][1] == "jack"

rows = getAllRows(db, sql"""SELECT a, b FROM tst
                            EXCEPT
                            SELECT a, b FROM tst2
                            ORDER BY a""")

doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "jack"

rows = getAllRows(db, sql"""SELECT c FROM tst2
                            EXCEPT
                            SELECT c * 2 FROM tst
                            WHERE c = 2
                            ORDER BY c""")

doAssert rows.len == 1
doAssert rows[0][0] == "2"

rows = getAllRows(db, sql"""SELECT c FROM tst2
                            EXCEPT ALL
                            SELECT 4 FROM tst
                            ORDER BY c""")

doAssert rows.len == 2
doAssert rows[0][0] == "2"
doAssert rows[1][0] == "2"
