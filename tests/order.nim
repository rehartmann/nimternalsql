import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int PRIMARY KEY, b text, c text)")
exec(db, sql"INSERT INTO tst VALUES (1, 'y', 'ace')")
exec(db, sql"INSERT INTO tst VALUES (2, 'x', 'king')")
exec(db, sql"INSERT INTO tst VALUES (3, 'y', 'queen')")
exec(db, sql"INSERT INTO tst VALUES (4, 'x', 'jack')")

var rows = getAllRows(db, sql"SELECT * FROM tst t ORDER BY b, t.a")
doAssert rows.len == 4
doAssert rows[0][0] == "2"
doAssert rows[0][1] == "x"
doAssert rows[0][2] == "king"
doAssert rows[1][0] == "4"
doAssert rows[1][1] == "x"
doAssert rows[1][2] == "jack"
doAssert rows[2][0] == "1"
doAssert rows[2][1] == "y"
doAssert rows[2][2] == "ace"
doAssert rows[3][0] == "3"
doAssert rows[3][1] == "y"
doAssert rows[3][2] == "queen"

rows = getAllRows(db, sql"SELECT a, b, c, a + 1 AS a1 FROM tst t ORDER BY a1 DESC")
doAssert rows.len == 4
doAssert rows[0][0] == "4"
doAssert rows[0][1] == "x"
doAssert rows[0][2] == "jack"
doAssert rows[1][0] == "3"
doAssert rows[1][1] == "y"
doAssert rows[1][2] == "queen"
doAssert rows[2][0] == "2"
doAssert rows[2][1] == "x"
doAssert rows[2][2] == "king"
doAssert rows[3][0] == "1"
doAssert rows[3][1] == "y"
doAssert rows[3][2] == "ace"
