import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b int, c text)")
exec(db, sql"INSERT INTO tst VALUES (1, 2, 'yo')")
exec(db, sql"INSERT INTO tst VALUES (2, 3, 'ya')")

exec(db, sql"UPDATE tst SET c = 'yoyo')")

var rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY b")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "2"
doAssert rows[0][2] == "yoyo"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "3"
doAssert rows[1][2] == "yoyo"

exec(db, sql"UPDATE tst SET b = b + 10 WHERE a = 1)")

rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "12"
doAssert rows[0][2] == "yoyo"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "3"
doAssert rows[1][2] == "yoyo"

exec(db, sql"UPDATE tst SET a = a + 10, c = 'x' WHERE a = 1)")

rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a DESC")
doAssert rows.len == 2
doAssert rows[0][0] == "11"
doAssert rows[0][1] == "12"
doAssert rows[0][2] == "x"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "3"
doAssert rows[1][2] == "yoyo"

exec(db, sql"UPDATE tst SET a = a + 10, c = c || 'y')")

rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a DESC")
doAssert rows.len == 2
doAssert rows[0][0] == "21"
doAssert rows[0][1] == "12"
doAssert rows[0][2] == "xy"
doAssert rows[1][0] == "12"
doAssert rows[1][1] == "3"
doAssert rows[1][2] == "yoyoy"
