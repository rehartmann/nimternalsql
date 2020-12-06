import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text, c varchar(2))")

exec(db, sql"INSERT INTO tst VALUES (1, 'Fritz', 'ui')")
exec(db, sql"INSERT INTO tst VALUES (2, 'Fritz', 'ui')")
exec(db, sql"INSERT INTO tst VALUES (3, 'Fritz', 'uh')")

let rows = getAllRows(db, sql"SELECT DISTINCT b, c FROM tst ORDER BY c")
doAssert rows.len == 2
doAssert rows[0][0] == "Fritz"
doAssert rows[0][1] == "uh"
doAssert rows[1][0] == "Fritz"
doAssert rows[1][1] == "ui"
