import db_nimternalsql
import algorithm

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text, c varchar(2))")

exec(db, sql"INSERT INTO tst VALUES (1, 'Fritz', 'ui')")
exec(db, sql"INSERT INTO tst VALUES (2, 'Fritz', 'ui')")
exec(db, sql"INSERT INTO tst VALUES (3, 'Fritz', 'uh')")

var rows = getAllRows(db, sql"SELECT DISTINCT b, c FROM tst ORDER BY c")
doAssert rows.len == 2
doAssert rows[0][0] == "Fritz"
doAssert rows[0][1] == "uh"
doAssert rows[1][0] == "Fritz"
doAssert rows[1][1] == "ui"

rows = getAllRows(db, sql"SELECT DISTINCT b, c FROM tst")
rows.sort(proc(r1, r2: Row): int =
      result = cmp(r1[1], r2[1]))
