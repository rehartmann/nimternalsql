import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b int, c text)")
exec(db, sql"CREATE TABLE tst2 (a int primary key, s text)")

exec(db, sql"INSERT INTO tst VALUES (1, 2, 'yo')")
exec(db, sql"INSERT INTO tst VALUES (2, 3, 'ya')")

db.setAutocommit(false)
exec(db, sql"INSERT INTO tst VALUES (3, 3, 'yai')")

exec(db, sql"UPDATE tst SET a = a + 10 WHERE b = 3")
exec(db, sql"UPDATE tst SET c = 'yi' WHERE a = 1")

exec(db, sql"DELETE FROM tst WHERE a = 12");

exec(db, sql"DROP TABLE tst2")

exec(db, sql"CREATE TABLE tst3 (a int primary key, s text)")

exec(db, sql"INSERT INTO tst3 VALUES (1, 'xoo')")

exec(db, sql"ROLLBACK")

var rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "2"
doAssert rows[0][2] == "yo"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "3"
doAssert rows[1][2] == "ya"

# tst2 must exist again
rows = getAllRows(db, sql"SELECT * FROM tst2")

# tst3 must be have been dropped by the rollback
try:
  rows = getAllRows(db, sql"SELECT * FROM tst3")
  raiseAssert("Accessing tst3 was succeeded")
except DbError:
  # ok
  discard

db.close()
