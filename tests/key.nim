import db_nimternalsql

let db = open("", "", "", "")

try:
  exec(db, sql"CREATE TABLE tst (a int primary key, a text, c text)")
  raiseAssert("CREATE TABLE succeeded with two columns having the same name")
except DbError:
  discard

exec(db, sql"CREATE TABLE tst (a int primary key, b text, c text)")

exec(db, sql"INSERT INTO tst VALUES (1, 'x', 'y')")

var rows = getAllRows(db, sql"SELECT * FROM tst WHERE a = 1")
doAssert rows.len == 1
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "x"
doAssert rows[0][2] == "y"

try:
  exec(db, sql"CREATE TABLE tst (a int, b text, c text, primary key(a, b))")
  raiseAssert("CREATE TABLE recreating existing table succeeded")
except DbError:
  discard

try:
  exec(db, sql"INSERT INTO tst VALUES (1, 'x', 'z ')")
  raiseAssert("INSERT succeeded with same key")
except DbError:
  discard

exec(db, sql"CREATE TABLE tst2 (a int, b text, c text, primary key(a, b))")

exec(db, sql"INSERT INTO tst2 VALUES (1, 'x', 'y')")

rows = getAllRows(db, sql"SELECT * FROM tst2 WHERE a = 1 AND b = ?", "x")
doAssert rows.len == 1
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "x"
doAssert rows[0][2] == "y"

rows = getAllRows(db, sql"SELECT * FROM tst2 WHERE a = 1 AND b = 'x' AND c ='Y'")
doAssert rows.len == 0

rows = getAllRows(db, sql"SELECT * FROM tst2 WHERE a = 1")
doAssert rows.len == 1
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "x"
doAssert rows[0][2] == "y"

rows = getAllRows(db, sql"SELECT a, b FROM tst2 WHERE a = 1 AND b = 'x'")
doAssert rows.len == 1
doAssert rows[0].len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "x"

try:
  exec(db, sql"INSERT INTO tst2 VALUES (1, 'x', 'y')")
  raiseAssert("INSERT succeeded with same key")
except DbError:
  discard
