import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text, c text, d text NOT NULL)")
exec(db, sql"INSERT INTO tst VALUES (1, NULL, 'yo', 'x')")
exec(db, sql"INSERT INTO tst (a, b, d) VALUES (2, 'yoyo', 'y')")
exec(db, sql"INSERT INTO tst (a, c, d) VALUES (3, NULL, 'z')")
exec(db, sql"INSERT INTO tst VALUES (4, NULL, NULL, 'z')")

try:
  exec(db, sql"INSERT INTO tst (a, c, d) VALUES (3, NULL, NULL)")
  raiseAssert("INSERT with non-nullable column set to NULL should fail")
except DbError:
  discard

var res: seq[seq[string]]

for r in instantRows(db, sql"SELECT * FROM tst ORDER BY a"):
  res.add(@[r[0], r[1], r[2], r[3]])
doAssert res.len == 4
doAssert res[0][0] == "1"
doAssert res[0][1] == ""
doAssert res[0][2] == "yo"
doAssert res[0][3] == "x"
doAssert res[1][0] == "2"
doAssert res[1][1] == "yoyo"
doAssert res[1][2] == ""
doAssert res[1][3] == "y"
doAssert res[2][0] == "3"
doAssert res[2][1] == ""
doAssert res[2][2] == ""
doAssert res[2][3] == "z"
doAssert res[3][0] == "4"
doAssert res[3][1] == ""
doAssert res[3][2] == ""
doAssert res[3][3] == "z"

var count = 0
for r in instantRows(db, sql"SELECT * FROM tst WHERE a = NULL"):
  count += 1
doAssert count == 0

var rows = getAllRows(db, sql"SELECT b FROM tst WHERE a IN (?, ?) ORDER BY b", 1, 2)
doAssert rows.len == 2
doAssert rows[0][0] == ""
doAssert rows[1][0] == "yoyo"

exec(db, sql"UPDATE tst SET b = NULL WHERE a = 2")

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst WHERE a = 2 AND b IS NULL"):
  res.add(@[r[0], r[1], r[2], r[3]])
doAssert res.len == 1
doAssert res[0][0] == "2"
doAssert res[0][1] == ""
doAssert res[0][2] == ""
doAssert res[0][3] == "y"

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst WHERE c IS NOT NULL"):
  res.add(@[r[0], r[1], r[2], r[3]])
doAssert res.len == 1
doAssert res[0][0] == "1"
doAssert res[0][1] == ""
doAssert res[0][2] == "yo"
doAssert res[0][3] == "x"

# Error
try:
  exec(db, sql"UPDATE tst SET a = NULL WHERE a = 2")
  raiseAssert("UPDATE setting primary key to NULL should fail")
except DbError:
  discard

try:
  exec(db, sql"UPDATE tst SET d = NULL WHERE a = 2")
  raiseAssert("UPDATE setting non-null column to NULL should fail")
except DbError:
  discard

doAssert getValue(db, sql"SELECT TRUE OR b FROM tst WHERE a = 4") == ""
