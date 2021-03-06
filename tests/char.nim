import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a char(3) primary key, b char, c varchar(5), d text)")

exec(db, sql"INSERT INTO tst VALUES (?, 'f', 'Don''t', ? || UPPER(?) || LOWER(?))",
     "yo", "Yo", "Yo", "Yo")

var res: seq[seq[string]]

for r in instantRows(db, sql"SELECT * FROM tst"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert res.len == 1
doAssert res[0][0] == "yo "
doAssert res[0][1] == "f"
doAssert res[0][2] == "Don't"
doAssert res[0][3] == "YoYOyo"

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst WHERE b IN (?)", "f"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert res.len == 1
doAssert res[0][0] == "yo "
doAssert res[0][1] == "f"
doAssert res[0][2] == "Don't"
doAssert res[0][3] == "YoYOyo"

exec(db, sql"UPDATE tst SET b = '', c = 'Do', d = d || d")

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert res.len == 1
doAssert res[0][0] == "yo "
doAssert res[0][1] == " "
doAssert res[0][2] == "Do"
doAssert res[0][3] == "YoYOyoYoYOyo"

assert getValue(db, sql"SELECT COUNT(*) FROM tst WHERE c >= 'Do'") == "1"

assert getValue(db, sql"SELECT COUNT(*) FROM tst WHERE c > 'Do'") == "0"

try:
  echo getValue(db, sql"SELECT COUNT(*) FROM tst WHERE c >= 3")
  raiseAssert("comparing character column with number succeeded")
except DbError:
  discard

exec(db, sql"UPDATE tst SET c = '2' WHERE a = 'yo '")

doAssert getValue(db, sql"SELECT COUNT(*) FROM tst WHERE c > ?", "10") == "1"

exec(db, sql"DROP TABLE tst")

try:
  for r in instantRows(db, sql"SELECT * FROM tst"):
    discard
  raiseAssert("tst is still accessible after DROP table")
except DbError:
  discard
