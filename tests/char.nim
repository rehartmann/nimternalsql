import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a char(3) primary key, b char, c varchar(5), d text)")

try:
  exec(db, sql"INSERT INTO tst (a, c) VALUES (?, ?)", "t", "123456")
  raiseAssert("value exceeding column length was excepted")
except DbError:
  discard

exec(db, sql"INSERT INTO tst VALUES (?, 'f', 'Don''t  ', ? || UPPER(?) || LOWER(?))",
     "yo", "Yo", "Yo", "Yo")

var res: seq[seq[string]]

for r in instantRows(db, sql"SELECT * FROM tst"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert res.len == 1
doAssert res[0][0] == "yo "
doAssert res[0][1] == "f"
doAssert res[0][2] == "Don't "
doAssert res[0][3] == "YoYOyo"

assert getValue(db, sql"SELECT SUBSTR(d, 2, 2) FROM tst") == "oY"
assert getValue(db, sql"SELECT SUBSTR(d, ?) FROM tst", "2") == "oYOyo"
assert getValue(db, sql"SELECT SUBSTR(d, -1, 1) FROM tst") == "o"
assert getValue(db, sql"SELECT SUBSTR(d, -2 * 1) FROM tst") == "yo"

assert getValue(db, sql"SELECT POSITION('Oy' IN d) FROM tst") == "4"
assert getValue(db, sql"SELECT POSITION('' IN d) FROM tst") == "1"
assert getValue(db, sql"SELECT POSITION('Bab' IN d) FROM tst") == "0"
assert getValue(db, sql"SELECT POSITION('h' IN 'Äh') FROM tst") == "2"

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst WHERE b IN (?)", "f"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert res.len == 1
doAssert res[0][0] == "yo "
doAssert res[0][1] == "f"
doAssert res[0][2] == "Don't "
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

assert getValue(db, sql"SELECT OCTET_LENGTH(c) FROM tst WHERE a = 'yo '") == "2"
assert getValue(db, sql"SELECT LENGTH(c) FROM tst WHERE a = 'yo '") == "2"
assert getValue(db, sql"SELECT CHAR_LENGTH(c) FROM tst WHERE a = 'yo '") == "2"

assert getValue(db, sql"SELECT TRIM(a) FROM tst WHERE TRIM(a) = 'yo'") == "yo"
assert getValue(db, sql"SELECT TRIM(LEADING FROM ' ' || a) FROM tst WHERE a = 'yo '") == "yo "
assert getValue(db, sql"SELECT TRIM(TRAILING FROM ' ' || a) FROM tst WHERE a = 'yo '") == " yo"
assert getValue(db, sql"SELECT TRIM(BOTH FROM ' ' || a) FROM tst WHERE a = 'yo '") == "yo"
assert getValue(db, sql"SELECT TRIM('x' FROM 'xofx') FROM tst WHERE a = 'yo '") == "of"
assert getValue(db, sql"SELECT TRIM(LEADING 'x' FROM 'xofx') FROM tst WHERE a = 'yo '") == "ofx"
assert getValue(db, sql"SELECT TRIM(TRAILING 'x' FROM 'xofx') FROM tst WHERE a = 'yo '") == "xof"
assert getValue(db, sql"SELECT TRIM(BOTH 'x' FROM 'xofx') FROM tst WHERE a = 'yo '") == "of"

exec(db, sql"UPDATE tst SET c = '2' WHERE a = 'yo '")

doAssert getValue(db, sql"SELECT COUNT(*) FROM tst WHERE c > ?", "10") == "1"

exec(db, sql"DROP TABLE tst")

try:
  for r in instantRows(db, sql"SELECT * FROM tst"):
    discard
  raiseAssert("tst is still accessible after DROP table")
except DbError:
  discard
