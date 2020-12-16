import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b char, c text)")

exec(db, prepare(db, sql"INSERT INTO tst VALUES ($1, $2, $3)"), "1", "y", "Yoyoo")

doAssert tryExec(db, prepare(db, sql"INSERT INTO tst VALUES ($1, $2, $3)"),
                 "2", "c", "Xox") == true

doAssert execAffectedRows(db, prepare(db, sql"INSERT INTO tst VALUES ($1, $2, $3)"),
                 "3", "d", "bloo") == 1

var res: seq[seq[string]]
for r in instantRows(db, prepare(db, sql"SELECT * FROM tst WHERE a <= $1 ORDER BY a"), "2"):
  res.add(@[r[0], r[1], r[2]])

doAssert res.len == 2
doAssert res[0][0] == "1"
doAssert res[0][1] == "y"
doAssert res[0][2] == "Yoyoo"
doAssert res[1][0] == "2"
doAssert res[1][1] == "c"
doAssert res[1][2] == "Xox"

res = @[]
for r in rows(db, prepare(db, sql"SELECT * FROM tst WHERE a <= $1 ORDER BY a"), "2"):
  res.add(@[r[0], r[1], r[2]])

doAssert res.len == 2
doAssert res[0][0] == "1"
doAssert res[0][1] == "y"
doAssert res[0][2] == "Yoyoo"
doAssert res[1][0] == "2"
doAssert res[1][1] == "c"
doAssert res[1][2] == "Xox"

res = getAllRows(db, prepare(db, sql"SELECT * FROM tst WHERE a >= $1 ORDER BY b"), "2")

doAssert res.len == 2
doAssert res[0][0] == "2"
doAssert res[0][1] == "c"
doAssert res[0][2] == "Xox"
doAssert res[1][0] == "3"
doAssert res[1][1] == "d"
doAssert res[1][2] == "bloo"

let row = getRow(db, prepare(db, sql"SELECT * FROM tst WHERE a = $1"), "1")
doAssert row[0] == "1"
doAssert row[1] == "y"
doAssert row[2] == "Yoyoo"

let val = getValue(db, prepare(db, sql"SELECT c FROM tst WHERE a = $1"), "1")
doAssert val == "Yoyoo"
