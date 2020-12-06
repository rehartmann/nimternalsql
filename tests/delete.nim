import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b int, c text)")
exec(db, sql"INSERT INTO tst VALUES (1, 2, 'yo')")
exec(db, sql"INSERT INTO tst VALUES (2, 3, 'ya')")
exec(db, sql"INSERT INTO tst VALUES (3, 3, 'yayo')")
exec(db, sql"INSERT INTO tst VALUES (4, 10, 'yo')")

doAssert execAffectedRows(db, sql"DELETE FROM tst WHERE a = 1") == 1

var res: seq[seq[string]]
for r in instantRows(db, sql"SELECT * FROM tst ORDER BY a"):
  res.add(@[r[0], r[1], r[2]])

doAssert res.len == 3
doAssert res[0][0] == "2"
doAssert res[0][1] == "3"
doAssert res[0][2] == "ya"
doAssert res[1][0] == "3"
doAssert res[1][1] == "3"
doAssert res[1][2] == "yayo"
doAssert res[2][0] == "4"
doAssert res[2][1] == "10"
doAssert res[2][2] == "yo"

doAssert execAffectedRows(db, sql"DELETE FROM tst WHERE c='yo'") == 1

res = @[]
for r in instantRows(db, sql"SELECT * FROM tst ORDER BY a"):
  res.add(@[r[0], r[1], r[2]])
doAssert res.len == 2
doAssert res[0][0] == "2"
doAssert res[0][1] == "3"
doAssert res[0][2] == "ya"
doAssert res[1][0] == "3"
doAssert res[1][1] == "3"
doAssert res[1][2] == "yayo"

doAssert execAffectedRows(db, sql"DELETE FROM tst") == 2

doAssert getValue(db, sql"SELECT COUNT(*) FROM tst") == "0"
