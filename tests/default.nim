import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst (
                   a char(3) DEFAULT 'nix' PRIMARY KEY,
                   b char DEFAULT 'x' not null,
                   c varchar(5))""")

# Error
try:
  exec(db, sql"INSERT INTO tst (a) VALUES('yay', 'Yo')")
  raiseAssert("INSERT succeeded although # of values differed from # of columns")
except DbError as e:
  doAssert e.sqlState() == "42601"

exec(db, sql"INSERT INTO tst (a, c) VALUES('yay', 'Yo')")
exec(db, sql"INSERT INTO tst DEFAULT VALUES")

var res: seq[seq[string]]

for r in instantRows(db, sql"SELECT * FROM tst ORDER BY a"):
  res.add(@[r[0], r[1], r[2]])

doAssert res.len == 2
doAssert res[0][0] == "nix"
doAssert res[0][1] == "x"
doAssert res[0][2] == ""
doAssert res[1][0] == "yay"
doAssert res[1][1] == "x"
doAssert res[1][2] == "Yo"
