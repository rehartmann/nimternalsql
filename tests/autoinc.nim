import db_nimternalsql

var db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (k bigint primary key autoincrement, n numeric, s text, i bigint autoincrement)""")

doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (2, 'Yo')") == 1
doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (5, 'Yaa')") == 1

var res = getAllRows(db, sql"SELECT * FROM tst ORDER BY k")

doAssert res.len == 2
doAssert res[0][0] == "1"
doAssert res[0][1] == "2"
doAssert res[0][2] == "Yo"
doAssert res[0][3] == "1"
doAssert res[1][0] == "2"
doAssert res[1][1] == "5"
doAssert res[1][2] == "Yaa"
doAssert res[1][3] == "2"

save(db, "snapshot.dump")

db = open("", "", "", "")
restore(db, "snapshot.dump")

doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (6, 'Ji')") == 1

res = getAllRows(db, sql"SELECT * FROM tst ORDER BY k")

doAssert res.len == 3
doAssert res[0][0] == "1"
doAssert res[0][1] == "2"
doAssert res[0][2] == "Yo"
doAssert res[0][3] == "1"
doAssert res[1][0] == "2"
doAssert res[1][1] == "5"
doAssert res[1][2] == "Yaa"
doAssert res[1][3] == "2"
doAssert res[2][0] == "3"
doAssert res[2][1] == "6"
doAssert res[2][2] == "Ji"
doAssert res[2][3] == "3"

close(db)
