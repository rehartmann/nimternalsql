import db_nimternalsql

var db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (k integer primary key autoincrement, n numeric, s text, i bigint not null autoincrement)""")

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

exec(db, sql"CREATE TABLE tst2 (n numeric primary key, t text)")
exec(db, sql"INSERT INTO tst2 VALUES (10, 'Ba')")
exec(db, sql"INSERT INTO tst2 VALUES (11, 'Bbo')")

exec(db, sql"INSERT INTO tst (n, s) SELECT * FROM tst2")

res = getAllRows(db, sql"SELECT * FROM tst ORDER BY k")

doAssert res.len == 5
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
doAssert res[3][0] == "4"
doAssert res[3][1] == "10"
doAssert res[3][2] == "Ba"
doAssert res[3][3] == "4"
doAssert res[4][0] == "5"
doAssert res[4][1] == "11"
doAssert res[4][2] == "Bbo"
doAssert res[4][3] == "5"

close(db)
