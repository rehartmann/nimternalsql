import db_nimternalsql
import os

removeDir("db")

var db = open("db", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (n numeric, k bigint primary key autoincrement, s text, i int autoincrement)""")

doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (2, 'Yo')") == 1
doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (5, 'Yaa')") == 1

var res = getAllRows(db, sql"SELECT * FROM tst ORDER BY k")

doAssert res.len == 2
doAssert res[0][0] == "2"
doAssert res[0][1] == "1"
doAssert res[0][2] == "Yo"
doAssert res[0][3] == "1"
doAssert res[1][0] == "5"
doAssert res[1][1] == "2"
doAssert res[1][2] == "Yaa"
doAssert res[1][3] == "2"

close(db)

db = open("db", "", "", "")

doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (6, 'Ji')") == 1
doAssert execAffectedRows(db, sql"INSERT INTO tst (n, s) VALUES (7, 'Yj')") == 1

res = getAllRows(db, sql"SELECT * FROM tst ORDER BY k")

doAssert res.len == 4
doAssert res[0][0] == "2"
doAssert res[0][1] == "1"
doAssert res[0][2] == "Yo"
doAssert res[0][3] == "1"
doAssert res[1][0] == "5"
doAssert res[1][1] == "2"
doAssert res[1][2] == "Yaa"
doAssert res[1][3] == "2"
doAssert res[2][0] == "6"
doAssert res[2][1] == "3"
doAssert res[2][2] == "Ji"
doAssert res[2][3] == "3"
doAssert res[3][0] == "7"
doAssert res[3][1] == "4"
doAssert res[3][2] == "Yj"
doAssert res[3][3] == "4"

close(db)
