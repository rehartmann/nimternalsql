import db_nimternalsql

var db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (a int primary key, b numeric(10,5) DEFAULT 1.23, c integer, d text, e varchar(40),
                f real, g bigint, h time, k date)""")

exec(db, sql"INSERT INTO tst VALUES (1, 3.6, 1200, 'Water', 'Fire', 7.8, 9223372036854775805, '17:05:06', '1901-05-02')")

save(db, "snapshot.dump")

db = open("", "", "", "")
restore(db, "snapshot.dump")

exec(db, sql"INSERT INTO tst (a, c, d, e, f, g) VALUES (2, 2000, 'Wine', 'Earth', 1.5e5, 1000)")

let rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a")

doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "3.60000"
doAssert rows[0][2] == "1200"
doAssert rows[0][3] == "Water"
doAssert rows[0][4] == "Fire"
doAssert rows[0][5] == "7.8"
doAssert rows[0][6] == "9223372036854775805"
doAssert rows[0][7] == "17:05:06"
doAssert rows[0][8] == "1901-05-02"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "1.23000"
doAssert rows[1][2] == "2000"
doAssert rows[1][3] == "Wine"
doAssert rows[1][4] == "Earth"
doAssert rows[1][5] == "150000.0"
doAssert rows[1][6] == "1000"
