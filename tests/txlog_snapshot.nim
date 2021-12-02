import db_nimternalsql
import os

removeDir("db")

var db = open("db", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (a int primary key, b numeric(10,5) DEFAULT 1.23, c integer, d text, e varchar(40),
                f real)""")

exec(db, sql"INSERT INTO tst VALUES (1, 3.6, 1200, 'Water', 'Fire', 7.8)")

exec(db, sql"INSERT INTO tst VALUES (2, 7.6, 200, 'Earth', 'Fire', 7.7)")

exec(db, sql"DELETE FROM tst WHERE a = 2")

exec(db, sql"""CREATE TABLE ttst
                (t time primary key, d date, ts timestamp, t3 time(3), ts2 timestamp(2))""")

exec(db, sql"""INSERT INTO ttst VALUES ('20:05:10', '1950-05-11', '2002-03-05 15:16:17.654321',
                                        time '23:10:11.234',
                                        timestamp '1901-01-02 03:04:05.56')""")

save(db)

exec(db, sql"INSERT INTO tst (a, c, d, e, f) VALUES (3, 2000, 'Wine', 'Earth', 1.5e5)")

close(db)

db = open("db", "", "", "")

var rows = getAllRows(db, sql"SELECT * FROM tst ORDER BY a")

doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "3.60000"
doAssert rows[0][2] == "1200"
doAssert rows[0][3] == "Water"
doAssert rows[0][4] == "Fire"
doAssert rows[0][5] == "7.8"
doAssert rows[1][0] == "3"
doAssert rows[1][1] == "1.23000"
doAssert rows[1][2] == "2000"
doAssert rows[1][3] == "Wine"
doAssert rows[1][4] == "Earth"
doAssert rows[1][5] == "150000.0"

rows = getAllRows(db, sql"SELECT * FROM ttst")

doAssert rows.len == 1
doAssert rows[0][0] == "20:05:10"
doAssert rows[0][1] == "1950-05-11"
doAssert rows[0][2] == "2002-03-05 15:16:17.654321"
doAssert rows[0][3] == "23:10:11.234"
doAssert rows[0][4] == "1901-01-02 03:04:05.56"

close(db)
