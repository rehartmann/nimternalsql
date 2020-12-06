import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, c text, rank text)")

var rows = getAllRows(db, sql"SELECT COUNT(*), MAX(a), SUM(a), AVG(a) FROM tst")
doAssert rows.len == 1
doAssert rows[0][0] == "0"
doAssert rows[0][1] == ""
doAssert rows[0][2] == ""
doAssert rows[0][3] == "" 

exec(db, sql"INSERT INTO tst VALUES (1, 'y', 'Ace')")
exec(db, sql"INSERT INTO tst VALUES (2, 'x', 'King')")
exec(db, sql"INSERT INTO tst VALUES (3, 'z', 'Queen')")
exec(db, sql"INSERT INTO tst VALUES (4, 'x', 'Jack')")
exec(db, sql"INSERT INTO tst VALUES (5, 'y', 'Ace')")
exec(db, sql"INSERT INTO tst VALUES (6, 'y', 'Ace')")
exec(db, sql"INSERT INTO tst VALUES (7, 'z', 'Queen')")

doAssert getValue(db, sql"SELECT COUNT(*) FROM tst") == "7"

rows = getAllRows(db, sql"""SELECT c, rank, COUNT(*), MAX(a), MIN(t.a), SUM(a), AVG(a)
                              FROM tst t
                              GROUP BY rank, c
                              ORDER BY c""")
doAssert rows.len == 4
doAssert rows[0][0] == "x"
doAssert rows[0][1] == "King"
doAssert rows[0][2] == "1"
doAssert rows[0][3] == "2"
doAssert rows[0][4] == "2"
doAssert rows[0][5] == "2"
doAssert rows[0][6] == "2.0"
doAssert rows[1][0] == "x"
doAssert rows[1][1] == "Jack"
doAssert rows[1][2] == "1"
doAssert rows[1][3] == "4"
doAssert rows[1][4] == "4"
doAssert rows[1][5] == "4"
doAssert rows[1][6] == "4.0"
doAssert rows[2][0] == "y"
doAssert rows[2][1] == "Ace"
doAssert rows[2][2] == "3"
doAssert rows[2][3] == "6"
doAssert rows[2][4] == "6"
doAssert rows[2][5] == "12"
doAssert rows[2][6] == "4.0"
doAssert rows[3][0] == "z"
doAssert rows[3][1] == "Queen"
doAssert rows[3][2] == "2"
doAssert rows[3][3] == "7"
doAssert rows[3][4] == "7"
doAssert rows[3][5] == "10"
doAssert rows[3][6] == "5.0"

rows = getAllRows(db, sql"""SELECT COUNT(*), MAX(a), c, rank r
                              FROM tst t
                              GROUP BY c, r
                              ORDER BY r""")
doAssert rows.len == 4
doAssert rows[0][0] == "3"
doAssert rows[0][1] == "6"
doAssert rows[0][2] == "y"
doAssert rows[0][3] == "Ace"
doAssert rows[1][0] == "1"
doAssert rows[1][1] == "4"
doAssert rows[1][2] == "x"
doAssert rows[1][3] == "Jack"
doAssert rows[2][0] == "1"
doAssert rows[2][1] == "2"
doAssert rows[2][2] == "x"
doAssert rows[2][3] == "King"
doAssert rows[3][0] == "2"
doAssert rows[3][1] == "7"
doAssert rows[3][2] == "z"
doAssert rows[3][3] == "Queen"

rows = getAllRows(db, sql"""SELECT rank r, COUNT(*), MAX(a), c
                              FROM tst t
                              WHERE a < ?
                              GROUP BY c, r
                              ORDER BY c""",
                  "8")
doAssert rows.len == 4
doAssert rows[0][0] == "King"
doAssert rows[0][1] == "1"
doAssert rows[0][2] == "2"
doAssert rows[0][3] == "x"
doAssert rows[1][0] == "Jack"
doAssert rows[1][1] == "1"
doAssert rows[1][2] == "4"
doAssert rows[1][3] == "x"
doAssert rows[2][0] == "Ace"
doAssert rows[2][1] == "3"
doAssert rows[2][2] == "6"
doAssert rows[2][3] == "y"
doAssert rows[3][0] == "Queen"
doAssert rows[3][1] == "2"
doAssert rows[3][2] == "7"
doAssert rows[3][3] == "z"
