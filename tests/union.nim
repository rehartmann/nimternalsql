import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text)")
exec(db, sql"CREATE TABLE tst2 (a int primary key, b text)")
exec(db, sql"INSERT INTO tst VALUES (1, 'ace')")
exec(db, sql"INSERT INTO tst VALUES (2, 'king')")
exec(db, sql"INSERT INTO tst2 VALUES (2, 'king')")
exec(db, sql"INSERT INTO tst2 VALUES (3, 'queen')")
exec(db, sql"INSERT INTO tst2 VALUES (4, 'jack')")

var rows = getAllRows(db, 
                      sql"""SELECT * FROM tst
                            UNION
                            SELECT * FROM tst2
                            ORDER BY a""")
doAssert rows.len == 4
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "king"
doAssert rows[2][0] == "3"
doAssert rows[2][1] == "queen"
doAssert rows[3][0] == "4"
doAssert rows[3][1] == "jack"

rows = getAllRows(db,
                  sql"""SELECT * FROM tst
                        UNION ALL
                        SELECT * FROM tst2
                        ORDER BY a""")
doAssert rows.len == 5
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "ace"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "king"
doAssert rows[2][0] == "2"
doAssert rows[2][1] == "king"
doAssert rows[3][0] == "3"
doAssert rows[3][1] == "queen"
doAssert rows[4][0] == "4"
doAssert rows[4][1] == "jack"
