import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE s (sno integer primary key, sname text)")
exec(db, sql"CREATE TABLE sp (sno integer, pno integer, primary key(sno, pno))")

exec(db, sql"INSERT INTO s VALUES(1, 'one')")
exec(db, sql"INSERT INTO s VALUES(2, 'two')")
exec(db, sql"INSERT INTO s VALUES(3, 'three')")
exec(db, sql"INSERT INTO s VALUES(4, 'four')")
exec(db, sql"INSERT INTO sp VALUES(1, 1)")
exec(db, sql"INSERT INTO sp VALUES(2, 2)")
exec(db, sql"INSERT INTO sp VALUES(3, 1)")
exec(db, sql"INSERT INTO sp VALUES(4, 3)")

var res = getAllRows(db, sql"""SELECT s.sname FROM s
                                  WHERE EXISTS (
                                    SELECT * FROM sp
                                      WHERE sp.pno = s.sno)
                                  ORDER BY s.sname""")
doAssert res.len == 3
doAssert res[0][0] == "one"
doAssert res[1][0] == "three"
doAssert res[2][0] == "two"

res = getAllRows(db, sql"""SELECT DISTINCT s.sname FROM s
                             WHERE 1 IN (
                               SELECT sp.pno FROM sp
                                 WHERE sp.sno = s.sno)
                             ORDER BY s.sname""")
doAssert res.len == 2
doAssert res[0][0] == "one"
doAssert res[1][0] == "three"

res = getAllRows(db, sql"""SELECT DISTINCT s.sname FROM s
                             WHERE s.sno = (
                               SELECT sp.sno FROM sp
                                 WHERE sp.pno = 3)""")
doAssert res.len == 1
doAssert res[0][0] == "four"
