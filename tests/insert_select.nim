import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b char, c text)")

exec(db, sql"INSERT INTO tst VALUES (1, 'a', 'Alpha')")
exec(db, sql"INSERT INTO tst VALUES (2, 'b', 'Beta')")
exec(db, sql"INSERT INTO tst VALUES (3, 'g', 'Gamma')")

exec(db, sql"CREATE TABLE tst2 (b char primary key, c text)")

doAssert execAffectedRows(db, sql"INSERT INTO tst2 SELECT b, c FROM tst") == 3

var rows = getAllRows(db, sql"SELECT * FROM tst2 ORDER BY c")
doAssert rows[0][0] == "a"
doAssert rows[0][1] == "Alpha"
doAssert rows[1][0] == "b"
doAssert rows[1][1] == "Beta"
doAssert rows[2][0] == "g"
doAssert rows[2][1] == "Gamma"

exec(db, sql"CREATE TABLE tst3 (x char primary key, y text)")

doAssert execAffectedRows(db, sql"INSERT INTO tst3 (y, x) SELECT c, b FROM tst") == 3

rows = getAllRows(db, sql"SELECT * FROM tst3 ORDER BY y")
doAssert rows[0][0] == "a"
doAssert rows[0][1] == "Alpha"
doAssert rows[1][0] == "b"
doAssert rows[1][1] == "Beta"
doAssert rows[2][0] == "g"
doAssert rows[2][1] == "Gamma"

exec(db, sql"CREATE TABLE tst4 (y text default '-', x char primary key)")

doAssert execAffectedRows(db, sql"INSERT INTO tst4 (x) SELECT b FROM tst") == 3

rows = getAllRows(db, sql"SELECT * FROM tst4 ORDER BY x")
doAssert rows[0][0] == "-"
doAssert rows[0][1] == "a"
doAssert rows[1][0] == "-"
doAssert rows[1][1] == "b"
doAssert rows[2][0] == "-"
doAssert rows[2][1] == "g"

close(db)
