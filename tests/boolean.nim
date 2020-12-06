import db_nimternalsql
import strutils

let db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (a int primary key, b boolean, c boolean)""")

doAssert execAffectedRows(db, sql"INSERT INTO tst VALUES (1, TRUE, false)") == 1

var rows = getAllRows(db, sql"SELECT b, c, b AND c, b OR c FROM tst")

doAssert rows.len == 1
doAssert rows[0][0] == "TRUE"
doAssert rows[0][1] == "FALSE"
doAssert rows[0][2] == "FALSE"
doAssert rows[0][3] == "TRUE"
