import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a binary primary key, b bytea, c varbinary, d longvarbinary, e raw)")

let s1 = "\x01\x02AB\xFE\xFF"
let s2 = "\x01\x02\x00AB\xFE\xFF"
let s3 = "\x00ab"
let s4 = "xX\x00\x08\x09"
let s5 = "\n\r\""
exec(db, sql"INSERT INTO tst VALUES (?, ?, ?, ?, ?)", s1, s2, s3, s4, s5)

let row = getRow(db, sql"SELECT a, b, c, d, e FROM tst")

doAssert row[0] == s1
doAssert row[1] == s2
doAssert row[1].len == 7
doAssert row[2] == s3
doAssert row[3] == s4
doAssert row[4] == s5

doAssert getValue(db,
                  sql"""SELECT COUNT(*)
                        FROM tst
                        WHERE a = ?""",
                  s1) == "1"

close(db)
