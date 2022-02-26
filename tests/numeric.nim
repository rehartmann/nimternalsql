import db_nimternalsql
import db_common
import strutils

let db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (a int primary key, b numeric, c numeric(10), d numeric(18, 5),
                e real, f integer, g double precision, h bigint)""")

doAssert execAffectedRows(db, sql"INSERT INTO tst VALUES (1, 2, 3.6, 1200.004, 130.12, 7, 1.7e6, 9223372036854775807)") == 1

var res: seq[Row]

for r in rows(db, sql"SELECT * FROM tst"):
  res.add(r)

doAssert res.len == 1
doAssert res[0][0] == "1"
doAssert res[0][1] == "2"
doAssert res[0][2] == "4"
doAssert res[0][3] == "1200.00400"
doAssert res[0][4] == "130.12"
doAssert res[0][5] == "7"
doAssert parseFloat(res[0][6]) == 1.7e6
doAssert res[0][7] == "9223372036854775807"

res = getAllRows(db, sql"SELECT t.d * 2, t.e * 2, t.d * 2.1, 1.1 * t.e, d + f, h - b FROM tst t")

doAssert res.len == 1
doAssert parseFloat(res[0][0]) == 2400.008
doAssert res[0][1] == "260.24"
doAssert parseFloat(res[0][2]) == 2520.0084
doAssert res[0][3] == "143.132"
doAssert parseFloat(res[0][4]) == 1207.004
doAssert res[0][5] == "9223372036854775805"

res = getAllRows(db,
                 sql"""SELECT ? + ?, 5.5 + 200.25, 20 - ?, ? / ?,
                              CASE
                                WHEN tst.b > 1000 THEN 'Very high'
                                WHEN tst.b > 100 THEN 'High'
                                WHEN tst.b > 10 THEN 'Medium'
                                WHEN tst.b > 1 THEN 'Low'
                                ELSE 'Very low'
                              END,
                              CASE tst.f
                                WHEN 6 THEN 'Six'
                                WHEN 7 THEN 'Seven'
                                WHEN 8 THEN 'Eight'
                                ELSE 'Other'
                              END,
                              CASE tst.f
                                WHEN 6 THEN 'Six'
                              END
                       FROM tst""",
                 "3", "2", "1.1", "5", "2")

doAssert res.len == 1
doAssert res[0][0] == "5"
doAssert res[0][1] == "205.75"
doAssert res[0][2] == "18.9"
doAssert res[0][3] == "2.5"
doAssert res[0][4] == "Low"
doAssert res[0][5] == "Seven"
doAssert res[0][6] == ""

try:
  exec(db, sql"UPDATE tst SET f = h")
  raiseAssert("updating integer column with large bigint succeeded")
except DbError:
  discard

exec(db, sql"UPDATE tst SET f = h / 100000000000000")

exec(db, sql"CREATE TABLE tst2 (a decimal primary key, b decimal(10), c dec(18, 8))")

doAssert execAffectedRows(db, sql"INSERT INTO tst2 VALUES (1, 12345678901, 1111111111.11111111)") == 1

res = @[]
var cols: DbColumns
for r in instantRows(db, cols, sql"SELECT a, b, c, c + 1000000000.00000001, a + 1 a1 FROM tst2"):
  res.add(@[r[0], r[1], r[2], r[3]])

doAssert cols.len == 5
doAssert cols[0].name == "A"
doAssert cols[0].typ.kind == dbDecimal
doAssert cols[1].name == "B"
doAssert cols[1].typ.kind == dbDecimal
doAssert cols[2].name == "C"
doAssert cols[2].typ.kind == dbDecimal
doAssert cols[3].name == ""
doAssert cols[3].typ.kind == dbUnknown
doAssert cols[4].name == "A1"
doAssert cols[4].typ.kind == dbUnknown

doAssert res[0][0] == "1"
doAssert res[0][1] == "12345678901"
doAssert res[0][2] == "1111111111.11111111"
doAssert res[0][3] == "2111111111.11111112"

exec(db, sql"UPDATE tst SET d = 12345.123455, f = 12345.5")

var r = getRow(db, sql"SELECT d, f FROM tst")
doAssert r[0] == "12345.12346"
doAssert r[1] == "12346"

doAssert getValue(db, sql"SELECT COUNT(*) FROM tst WHERE b <= ?", "2") == "1"

try:
  exec(db, sql"UPDATE tst set d = 12345678901234.1234")
  raiseAssert("UPDATE succeded although value was too large")
except DbError:
  discard

close(db)
