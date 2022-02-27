import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (k int PRIMARY KEY, n numeric(10, 1), s text)""")
exec(db, sql"""INSERT INTO tst VALUES(1, 5.5, 'T')""")

doAssert getValue(db, sql"""SELECT k + CAST ('2' AS INT) FROM tst""") == "3"
doAssert getValue(db, sql"""SELECT k + CAST ('2' AS REAL) FROM tst""") == "3.0"
doAssert getValue(db, sql"""SELECT k + CAST ('2' AS DOUBLE PRECISION) FROM tst""") == "3.0"
doAssert getValue(db, sql"""SELECT k + CAST ('2' AS BIGINT) FROM tst""") == "3"
doAssert getValue(db, sql"""SELECT CAST (n AS NUMERIC(4,2)) FROM tst""") == "5.5"
doAssert getValue(db, sql"""SELECT ':' || CAST (n as TEXT) FROM tst""") == ":5.5"
doAssert getValue(db, sql"""SELECT CAST (s || 'RUE' AS BOOLEAN) FROM tst""") == "TRUE"

var cols: DbColumns
var val: string
for r in instantRows(db, cols, sql"""SELECT CAST(s || 'TX' AS CHAR(3)) FROM tst"""):
  val = r[0]

doAssert val == "TTX"
doAssert cols[0].typ.kind == dbFixedChar

try:
  echo getValue(db, sql"""SELECT CAST(500 + n AS NUMERIC(1, 1)) FROM tst""")
  raiseAssert("CAST with insufficient precision succeeded")
except DbError as e:
  doAssert sqlState(e) == "22003"
try:
  echo getValue(db, sql"""SELECT CAST(s || 'TX' AS CHAR(2)) FROM tst""")
  raiseAssert("CAST with insufficient size succeeded")
except DbError as e:
  doAssert sqlState(e) == "22001"

exec(db, sql"""INSERT INTO tst VALUES(2, 5.5, '05:')""")

var row = getRow(db, sql"""SELECT CAST(s || '10:12' AS TIME),
                                  CAST('2002-12-02' AS DATE),
                                  CAST('1902-02-03 ' || s || '01:03.223344' AS TIMESTAMP)
                           FROM tst""")
doAssert row[0] == "05:10:12"
doAssert row[1] == "2002-12-02"
doAssert row[2] == "1902-02-03 05:01:03.223344"

row = getRow(db, sql"""SELECT CAST(s || '10:12.345' AS TIME(2)),
                                  CAST('2002-12-02' AS DATE),
                                  CAST('1902-02-03 ' || s || '01:03.223344' AS TIMESTAMP(5))
                           FROM tst""")
doAssert row[0] == "05:10:12.34"
doAssert row[1] == "2002-12-02"
doAssert row[2] == "1902-02-03 05:01:03.22334"

close(db)
