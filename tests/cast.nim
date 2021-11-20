import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"""CREATE TABLE tst
                (k int PRIMARY KEY, n numeric(10, 1), s text)""")
exec(db, sql"""INSERT INTO tst VALUES(1, 5.5, 'T')""")

assert getValue(db, sql"""SELECT k + CAST ('2' AS INT) FROM tst""") == "3"
assert getValue(db, sql"""SELECT k + CAST ('2' AS REAL) FROM tst""") == "3.0"
assert getValue(db, sql"""SELECT k + CAST ('2' AS DOUBLE PRECISION) FROM tst""") == "3.0"
assert getValue(db, sql"""SELECT k + CAST ('2' AS BIGINT) FROM tst""") == "3"
assert getValue(db, sql"""SELECT CAST (n AS NUMERIC(4,2)) FROM tst""") == "5.5"
assert getValue(db, sql"""SELECT ':' || CAST (n as TEXT) FROM tst""") == ":5.5"
assert getValue(db, sql"""SELECT CAST (s || 'RUE' AS BOOLEAN) FROM tst""") == "TRUE"
assert getValue(db, sql"""SELECT CAST(s || 'TX' AS CHAR(3)) FROM tst""") == "TTX"

try:
  echo getValue(db, sql"""SELECT CAST(500 + n AS NUMERIC(1, 1)) FROM tst""")
  raiseAssert("CAST with insufficient precision succeeded")
except:
  discard
try:
  echo getValue(db, sql"""SELECT CAST(s || 'TX' AS CHAR(2)) FROM tst""")
  raiseAssert("CAST with insufficient size succeeded")
except:
  discard

exec(db, sql"""INSERT INTO tst VALUES(2, 5.5, '05:')""")

var row = getRow(db, sql"""SELECT CAST(s || '10:12' AS TIME),
                                  CAST('2002-12-02' AS DATE),
                                  CAST('1902-02-03 ' || s || '01:03.223344' AS TIMESTAMP)
                           FROM tst""")
assert row[0] == "05:10:12"
assert row[1] == "2002-12-02"
assert row[2] == "1902-02-03 05:01:03.223344"

row = getRow(db, sql"""SELECT CAST(s || '10:12.345' AS TIME(2)),
                                  CAST('2002-12-02' AS DATE),
                                  CAST('1902-02-03 ' || s || '01:03.223344' AS TIMESTAMP(5))
                           FROM tst""")
assert row[0] == "05:10:12.34"
assert row[1] == "2002-12-02"
assert row[2] == "1902-02-03 05:01:03.22334"

close(db)
