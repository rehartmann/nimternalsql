import times
import db_nimternalsql

let db = open("", "", "", "")

exec(db, sql"""CREATE TABLE dt (n int primary key, t time, t6 time(6), d date,
               ts timestamp, ts0 timestamp(0))""")

exec(db, sql"""INSERT INTO dt VALUES(1, '09:01:02', '11:12:13.123456', '2021-12-23',
         '1993-06-13 22:59:59.123456', '2003-06-13 22:59:59')""")

doAssert getValue(db, sql"""SELECT t FROM dt WHERE n = 1""") == "09:01:02"

doAssert getValue(db, sql"""SELECT t6 FROM dt WHERE n = 1""") == "11:12:13.123456"

doAssert getValue(db, sql"""SELECT d FROM dt WHERE n = 1""") == "2021-12-23"

doAssert getValue(db, sql"""SELECT ts FROM dt WHERE n = 1""") == "1993-06-13 22:59:59.123456"

doAssert getValue(db, sql"""SELECT ts0 FROM dt WHERE n = 1""") == "2003-06-13 22:59:59"

doAssert getValue(db, sql"""SELECT t FROM dt WHERE t >= t""") == "09:01:02"
doAssert getValue(db, sql"""SELECT t FROM dt WHERE t >= '09:01:02'""") == "09:01:02"
doAssert getValue(db, sql"""SELECT t FROM dt WHERE '09:01:02' <= t""") == "09:01:02"
doAssert getValue(db, sql"""SELECT t FROM dt WHERE t = t""") == "09:01:02"
doAssert getValue(db, sql"""SELECT t FROM dt WHERE t = '09:01:02'""") == "09:01:02"
doAssert getValue(db, sql"""SELECT t FROM dt WHERE '09:01:02' = t""") == "09:01:02"

doAssert getValue(db, sql"""SELECT d FROM dt WHERE d >= d""") == "2021-12-23"
doAssert getValue(db, sql"""SELECT d FROM dt WHERE d >= '2021-12-23'""") == "2021-12-23"
doAssert getValue(db, sql"""SELECT d FROM dt WHERE '2021-12-23' <= d""") == "2021-12-23"
doAssert getValue(db, sql"""SELECT d FROM dt WHERE d = d""") == "2021-12-23"
doAssert getValue(db, sql"""SELECT d FROM dt WHERE d = '2021-12-23'""") == "2021-12-23"
doAssert getValue(db, sql"""SELECT d FROM dt WHERE '2021-12-23' = d""") == "2021-12-23"

doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts >= ts""") == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts <= ts""") == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts > ts""") == ""
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts < ts""") == ""
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts >= '1993-06-13 22:59:59.123456'""") == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE '1993-06-13 22:59:59.123456' >= ts """) == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts = ts""") == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE ts = '1993-06-13 22:59:59.123456'""") == "1993-06-13 22:59:59.123456"
doAssert getValue(db, sql"""SELECT ts FROM dt WHERE '1993-06-13 22:59:59.123456' = ts """) == "1993-06-13 22:59:59.123456"

exec(db, sql"INSERT INTO dt (n, t, d, ts) VALUES(2, current_time, current_date, current_timestamp)")

discard parse(getValue(db, sql"""SELECT t FROM dt WHERE n = 2"""), "HH:mm:ss")

var datestr = getValue(db, sql"""SELECT d FROM dt WHERE n = 2""")
var dt = parse(datestr, "yyyy-MM-dd", utc())
doAssert dt.year == now().year
doAssert dt.month == now().month
doAssert dt.monthday == now().monthday

datestr = getValue(db, sql"""SELECT ts FROM dt WHERE n = 2""")
dt = parse(datestr, "yyyy-MM-dd HH:mm:ss'.'ffffff", utc())
doAssert dt.year == now().year
doAssert dt.month == now().month
doAssert dt.monthday == now().utc.monthday

exec(db, sql"""CREATE TABLE dt2 (n int primary key, t time(3), ts timestamp(2))""")

exec(db, sql"""INSERT INTO dt2 VALUES(1, '01:02:03.123456', '1993-06-13 20:21:22.123456')""")

doAssert getValue(db, sql"""SELECT t FROM dt2""") == "01:02:03.123"

doAssert getValue(db, sql"""SELECT t FROM dt2 WHERE t = '01:02:03.123'""") == "01:02:03.123"

doAssert getValue(db, sql"""SELECT ts FROM dt2""") == "1993-06-13 20:21:22.12"

doAssert getValue(db, sql"""SELECT ts FROM dt2 WHERE ts = '1993-06-13 20:21:22.12'""") == "1993-06-13 20:21:22.12"

try:
  exec(db, sql"INSERT INTO dt (n, t) VALUES(3, TIME '05:06:7')")
  raiseAssert("invalid time literal accepted")
except DbError as e:
  doAssert e.sqlState() == "22007"
try:
  exec(db, sql"INSERT INTO dt (n, t) VALUES(3, TIME '05:06:07.f')")
  raiseAssert("invalid time literal accepted")
except DbError as e:
  doAssert e.sqlState() == "22007"
exec(db, sql"""INSERT INTO dt (n, t, ts, d)
               VALUES(3, TIME '05:06:08', TIMESTAMP '2000-01-02 05:04:06.123456',
                      DATE '2003-02-01')""")

let row = getRow(db, sql"SELECT t, ts, d FROM dt WHERE n = 3")
doAssert row[0] == "05:06:08"
doAssert row[1] == "2000-01-02 05:04:06.123456"
doAssert row[2] == "2003-02-01"

close(db)
