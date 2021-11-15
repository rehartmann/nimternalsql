import times
import db_nimternalsql

let db = open("", "", "", "")

exec(db, sql"CREATE TABLE dt (n int primary key, t time, d date, ts timestamp, ts0 timestamp(0))")

exec(db, sql"INSERT INTO dt VALUES(1, '09:01:02', '2021-12-23', '1993-06-13 22:59:59.123456', '1993-06-13 22:59:59')")

assert getValue(db, sql"""SELECT t FROM dt WHERE n = 1""") == "09:01:02"

assert getValue(db, sql"""SELECT d FROM dt WHERE n = 1""") == "2021-12-23"

assert getValue(db, sql"""SELECT ts FROM dt WHERE n = 1""") == "1993-06-13 22:59:59.123456"

assert getValue(db, sql"""SELECT ts0 FROM dt WHERE n = 1""") == "1993-06-13 22:59:59"

exec(db, sql"INSERT INTO dt (n, t, d, ts) VALUES(2, current_time, current_date, current_timestamp)")

discard parse(getValue(db, sql"""SELECT t FROM dt WHERE n = 2"""), "HH:mm:ss")

var datestr = getValue(db, sql"""SELECT d FROM dt WHERE n = 2""")
var dt = parse(datestr, "yyyy-MM-dd", utc())
assert dt.year == now().year
assert dt.month == now().month
assert dt.monthday == now().monthday

datestr = getValue(db, sql"""SELECT ts FROM dt WHERE n = 2""")
dt = parse(datestr, "yyyy-MM-dd HH:mm:ss'.'ffffff", utc())
assert dt.year == now().year
assert dt.month == now().month
assert dt.monthday == now().monthday

close(db)
