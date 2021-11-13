import db_nimternalsql

let db = open("", "", "", "")

exec(db, sql"CREATE TABLE dt (n int primary key, t time, d date)")

exec(db, sql"INSERT INTO dt VALUES(1, '09:01:02', '2021-12-23')")

assert getValue(db, sql"""SELECT t FROM dt WHERE n = 1""") == "09:01:02"

assert getValue(db, sql"""SELECT d FROM dt WHERE n = 1""") == "2021-12-23"

exec(db, sql"INSERT INTO dt VALUES(2, current_time, current_date)")

discard getValue(db, sql"""SELECT t FROM dt WHERE n = 2""")

discard getValue(db, sql"""SELECT d FROM dt WHERE n = 2""")

close(db)
