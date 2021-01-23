import db_memnolo
import times

let db = open("", "", "", "")

exec(db, sql"CREATE TABLE tst (a int primary key, b text, c text)")

for i in 1..10000:
  exec(db, sql"INSERT INTO tst VALUES (?, ?, 'y')", $i, $i)

var before = now()

echo getValue(db, sql"SELECT c FROM tst WHERE a = 1")

echo now() - before

before = now()

echo getValue(db, sql"SELECT c FROM tst WHERE b = '1'")

echo now() - before
