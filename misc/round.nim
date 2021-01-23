import db_memnolo

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b numeric(10,5), c numeric(10, 4))")
exec(db, sql"INSERT INTO tst VALUES (1, 1.12345, 0)")
exec(db, sql"UPDATE tst SET c = b")

echo getValue(db, sql"SELECT c FROM tst")
