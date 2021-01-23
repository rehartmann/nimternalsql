import db_memnolo
let db = open("", "", "", "")
db.close()

db.exec(sql"DROP TABLE IF EXISTS my_table")
db.exec(sql("""CREATE TABLE my_table (
                 id integer PRIMARY KEY,
                 name varchar(50) not null)"""))

db.exec(sql"INSERT INTO my_table (id, name) VALUES (0, ?)",
        "Nicolas")
