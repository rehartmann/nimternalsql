import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE test1 (a text not null, b text, c int primary key)")
exec(db, sql"CREATE TABLE test2 (d text, e int primary key)")

exec(db, sql"INSERT INTO test1 VALUES('Pitch', 'P', 1)")
exec(db, sql"INSERT INTO test1 VALUES('Pitsh', 'Px', 2)")
exec(db, sql"INSERT INTO test2 VALUES('Sulphur', 1)")
exec(db, sql"INSERT INTO test2 VALUES('Sulfur', 2)")

var rows: seq[seq[string]]

var cols: DbColumns
for r in instantRows(db, cols, 
                     sql"""SELECT * FROM test1, test2 t2
                           WHERE test1.c = t2.e
                           ORDER BY a"""):
  rows.add(@[r[0], r[1], r[2], r[3]])

doAssert cols.len == 5
doAssert cols[0].name == "A"
doAssert cols[0].tableName == "TEST1"
doAssert cols[0].typ.kind == dbVarchar
doAssert cols[1].name == "B"
doAssert cols[1].tableName == "TEST1"
doAssert cols[1].typ.kind == dbVarchar
doAssert cols[2].name == "C"
doAssert cols[2].tableName == "TEST1"
doAssert cols[2].typ.kind == dbInt
doAssert cols[3].name == "D"
doAssert cols[3].tableName == "TEST2"
doAssert cols[3].typ.kind == dbVarchar
doAssert cols[4].name == "E"
doAssert cols[4].tableName == "TEST2"
doAssert cols[4].typ.kind == dbInt

doAssert rows.len == 2

rows = getAllRows(db,
                  sql"""SELECT * FROM test2 t2, test1
                          WHERE test1.c = t2.e
                          ORDER BY e""")
doAssert rows.len == 2
doAssert rows[0][0] == "Sulphur"
doAssert rows[0][1] == "1"
doAssert rows[0][2] == "Pitch"
doAssert rows[0][3] == "P"
doAssert rows[0][4] == "1"
doAssert rows[1][0] == "Sulfur"
doAssert rows[1][1] == "2"
doAssert rows[1][2] == "Pitsh"
doAssert rows[1][3] == "Px"
doAssert rows[1][4] == "2"

rows = getAllRows(db,
                  sql"""SELECT * FROM test1, test2 t2, test2 t3
                          WHERE test1.c = t2.e AND t2.e = t3.e
                          ORDER BY c, t2.e, t3.e""")
doAssert rows.len == 4
doAssert rows[0][0] == "Pitch"
doAssert rows[0][1] == "P"
doAssert rows[0][2] == "1"
doAssert rows[0][3] == "Sulphur"
doAssert rows[0][4] == "1"
doAssert rows[0][5] == "Sulphur"
doAssert rows[0][6] == "1"
doAssert rows[1][0] == "Pitch"
doAssert rows[1][1] == "P"
doAssert rows[1][2] == "1"
doAssert rows[1][3] == "Sulfur"
doAssert rows[1][4] == "2"
doAssert rows[1][5] == "Sulfur"
doAssert rows[1][6] == "2"
doAssert rows[2][0] == "Pitsh"
doAssert rows[2][1] == "Px"
doAssert rows[2][2] == "2"
doAssert rows[2][3] == "Sulphur"
doAssert rows[2][4] == "1"
doAssert rows[2][5] == "Sulphur"
doAssert rows[2][6] == "1"
doAssert rows[3][0] == "Pitsh"
doAssert rows[3][1] == "Px"
doAssert rows[3][2] == "2"
doAssert rows[3][3] == "Sulfur"
doAssert rows[3][4] == "2"
doAssert rows[3][5] == "Sulfur"
doAssert rows[3][6] == "2"

try:
  rows = getAllRows(db, sql"""SELECT * FROM test1, test2 t2, test2 t3
                                WHERE test1.c = t2.e and t2.e = e""")
  raiseAssert("SELECT with ambiguous column refs succeeded")
except DbError:
  discard
