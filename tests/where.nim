import db_nimternalsql
import db_common
import algorithm
import strutils
    
let db = open("", "", "", "")
exec(db, sql"CREATE TABLE babababa (a text not null, b text, c int primary key, d bigint)")
doAssert execAffectedRows(db,
                          sql"""INSERT INTO babababa (a, b, c)
                                  VALUES('Baba', 'Ba' || lower('Baa') || upper('Baa'), 1)"""
                          ) == 1
exec(db,
     sql"""INSERT INTO babababa (a, b, c) VALUES('Electrica', 'Salsa', (1 + 2) * -3)""")
exec(db,
     sql"""INSERT INTO babababa (a, b, c) VALUES('Alpen', NULL, 2)""")
exec(db,
     sql"""INSERT INTO babababa (a, b, c, d) VALUES(?, ?, ?, ?)""", "Adam", "Eve", "3", "-9223372036854775808")

var rows = getAllRows(db, sql"SELECT * FROM babababa b WHERE b.a = 'Baba' or c > 1")
sort(rows) do (row1, row2: Row) -> int:
  parseInt(row1[2]) - parseInt(row2[2])
doAssert rows.len == 3
doAssert rows[0][0] == "Baba"
doAssert rows[0][1] == "BabaaBAA"
doAssert rows[0][2] == "1"
doAssert rows[1][0] == "Alpen"
doAssert rows[1][1] == ""
doAssert rows[1][2] == "2"
doAssert rows[2][0] == "Adam"
doAssert rows[2][1] == "Eve"
doAssert rows[2][2] == "3"

rows = @[]
var cols: DbColumns
for r in instantRows(db, cols, sql"SELECT * FROM babababa WHERE c IN (1,-9) ORDER BY c"):
  rows.add(@[r[0], r[1], r[2]])

doAssert cols.len == 4
doAssert cols[0].name == "A"
doAssert cols[0].typ.kind == dbVarchar
doAssert cols[1].name == "B"
doAssert cols[1].typ.kind == dbVarchar
doAssert cols[2].name == "C"
doAssert cols[2].typ.kind == dbInt
doAssert cols[2].typ.size == 4
doAssert cols[3].name == "D"
doAssert cols[3].typ.kind == dbInt
doAssert cols[3].typ.size == 8

doAssert rows.len == 2
doAssert rows[0][0] == "Electrica"
doAssert rows[0][1] == "Salsa"
doAssert rows[0][2] == "-9"
doAssert rows[1][0] == "Baba"
doAssert rows[1][1] == "BabaaBAA"
doAssert rows[1][2] == "1"

rows = getAllRows(db, sql"""SELECT * FROM babababa
                              WHERE a NOT IN ('Adam', 'Baba')
                              ORDER BY a""")
doAssert rows.len == 2
doAssert rows[0][0] == "Alpen"
doAssert rows[0][1] == ""
doAssert rows[0][2] == "2"
doAssert rows[1][0] == "Electrica"
doAssert rows[1][1] == "Salsa"
doAssert rows[1][2] == "-9"

rows = getAllRows(db, sql"SELECT * FROM babababa WHERE a LIKE ?", "%lpe%")
doAssert rows.len == 1
doAssert rows[0][0] == "Alpen"
doAssert rows[0][1] == ""
doAssert rows[0][2] == "2"

rows = getAllRows(db, sql"SELECT * FROM babababa WHERE d < ?", "-9223372036854775000")
doAssert rows.len == 1
doAssert rows[0][0] == "Adam"
doAssert rows[0][1] == "Eve"
doAssert rows[0][2] == "3"
doAssert rows[0][3] == "-9223372036854775808"

rows = getAllRows(db, sql"SELECT * FROM babababa WHERE d = ?", "-9223372036854775808")
doAssert rows.len == 1
doAssert rows[0][0] == "Adam"
doAssert rows[0][1] == "Eve"
doAssert rows[0][2] == "3"
doAssert rows[0][3] == "-9223372036854775808"
