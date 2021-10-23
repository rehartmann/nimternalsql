import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE emp (empno int primary key, first_name text not null, last_name text)")
exec(db, sql"CREATE TABLE emp_team (empno int, team_name text, primary key (empno, team_name))")
exec(db, sql"CREATE TABLE emp2 (no int primary key, first_name text not null, last_name text, team_name text)")

exec(db, sql"INSERT INTO emp VALUES(1, 'Bob', 'Base')")
exec(db, sql"INSERT INTO emp VALUES(2, 'Daisy', 'Database')")
exec(db, sql"INSERT INTO emp_team VALUES(1, 'Team A')")
exec(db, sql"INSERT INTO emp_team VALUES(2, 'Team B')")
exec(db, sql"INSERT INTO emp2 VALUES(1, 'Bob', 'Base', 'Team A')")

var rows = getAllRows(db, sql"""SELECT * FROM emp_team t
                              LEFT JOIN emp ON t.empno = emp.empno
                              ORDER BY last_name""")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Team A"
doAssert rows[0][2] == "1"
doAssert rows[0][3] == "Bob"
doAssert rows[0][4] == "Base"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "Team B"
doAssert rows[1][2] == "2"
doAssert rows[1][3] == "Daisy"
doAssert rows[1][4] == "Database"

rows = getAllRows(db, sql"""SELECT * FROM emp_team LEFT JOIN emp2
                                ON no = empno
                                ORDER BY emp_team.team_name""")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Team A"
doAssert rows[0][2] == "1"
doAssert rows[0][3] == "Bob"
doAssert rows[0][4] == "Base"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "Team B"
doAssert rows[1][2] == ""
doAssert rows[1][3] == ""
doAssert rows[1][4] == ""

rows = getAllRows(db, sql"""SELECT * FROM emp_team LEFT JOIN emp2
                                ON emp_team.team_name = emp2.team_name
                                ORDER BY emp_team.team_name""")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Team A"
doAssert rows[0][2] == "1"
doAssert rows[0][3] == "Bob"
doAssert rows[0][4] == "Base"
doAssert rows[0][5] == "Team A"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "Team B"
doAssert rows[1][2] == ""
doAssert rows[1][3] == ""
doAssert rows[1][4] == ""
doAssert rows[1][5] == ""
