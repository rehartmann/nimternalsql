import db_nimternalsql

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE emp (empno int primary key, firstName text not null, last_name text)")
exec(db, sql"CREATE TABLE emp_team (empno int, team_name text, primary key (empno, team_name))")
exec(db, sql"CREATE TABLE emp2 (no int primary key, firstName text not null, last_name text)")

exec(db, sql"INSERT INTO emp VALUES(1, 'Bob', 'Base')")
exec(db, sql"INSERT INTO emp VALUES(2, 'Daisy', 'Database')")
exec(db, sql"INSERT INTO emp_team VALUES(1, 'Team A')")
exec(db, sql"INSERT INTO emp_team VALUES(2, 'Team B')")
exec(db, sql"INSERT INTO emp2 VALUES(1, 'Bob', 'Base')")

var rows = getAllRows(db,
                      sql"""SELECT * FROM emp
                              CROSS JOIN emp_team
                              ORDER BY emp.empno, emp_team.empno""")
doAssert rows.len == 4
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Bob"
doAssert rows[0][2] == "Base"
doAssert rows[0][3] == "1"
doAssert rows[0][4] == "Team A"
doAssert rows[1][0] == "1"
doAssert rows[1][1] == "Bob"
doAssert rows[1][2] == "Base"
doAssert rows[1][3] == "2"
doAssert rows[1][4] == "Team B"
doAssert rows[2][0] == "2"
doAssert rows[2][1] == "Daisy"
doAssert rows[2][2] == "Database"
doAssert rows[2][3] == "1"
doAssert rows[2][4] == "Team A"
doAssert rows[3][0] == "2"
doAssert rows[3][1] == "Daisy"
doAssert rows[3][2] == "Database"
doAssert rows[3][3] == "2"
doAssert rows[3][4] == "Team B"

rows = getAllRows(db,
                  sql"""SELECT * FROM emp
                        CROSS JOIN emp_team t1 CROSS JOIN emp_team t2
                        WHERE emp.empno = t1.empno and t1.empno = t2.empno
                        ORDER BY emp.empno""")
doAssert rows.len == 2
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Bob"
doAssert rows[0][2] == "Base"
doAssert rows[0][3] == "1"
doAssert rows[0][4] == "Team A"
doAssert rows[1][0] == "2"
doAssert rows[1][1] == "Daisy"
doAssert rows[1][2] == "Database"
doAssert rows[1][3] == "2"
doAssert rows[1][4] == "Team B"

rows = getAllRows(db, sql"""SELECT * FROM emp_team t
                              INNER JOIN emp ON t.empno = emp.empno
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

rows = getAllRows(db, sql"""SELECT * FROM emp2 JOIN emp_team t 
                                ON no = empno
                                ORDER BY last_name""")
doAssert rows.len == 1
doAssert rows[0][0] == "1"
doAssert rows[0][1] == "Bob"
doAssert rows[0][2] == "Base"
doAssert rows[0][3] == "1"
doAssert rows[0][4] == "Team A"
