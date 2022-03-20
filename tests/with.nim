import db_nimternalsql
import db_common

let db = open("", "", "", "")
exec(db, sql"CREATE TABLE tst (a int primary key, b text)")

exec(db, sql"INSERT INTO tst VALUES (1, 'Abcdef')")
exec(db, sql"INSERT INTO tst VALUES (2, 'Bcdefg')")

var rows = getAllRows(db,
                      sql"""WITH t1 AS (
                                SELECT a, b FROM tst WHERE a = 2
                            )
                            SELECT b FROM t1""")

doAssert rows.len == 1
doAssert rows[0][0] == "Bcdefg"

exec(db, sql"CREATE TABLE tst2 (a int primary key, b text)")
exec(db, sql"INSERT INTO tst2 VALUES (1, 'w')")
exec(db, sql"INSERT INTO tst2 VALUES (2, 'x')")
exec(db, sql"INSERT INTO tst2 VALUES (3, 'y')")

rows = getAllRows(db,
                  sql"""WITH t2 AS (
                            SELECT * FROM tst2 WHERE a = 2
                        )
                        SELECT b FROM tst WHERE a IN (SELECT a FROM t2)""")
                            
doAssert rows.len == 1
doAssert rows[0][0] == "Bcdefg"

rows = getAllRows(db,
                  sql"""WITH t2 AS (
                            SELECT * FROM tst2 WHERE a = 2
                        )
                        SELECT b FROM tst WHERE EXISTS (SELECT a FROM t2 WHERE t2.a = tst.a)""")
                            
doAssert rows.len == 1
doAssert rows[0][0] == "Bcdefg"

rows = getAllRows(db,
                  sql"""WITH t2 AS (
                            SELECT a, b FROM tst2 WHERE a = 2
                        )
                        SELECT b FROM tst WHERE EXISTS (SELECT a FROM t2 WHERE t2.a = tst.a)""")

doAssert rows.len == 1
doAssert rows[0][0] == "Bcdefg"

exec(db, sql"CREATE TABLE orders (order_id int primary key, region text, quantity int, amount int, product text)")

exec(db, sql"INSERT INTO orders VALUES(1, 'USA', 7, 50, 'Coffee')")
exec(db, sql"INSERT INTO orders VALUES(2, 'France', 17, 80, 'Coffee')")
exec(db, sql"INSERT INTO orders VALUES(3, 'Italy', 1, 4, 'Tea')")
exec(db, sql"INSERT INTO orders VALUES(4, 'Italy', 5, 10, 'Coffee')")
exec(db, sql"INSERT INTO orders VALUES(5, 'France', 10, 20, 'Tea')")
exec(db, sql"INSERT INTO orders VALUES(6, 'France', 2, 10, 'Coffee')")

rows = getAllRows(db,
          sql"""WITH regional_sales AS (
                    SELECT region, SUM(amount) AS total_sales
                    FROM orders
                    GROUP BY region
                ), top_regions AS (
                    SELECT region
                    FROM regional_sales
                    WHERE total_sales * 10 > (SELECT SUM(total_sales) FROM regional_sales)
                )
                SELECT region,
                    product,
                    SUM(quantity) AS product_units,
                    SUM(amount) AS product_sales
                FROM orders
                WHERE region IN (SELECT region FROM top_regions)
                GROUP BY region, product
                ORDER BY region, product""")

echo $rows.len

for i in 0..<rows.len:
  echo $rows[i]

close(db)
