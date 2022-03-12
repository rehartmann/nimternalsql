# NimternalSQL, an in-memory SQL database for Nim

NimternalSQL is a Nim library providing an in-memory SQL database.
It uses the same interface as the Nim db_*.nim database wrappers.

Tables are implemented using hash tables, so specifying a key using PRIMARY KEY is mandatory.
Compound keys are supported.

## Data types

The following data types are supported:

* INTEGER
* TEXT
* CHAR
* VARCHAR
* DECIMAL
* NUMERIC
* REAL
* DOUBLE PRECISION
* BIGINT
* BOOLEAN
* BINARY
* VARBINARY
* LONGVARBINARY
* RAW
* BYTEA
* TIME
* DATE
* TIMESTAMP

DECIMAL and NUMERIC are internally represented as 64-bit integers. The maximum number of digits is 18.

AUTOINCREMENT is supported on INTEGER and BIGINT columns. It is not restricted to key columns.

## Scalar operators

Besides the usual arithmetic, comparison, and logical operators, NimternalSQL supports the following scalar operators:

* || (string concatenation)
* CASE
* CAST
* CHAR_LENGTH
* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* LENGTH
* LOWER
* UPPER
* LIKE
* OCTET_LENGTH
* POSITION
* SUBSTR
* TRIM

## Persistence

Persistence is supported through snapshots or (optionally) a transaction
log.

## Transactions

By default, NimternalSQL is in autocommit mode. Autocommit mode can be
enabled or disabled using DbConn.setAutocommit().

## Unimplemented SQL features

A number of SQL features is not implemented, most notably:

OUTER JOIN (JOIN .. ON, CROSS JOIN, and LEFT JOIN are supported)

HAVING (GROUP BY is supported)

ALTER TABLE

Views
