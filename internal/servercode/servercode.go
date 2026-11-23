// Package servercode reads the codes a database server answered with out of a
// driver's error.
//
// A run document reports these codes for a caller that must not publish what a
// database said: the message of the same error can quote a row of data or the
// statement, and a code cannot. The migrator's run document, its migration log
// report and the `schema apply` document read through here, so the three agree on
// what each engine's driver gives them.
//
// The reading is from the driver's error type and never from the message, which
// a server can localize.
package servercode

import (
	"errors"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// numberedError is what the SQL Server driver's error offers, read through the
// method so that this package does not name a driver it has no other use for.
type numberedError interface {
	error
	SQLErrorNumber() int32
}

// Read returns the SQLSTATE and the error number a server answered with.
//
// SQLSTATE comes from the PostgreSQL protocol, which gives no number, and from
// the MySQL protocol, which gives both. SQL Server, Oracle and ClickHouse give a
// number and no SQLSTATE. Both results are empty when the server gave no answer,
// such as after a lost connection or a canceled statement, and for an engine
// whose driver error carries neither, such as SQLite's.
//
// A number means something only on the engine that gave it: 1205 is a deadlock
// on SQL Server and a lock wait timeout on MySQL.
func Read(err error) (sqlState string, errorNumber int) {
	if serverErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return serverErr.Code, 0
	}
	if serverErr, ok := errors.AsType[*mysqldriver.MySQLError](err); ok {
		if serverErr.SQLState != [5]byte{} {
			sqlState = string(serverErr.SQLState[:])
		}
		return sqlState, int(serverErr.Number)
	}
	if serverErr, ok := errors.AsType[numberedError](err); ok {
		return "", int(serverErr.SQLErrorNumber())
	}
	if serverErr, ok := errors.AsType[*clickhousedriver.Exception](err); ok {
		return "", int(serverErr.Code)
	}
	return "", oracleErrorNumber(err)
}

// UniqueViolation reports whether a server refused a statement because a row
// already holds the unique key the statement wrote: SQLSTATE 23505 from the
// PostgreSQL protocol, error 1062 from MySQL and MariaDB, 2627 or 2601 from SQL
// Server, ORA-00001 from Oracle, and the extended result code 1555
// (SQLITE_CONSTRAINT_PRIMARYKEY) or 2067 (SQLITE_CONSTRAINT_UNIQUE) from the
// SQLite driver. It reads the driver's error type, so a number one engine uses
// for this means nothing on another, and it is false for every other error,
// including one no server answered.
func UniqueViolation(err error) bool {
	if serverErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return serverErr.Code == "23505"
	}
	if serverErr, ok := errors.AsType[*mysqldriver.MySQLError](err); ok {
		return serverErr.Number == 1062
	}
	if serverErr, ok := errors.AsType[numberedError](err); ok {
		number := serverErr.SQLErrorNumber()
		return number == 2627 || number == 2601
	}
	if _, ok := errors.AsType[*clickhousedriver.Exception](err); ok {
		return false
	}
	return oracleErrorNumber(err) == 1 || sqliteUniqueViolation(err)
}

// RefusedInATransaction reports whether a PostgreSQL-protocol server refused a
// statement because it ran inside a transaction block: SQLSTATE 25001
// (active_sql_transaction), such as CREATE INDEX CONCURRENTLY or VACUUM in a
// transaction; 2D000 (invalid_transaction_termination), such as a COMMIT in a
// procedure a transaction calls; and 55P04 (unsafe_new_enum_value_usage), an
// enum value used in the transaction that added it. It is false for every
// other error, including one from another protocol and one no server answered.
func RefusedInATransaction(err error) bool {
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return false
	}
	switch serverErr.Code {
	case "25001", "2D000", "55P04":
		return true
	}
	return false
}
