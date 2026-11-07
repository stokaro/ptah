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
