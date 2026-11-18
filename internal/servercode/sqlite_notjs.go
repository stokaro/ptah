//go:build !js

package servercode

import (
	"errors"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// sqliteUniqueViolation reports whether err is the SQLite driver's refusal of
// a key a row already holds: the extended result codes
// SQLITE_CONSTRAINT_PRIMARYKEY and SQLITE_CONSTRAINT_UNIQUE. Every other
// constraint SQLite reports, such as NOT NULL or CHECK, has a code of its own.
func sqliteUniqueViolation(err error) bool {
	if serverErr, ok := errors.AsType[*sqlite.Error](err); ok {
		code := serverErr.Code()
		return code == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY || code == sqlite3.SQLITE_CONSTRAINT_UNIQUE
	}
	return false
}
