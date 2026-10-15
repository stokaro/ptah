package scratchdb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/internal/scratchdb"
)

// TestCanIsolate_HappyPath pins the engines a case can get a database of its
// own on: a file on SQLite, a dev realm on YDB, and a database the server
// creates on the PostgreSQL and MySQL families.
func TestCanIsolate_HappyPath(t *testing.T) {
	for _, baseURL := range []string{
		"",
		"sqlite://file.db",
		"ydb://localhost:2136/local",
		"postgres://localhost:5432/app",
		"cockroachdb://localhost:26257/app",
		"yugabytedb://localhost:5433/app",
		"spanner://localhost:5432/app",
		"mysql://localhost:3306/app",
		"mariadb://localhost:3306/app",
	} {
		t.Run(baseURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(scratchdb.CanIsolate(baseURL), qt.IsNil)
		})
	}
}

// TestCanIsolate_FailurePath pins the engines that cannot isolate a case,
// which are refused rather than shared.
func TestCanIsolate_FailurePath(t *testing.T) {
	for _, baseURL := range []string{
		"clickhouse://localhost:9000/app",
		"sqlserver://localhost:1433?database=app",
		"oracle://localhost:1521/app",
	} {
		t.Run(baseURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(scratchdb.CanIsolate(baseURL), qt.ErrorIs, scratchdb.ErrNoIsolation)
		})
	}
}
