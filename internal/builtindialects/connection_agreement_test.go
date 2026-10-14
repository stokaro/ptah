package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_ConnectionAgreesWithTheSwitchTheyReplace pins Driver and
// WireScheme against the switch dbschema chose a driver with before the
// descriptors carried them, written out here as it stood: databaseDriverConfig
// gave each dialect its database/sql driver, and convertPostgresWireURL spelled
// the scheme of a CockroachDB, YugabyteDB or Spanner URL as postgres for pgx.
// SQLite's libsql transport is the URL's choice rather than the dialect's, so
// it is not a row here; internal/connector/sqliteconnect pins it.
func TestDescriptors_ConnectionAgreesWithTheSwitchTheyReplace(t *testing.T) {
	tests := []struct {
		dialect    string
		driver     string
		wireScheme string
	}{
		{dialect: platform.Postgres, driver: "pgx"},
		{dialect: platform.CockroachDB, driver: "pgx", wireScheme: "postgres"},
		{dialect: platform.YugabyteDB, driver: "pgx", wireScheme: "postgres"},
		{dialect: platform.Spanner, driver: "pgx", wireScheme: "postgres"},
		{dialect: platform.MySQL, driver: "mysql"},
		{dialect: platform.MariaDB, driver: "mysql"},
		{dialect: platform.ClickHouse, driver: "clickhouse"},
		{dialect: platform.SQLite, driver: "sqlite"},
		{dialect: platform.SQLServer, driver: "sqlserver"},
		{dialect: platform.Oracle, driver: "oracle"},
		{dialect: platform.YDB, driver: "ydb"},
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	// Every built-in dialect has a row, so a new one cannot go unpinned.
	c.Assert(described, qt.HasLen, len(tests))
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got, found := platform.Lookup(test.dialect)

			c.Assert(found, qt.IsTrue)
			c.Assert(got.Driver, qt.Equals, test.driver)
			c.Assert(got.WireScheme, qt.Equals, test.wireScheme)
		})
	}
}
