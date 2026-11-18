package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
)

// TestForeignKeyReferencesCheckedAtCreate_NamesTheMeasuredEngines pins the
// engines whose server refuses, at CREATE TABLE, a foreign key whose
// referenced columns the target's policy does not accept: PostgreSQL,
// CockroachDB, YugabyteDB, MySQL, MariaDB and SQL Server, each measured and
// stated beside its descriptor. SQLite creates such a table and refuses the
// first write to it, and Oracle and ClickHouse were not measured, so the field
// is unset there. Every built-in descriptor is listed, so a new one states its
// value here.
func TestForeignKeyReferencesCheckedAtCreate_NamesTheMeasuredEngines(t *testing.T) {
	c := qt.New(t)
	want := map[string]bool{
		platform.Postgres:    true,
		platform.CockroachDB: true,
		platform.YugabyteDB:  true,
		platform.MySQL:       true,
		platform.MariaDB:     true,
		platform.SQLServer:   true,
		platform.SQLite:      false,
		platform.Oracle:      false,
		platform.ClickHouse:  false,
	}
	got := make(map[string]bool)
	for _, described := range builtindialects.Descriptors() {
		got[described.Name] = described.ForeignKeyReferencesCheckedAtCreate
	}

	c.Assert(got, qt.DeepEquals, want)
}
