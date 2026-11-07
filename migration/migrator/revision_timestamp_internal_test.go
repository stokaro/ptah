package migrator

// White-box testing required: the revision table is created before any Ptah
// command reports anything, so a type the target does not have surfaces as a
// driver error from inside initialization and never as a value a black-box test
// could read. The engine's revision table statement is where the decision is
// made.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestPtahRevisionsTableDDL_TimestampType pins the type each built-in engine's
// revision table gives applied_at. TIMESTAMP is what every one of them accepts,
// so a change that spelled TIMESTAMPTZ everywhere would rewrite the column type
// under every existing PostgreSQL and MySQL deployment.
func TestPtahRevisionsTableDDL_TimestampType(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "postgres", dialect: platform.Postgres, want: "TIMESTAMP"},
		{name: "cockroachdb", dialect: platform.CockroachDB, want: "TIMESTAMP"},
		{name: "yugabytedb", dialect: platform.YugabyteDB, want: "TIMESTAMP"},
		{name: "mysql", dialect: platform.MySQL, want: "TIMESTAMP"},
		{name: "mariadb", dialect: platform.MariaDB, want: "TIMESTAMP"},
		{name: "sqlite", dialect: platform.SQLite, want: "TIMESTAMP"},
		{name: "clickhouse", dialect: platform.ClickHouse, want: "TIMESTAMP"},
		{
			// A zero-value Migrator has no connection and no dialect, and its
			// generated statements are asserted by the guard tests.
			name:    "no dialect at all",
			dialect: "",
			want:    "TIMESTAMP",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(ptahRevisionsTableDDL(test.dialect, "t", "", ""), qt.Contains, "\n    applied_at "+test.want+" NOT NULL,\n")
		})
	}
}

// TestCreateMigrationsTableSQL_CarriesTheTargetsTimestampType is the same rule
// where it is consumed, because a helper nothing calls proves nothing.
func TestCreateMigrationsTableSQL_CarriesTheTargetsTimestampType(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
		absent  string
	}{
		{
			name:    "postgres",
			dialect: platform.Postgres,
			want:    "applied_at TIMESTAMP NOT NULL",
			absent:  "applied_at TIMESTAMPTZ NOT NULL",
		},
		{
			// SQL Server takes its own branch above the timestamp decision, so
			// this row is what keeps a change to that decision from silently
			// rewriting a statement it does not reach.
			name:    "sqlserver keeps DATETIME2",
			dialect: platform.SQLServer,
			want:    "applied_at DATETIME2 NOT NULL",
			absent:  "TIMESTAMPTZ",
		},
		{
			// The MySQL family still gets its engine clause: the extraction
			// that made this function testable moved three call sites off the
			// receiver, and this is the one whose value is not a type.
			name:    "mysql keeps its engine clause",
			dialect: platform.MySQL,
			want:    "ENGINE=InnoDB",
			absent:  "TIMESTAMPTZ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ddl := ptahRevisionsTableDDL(test.dialect, `"ptah_migrations"`, "N'ptah_migrations'", "")

			c.Assert(ddl, qt.Contains, test.want)
			c.Assert(ddl, qt.Not(qt.Contains), test.absent)
		})
	}
}
