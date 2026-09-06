package migrator

// White-box testing required: the revision table is created before any Ptah
// command reports anything, so a type the target does not have surfaces as a
// driver error from inside initialization and never as a value a black-box test
// could read. revisionTimestampType is where the decision is made.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestRevisionTimestampType_SpannerHasNoTimestamp pins the one engine whose
// PostgreSQL interface does not have the type every other engine spells the
// same way.
//
// `ptah migrations up` could not run against Spanner at all, and neither could
// `ptah migrations status`: both failed on the revision table, before the first
// migration. Measured against the Cloud Spanner emulator behind PGAdapter
// 0.55.2 (stokaro/ptah#2233):
//
//	applied_at TIMESTAMP    ERROR: Type <timestamp> is not supported. (SQLSTATE P0001)
//	applied_at TIMESTAMPTZ  accepted
//
// The other rows are the control. TIMESTAMP is what the column has always been
// and what six engines accept, so a change that spelled TIMESTAMPTZ everywhere
// would rewrite the column type under every existing PostgreSQL and MySQL
// deployment; it would also pass a table that only checked Spanner.
func TestRevisionTimestampType_SpannerHasNoTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "spanner", dialect: platform.Spanner, want: "TIMESTAMPTZ"},
		{name: "postgres", dialect: platform.Postgres, want: "TIMESTAMP"},
		{name: "cockroachdb", dialect: platform.CockroachDB, want: "TIMESTAMP"},
		{name: "yugabytedb", dialect: platform.YugabyteDB, want: "TIMESTAMP"},
		{name: "mysql", dialect: platform.MySQL, want: "TIMESTAMP"},
		{name: "mariadb", dialect: platform.MariaDB, want: "TIMESTAMP"},
		{name: "sqlite", dialect: platform.SQLite, want: "TIMESTAMP"},
		{name: "clickhouse", dialect: platform.ClickHouse, want: "TIMESTAMP"},
		{
			// A dialect alias, so the answer cannot depend on the caller having
			// normalized first: a Migrator built without a live connection
			// reaches this with whatever string it was given.
			name:    "a spanner alias",
			dialect: "cloudspanner",
			want:    "TIMESTAMPTZ",
		},
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

			c.Assert(revisionTimestampType(test.dialect), qt.Equals, test.want)
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
			name:    "spanner",
			dialect: platform.Spanner,
			want:    "applied_at TIMESTAMPTZ NOT NULL",
			absent:  "applied_at TIMESTAMP NOT NULL",
		},
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

// TestIsPostgresFamily_SpannerIsExcludedOnPurpose pins the divergence from
// [platform.IsPostgresFamily], which does count Spanner in.
//
// It looks like an oversight and reads like one, which is why it is asserted
// rather than left to a comment. The predicate selects exactly one thing: a
// metadata query written around `current_schema()`, which Spanner's PostgreSQL
// interface refuses inside a query. Measured against the Cloud Spanner emulator
// behind PGAdapter 0.55.2 (stokaro/ptah#2233):
//
//	SELECT ... WHERE table_schema = current_schema()
//	ERROR: Postgres function current_schema() is not supported
//
// Aligning the two predicates would replace a read that works with one the
// server refuses.
func TestIsPostgresFamily_SpannerIsExcludedOnPurpose(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		migrator bool
		exported bool
	}{
		{name: "spanner is the divergence", dialect: platform.Spanner, migrator: false, exported: true},
		{name: "postgres is in both", dialect: platform.Postgres, migrator: true, exported: true},
		{name: "cockroachdb is in both", dialect: platform.CockroachDB, migrator: true, exported: true},
		{name: "yugabytedb is in both", dialect: platform.YugabyteDB, migrator: true, exported: true},
		{name: "mysql is in neither", dialect: platform.MySQL, migrator: false, exported: false},
		{name: "clickhouse is in neither", dialect: platform.ClickHouse, migrator: false, exported: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(usesPostgresMetadataQueries(test.dialect), qt.Equals, test.migrator)
			c.Assert(platform.IsPostgresFamily(test.dialect), qt.Equals, test.exported)
		})
	}
}
