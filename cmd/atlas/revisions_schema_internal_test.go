package atlas

// White-box testing required: the default is deliberately NOT the flag's
// declared default -- the community binary prints none for --revisions-schema
// and the cli-surface tier compares help text -- so there is nothing on the
// exported command surface that reveals it. The resolver is the only place the
// decision exists.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// Where the revision table belongs is a per-dialect fact, measured against the
// pinned community binary v1.3.0 rather than assumed from one engine:
//
//	PostgreSQL 18   schema atlas_schema_revisions
//	MySQL 8.4       the connected database, not one of its own
//	SQLite          the one namespace there is
//
// Getting this wrong in either direction is visible. Too narrow and the
// PostgreSQL drop-in swap fails in both directions, each binary reporting the
// other's database as never migrated (stokaro/ptah#1563). Too wide and SQLite
// fails outright with `unknown database "atlas_schema_revisions"`, and MySQL
// quietly moves the table out of the database the user connected to.
func TestApplyAtlasRevisionsSchemaDefault(t *testing.T) {
	tests := []struct {
		name     string
		resolved string
		url      string
		want     string
	}{
		{
			name: "postgres takes the atlas schema",
			url:  "postgres://localhost:5432/app?sslmode=disable",
			want: "atlas_schema_revisions",
		},
		{
			// MySQL's schema IS its database, so naming one would move the
			// table out of the database the connection opened.
			name: "mysql keeps the connected database",
			url:  "mysql://localhost:3306/app",
			want: "",
		},
		{
			// There is no schema to name, and passing one is an error rather
			// than a no-op.
			name: "sqlite keeps its single namespace",
			url:  "sqlite://./app.db",
			want: "",
		},
		{
			name:     "an explicit value is never overridden",
			resolved: "custom_revisions",
			url:      "postgres://localhost:5432/app?sslmode=disable",
			want:     "custom_revisions",
		},
		{
			// The command that connects reports a bad URL with context this
			// resolver does not have, so it declines rather than guessing.
			name: "an unreadable url leaves the connection default",
			url:  "::not a url::",
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := applyAtlasRevisionsSchemaDefault(test.resolved, test.url)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The PostgreSQL family is a family: a CockroachDB or YugabyteDB drop-in reads
// the same Atlas history and needs the same answer.
func TestApplyAtlasRevisionsSchemaDefaultCoversThePostgresFamily(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "cockroachdb", url: "cockroachdb://localhost:26257/app?sslmode=disable"},
		{name: "yugabytedb", url: "yugabytedb://localhost:5433/app?sslmode=disable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := applyAtlasRevisionsSchemaDefault("", test.url)

			c.Assert(got, qt.Equals, "atlas_schema_revisions")
		})
	}
}
