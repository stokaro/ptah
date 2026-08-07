package migrateclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrateclean"
)

// The expected strings below are transcribed from runs of the pinned community
// binary v1.3.0 on 2026-08-07: PostgreSQL 17 in a throwaway container, MySQL
// 9.7 in a throwaway container, and SQLite files. Each row names the state the
// binary was in when it produced that line.

func TestGoverns_EnforcedDialects(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "postgres driver spelling", dialect: "pgx"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dialect: "sqlite"},
		{name: "sqlite driver spelling", dialect: "sqlite3"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(migrateclean.Governs(test.dialect), qt.IsTrue)
		})
	}
}

// The gate stays off for dialects nobody has run the pinned binary against.
// Turning it on for them would refuse runs that work today with no measurement
// saying the other implementation refuses them, which is the drop-in
// regression the compatibility policy forbids.
func TestGoverns_UnmeasuredDialects(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
	}{
		{name: "clickhouse", dialect: "clickhouse"},
		{name: "sqlserver", dialect: "sqlserver"},
		{name: "spanner", dialect: "spanner"},
		{name: "cockroachdb", dialect: "cockroachdb"},
		{name: "yugabytedb", dialect: "yugabytedb"},
		{name: "empty", dialect: ""},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(migrateclean.Governs(test.dialect), qt.IsFalse)
		})
	}
}

func TestScopeRefusal_CleanDatabases(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		scope migrateclean.Scope
	}{
		{
			// A brand new database. The binary applies at exit 0.
			name: "postgres empty schema",
			scope: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			// Measured: a database holding nothing but an empty
			// atlas_schema_revisions applies at exit 0, so an empty revision
			// table does not make a database unclean on its own.
			name: "postgres revisions table only",
			scope: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				Tables:        []string{"atlas_schema_revisions"},
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			name: "mysql revisions table only",
			scope: migrateclean.Scope{
				Dialect:       "mysql",
				Schema:        "app",
				Tables:        []string{"atlas_schema_revisions"},
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			// Measured: a SQLite database whose only table is
			// atlas_schema_revisions applies at exit 0.
			name: "sqlite revisions table only",
			scope: migrateclean.Scope{
				Dialect:       "sqlite",
				Tables:        []string{"atlas_schema_revisions"},
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			name: "sqlite empty file",
			scope: migrateclean.Scope{
				Dialect:       "sqlite",
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			// A dialect the gate does not govern reports clean even with
			// tables present, so that turning Governs on is the one decision
			// that widens enforcement.
			name: "ungoverned dialect with tables",
			scope: migrateclean.Scope{
				Dialect:       "clickhouse",
				Schema:        "default",
				Tables:        []string{"legacy_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(test.scope.Refusal(), qt.IsNil)
		})
	}
}

func TestScopeRefusal_UncleanDatabases(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		scope   migrateclean.Scope
		wantErr string
	}{
		{
			// The measured quirk: the binary names the alphabetically first
			// table in the schema, and its own revisions table sorts before
			// `legacy_stuff`, so that is the name it reports.
			name: "postgres unrelated table sorting after the revisions table",
			scope: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				Tables:        []string{"atlas_schema_revisions", "legacy_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "atlas_schema_revisions" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The same state with a table that sorts first. This row and the
			// one above are what separate "alphabetically first" from "the
			// offending table": only one ordering satisfies both.
			name: "postgres unrelated table sorting before the revisions table",
			scope: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				Tables:        []string{"aaa_legacy", "atlas_schema_revisions"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "aaa_legacy" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// --revisions-schema moved the bookkeeping out of the connected
			// schema, so `public` holds exactly one table and the binary still
			// refuses. This is the fixture that kills "more than one table" as
			// the predicate.
			name: "postgres single table with revisions kept in another schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Schema:  "public",
				Tables:  []string{"legacy_stuff"},
			},
			wantErr: `sql/migrate: connected database is not clean: found table "legacy_stuff" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// A dry run never creates the revisions table here, while the
			// binary creates it before checking. Counting it anyway is what
			// makes --dry-run report the same table the binary reports.
			name: "postgres dry run before the revisions table exists",
			scope: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				Tables:        []string{"legacy_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "atlas_schema_revisions" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// MySQL reports the database as the schema.
			name: "mysql unrelated table",
			scope: migrateclean.Scope{
				Dialect:       "mysql",
				Schema:        "app",
				Tables:        []string{"aaa_legacy", "atlas_schema_revisions"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "aaa_legacy" in schema "app". baseline version or allow-dirty is required`,
		},
		{
			// SQLite reports a count instead of a name, and the count includes
			// the revisions table.
			name: "sqlite one unrelated table",
			scope: migrateclean.Scope{
				Dialect:       "sqlite",
				Tables:        []string{"atlas_schema_revisions", "legacy_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found multiple tables: 2. baseline version or allow-dirty is required`,
		},
		{
			name: "sqlite two unrelated tables",
			scope: migrateclean.Scope{
				Dialect:       "sqlite",
				Tables:        []string{"atlas_schema_revisions", "legacy_stuff", "other_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found multiple tables: 3. baseline version or allow-dirty is required`,
		},
		{
			// The count survives the dry run too: one real table plus the
			// revisions table the binary would have created is still 2.
			name: "sqlite dry run before the revisions table exists",
			scope: migrateclean.Scope{
				Dialect:       "sqlite",
				Tables:        []string{"legacy_stuff"},
				RevisionTable: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found multiple tables: 2. baseline version or allow-dirty is required`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := test.scope.Refusal()
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
		})
	}
}
