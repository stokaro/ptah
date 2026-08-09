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

// Realm scope: the connection pinned no schema, so the operand is schemas.
//
// These rows are the states the pinned community binary v1.3.0 applied against
// on 2026-08-07 through a plain `postgres://user:pass@host:port/db?sslmode=disable`
// URL — no search_path — against a throwaway PostgreSQL 17 database per cell.

func TestScopeRefusal_CleanRealms(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		scope migrateclean.Scope
	}{
		{
			// A brand new database: `public` exists and is empty.
			name: "empty database",
			scope: migrateclean.Scope{
				Dialect:         "postgres",
				Realm:           true,
				Schemas:         []migrateclean.RealmSchema{{Name: "public"}},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
		},
		{
			// The state a first realm-scope run leaves behind. The bookkeeping
			// schema holding nothing but the revision table applies at exit 0.
			name: "bookkeeping schema beside an empty public",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "atlas_schema_revisions", Tables: []string{"atlas_schema_revisions"}},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
		},
		{
			// The bookkeeping schema exists but is empty, which is what a
			// --dry-run leaves behind.
			name: "empty bookkeeping schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "atlas_schema_revisions"},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
		},
		{
			// --revisions-schema moves the exemption with it.
			name: "bookkeeping schema named by --revisions-schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "public"},
					{Name: "revs", Tables: []string{"atlas_schema_revisions"}},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "revs",
			},
		},
		{
			// --revisions-schema public makes `public` the bookkeeping schema,
			// and the revision table alone in it is then tolerated as such.
			name: "bookkeeping schema pointed at public",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "public", Tables: []string{"atlas_schema_revisions"}},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "public",
			},
		},
		{
			// The gate stays off for a dialect Governs does not cover even when
			// the realm is full.
			name: "ungoverned dialect with schemas",
			scope: migrateclean.Scope{
				Dialect:         "clickhouse",
				Realm:           true,
				Schemas:         []migrateclean.RealmSchema{{Name: "extra"}},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(test.scope.Refusal(), qt.IsNil)
		})
	}
}

func TestScopeRefusal_UncleanRealms(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		scope   migrateclean.Scope
		wantErr string
	}{
		{
			// The cell stokaro/ptah#1257 is about. An EMPTY extra schema is
			// enough: the binary is not looking for tables at this scope. The
			// same database through `?search_path=public` applies at exit 0,
			// which is pinned as a clean row of the schema-scope table above.
			name: "an empty extra schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "extra"},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			name: "a table living only in another schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "extra", Tables: []string{"legacy_stuff"}},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			// `public` is tolerated only while it is empty, and a table in it
			// is reported by the SCHEMA shape rather than the table shape.
			name: "a table in public",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "public", Tables: []string{"legacy_stuff"}},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "public". baseline version or allow-dirty is required`,
		},
		{
			// This row and the next one separate "the first offender by name"
			// from "the first offender in the list": only name order satisfies
			// both, and the offenders sort either side of `public`.
			name: "an offender sorting before public",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "extra"},
					{Name: "public", Tables: []string{"legacy_stuff"}},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			name: "an offender sorting after public",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "public", Tables: []string{"legacy_stuff"}},
					{Name: "zextra"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The bookkeeping schema does not outrank a schema sorting before
			// it: the walk stops at the first failure either way.
			name: "a dirty bookkeeping schema behind another offender",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "aextra"},
					{Name: "atlas_schema_revisions", Tables: []string{"other"}},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "aextra". baseline version or allow-dirty is required`,
		},
		{
			name: "a dirty bookkeeping schema ahead of another offender",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "atlas_schema_revisions", Tables: []string{"other"}},
					{Name: "bextra"},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found 2 tables in schema "atlas_schema_revisions". baseline version or allow-dirty is required`,
		},
		{
			// The count includes the revision table the binary creates before
			// it looks, which is why one unrelated table reads as two. Without
			// the addition a --dry-run would report `1`.
			name: "one unrelated table in the bookkeeping schema",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "atlas_schema_revisions", Tables: []string{"other"}},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found 2 tables in schema "atlas_schema_revisions". baseline version or allow-dirty is required`,
		},
		{
			// The same shape once --revisions-schema points at public.
			name: "bookkeeping in public beside another table",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "public", Tables: []string{"atlas_schema_revisions", "legacy_stuff"}},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "public",
			},
			wantErr: `sql/migrate: connected database is not clean: found 2 tables in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The exemption is keyed on THIS run's bookkeeping schema, so the
			// default one becomes an ordinary offender when the run moved.
			name: "the default bookkeeping schema while the run moved",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "atlas_schema_revisions", Tables: []string{"atlas_schema_revisions"}},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "revs",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "atlas_schema_revisions". baseline version or allow-dirty is required`,
		},
		{
			// Byte order, not the server's collation: PostgreSQL's default
			// collation sorts "app" first and the binary reports "Zed".
			name: "uppercase schema names sort by byte",
			scope: migrateclean.Scope{
				Dialect: "postgres",
				Realm:   true,
				Schemas: []migrateclean.RealmSchema{
					{Name: "Zed"},
					{Name: "app"},
					{Name: "public"},
				},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "Zed". baseline version or allow-dirty is required`,
		},
		{
			// A realm scope for a dialect whose realm shapes this package does
			// not implement must never read as clean. Inspect refuses to build
			// one, and a hand-built one says so rather than passing.
			name: "a dialect with no realm rule",
			scope: migrateclean.Scope{
				Dialect:         "mysql",
				Realm:           true,
				Schemas:         []migrateclean.RealmSchema{{Name: "appdb"}},
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
			wantErr: `migrate apply clean check has no realm-scope rule for dialect "mysql"`,
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

// ForRevisions is the boundary between the catalog read and the decision, and
// the two scopes fill different fields from the same call.
func TestScopeForRevisions(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name            string
		scope           migrateclean.Scope
		revisionsSchema string
		revisionTable   string
		want            migrateclean.Scope
	}{
		{
			name:          "schema scope keeps the table it will create",
			scope:         migrateclean.Scope{Dialect: "postgres", Schema: "public"},
			revisionTable: "atlas_schema_revisions",
			want: migrateclean.Scope{
				Dialect:       "postgres",
				Schema:        "public",
				RevisionTable: "atlas_schema_revisions",
			},
		},
		{
			// --revisions-schema took the bookkeeping table out of the scope,
			// so there is nothing inside it left to exempt.
			name:            "schema scope drops a table kept elsewhere",
			scope:           migrateclean.Scope{Dialect: "postgres", Schema: "public"},
			revisionsSchema: "revs",
			revisionTable:   "atlas_schema_revisions",
			want:            migrateclean.Scope{Dialect: "postgres", Schema: "public"},
		},
		{
			// At realm scope an unset --revisions-schema is not "the connected
			// schema" but the binary's own bookkeeping schema.
			name:          "realm scope defaults the bookkeeping schema",
			scope:         migrateclean.Scope{Dialect: "postgres", Schema: "public", Realm: true},
			revisionTable: "atlas_schema_revisions",
			want: migrateclean.Scope{
				Dialect:         "postgres",
				Schema:          "public",
				Realm:           true,
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "atlas_schema_revisions",
			},
		},
		{
			name:            "realm scope follows --revisions-schema",
			scope:           migrateclean.Scope{Dialect: "postgres", Schema: "public", Realm: true},
			revisionsSchema: "revs",
			revisionTable:   "atlas_schema_revisions",
			want: migrateclean.Scope{
				Dialect:         "postgres",
				Schema:          "public",
				Realm:           true,
				RevisionTable:   "atlas_schema_revisions",
				RevisionsSchema: "revs",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := test.scope.ForRevisions(test.revisionsSchema, test.revisionTable)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
