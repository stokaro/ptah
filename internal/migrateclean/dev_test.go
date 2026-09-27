package migrateclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrateclean"
)

// The expected sentences below are the pinned community binary v1.3.0's,
// transcribed from runs against PostgreSQL 18, MySQL 8.4, MariaDB 11.8 and
// SQLite on 2026-09-26, with the dev database in the state each row names. The
// binary prints each after `sql/migrate: `, which belongs to the verb.

// TestScopeDevRefusal_HappyPath is the states the binary takes as a clean dev
// database.
func TestScopeDevRefusal_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		scope migrateclean.Scope
	}{
		{name: "postgres schema with no table", scope: migrateclean.Scope{Dialect: "postgres", Schema: "public"}},
		{name: "mysql database with no table", scope: migrateclean.Scope{Dialect: "mysql", Schema: "dev"}},
		{name: "mariadb database with no table", scope: migrateclean.Scope{Dialect: "mariadb", Schema: "dev"}},
		{name: "sqlite file with no table", scope: migrateclean.Scope{Dialect: "sqlite"}},
		{name: "postgres realm with no schema", scope: migrateclean.Scope{Dialect: "postgres", Realm: true}},
		{
			name: "postgres realm with an empty public alone",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "public"}},
			},
		},
		{
			// A connection with no dialect is not judged.
			name:  "no dialect",
			scope: migrateclean.Scope{Schema: "dev", Tables: []string{"t"}},
		},
		{
			name:  "cockroachdb schema with no table",
			scope: migrateclean.Scope{Dialect: "cockroachdb", Schema: "public"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.scope.DevRefusal(), qt.IsNil)
		})
	}
}

// TestScopeDevRefusal_FailurePath is the states the binary refuses, with the
// object it names.
func TestScopeDevRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		scope   migrateclean.Scope
		wantErr string
	}{
		{
			name:    "postgres schema names its first table",
			scope:   migrateclean.Scope{Dialect: "postgres", Schema: "public", Tables: []string{"Zed", "abc"}},
			wantErr: `connected database is not clean: found table "Zed" in connected schema`,
		},
		{
			name:    "mysql database names its first table and itself",
			scope:   migrateclean.Scope{Dialect: "mysql", Schema: "dev", Tables: []string{"aaa_t", "zzz_t"}},
			wantErr: `connected database is not clean: found table "aaa_t" in schema "dev"`,
		},
		{
			name:    "mariadb reads as mysql",
			scope:   migrateclean.Scope{Dialect: "mariadb", Schema: "dev", Tables: []string{"keep_me"}},
			wantErr: `connected database is not clean: found table "keep_me" in schema "dev"`,
		},
		{
			// Tables is in catalog order on SQLite: zzz_t was created first.
			name:    "sqlite names the first table it created",
			scope:   migrateclean.Scope{Dialect: "sqlite", Tables: []string{"zzz_t", "aaa_t"}},
			wantErr: `connected database is not clean: found table "zzz_t"`,
		},
		{
			name: "postgres realm with a table in public alone",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "public", Tables: []string{"aaa_t", "zzz_t"}}},
			},
			wantErr: `connected database is not clean: found table "aaa_t" in schema "public"`,
		},
		{
			name: "postgres realm with an empty schema beside public",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "abc"}, {Name: "public"}},
			},
			wantErr: `connected database is not clean: found schema "abc"`,
		},
		{
			// public sorts first and is named, empty as it is.
			name: "postgres realm names public when it sorts first",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "public"}, {Name: "zeta"}},
			},
			wantErr: `connected database is not clean: found schema "public"`,
		},
		{
			// Measured on CockroachDB 26.2 through a postgres:// URL.
			name:    "cockroachdb schema names its first table",
			scope:   migrateclean.Scope{Dialect: "cockroachdb", Schema: "public", Tables: []string{"keep_me"}},
			wantErr: `connected database is not clean: found table "keep_me" in connected schema`,
		},
		{
			name: "cockroachdb realm with a schema beside public",
			scope: migrateclean.Scope{
				Dialect: "cockroachdb", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "keep_schema"}, {Name: "public"}},
			},
			wantErr: `connected database is not clean: found schema "keep_schema"`,
		},
		{
			name:    "yugabytedb reads as postgres",
			scope:   migrateclean.Scope{Dialect: "yugabytedb", Schema: "public", Tables: []string{"keep_me"}},
			wantErr: `connected database is not clean: found table "keep_me" in connected schema`,
		},
		{
			// The binary opens none of the dialects below; the sentence takes
			// the shape of its others.
			name:    "sqlserver names the schema",
			scope:   migrateclean.Scope{Dialect: "sqlserver", Schema: "dbo", Tables: []string{"keep_me"}},
			wantErr: `connected database is not clean: found table "keep_me" in schema "dbo"`,
		},
		{
			name:    "clickhouse names the database",
			scope:   migrateclean.Scope{Dialect: "clickhouse", Schema: "dev", Tables: []string{"keep_me"}},
			wantErr: `connected database is not clean: found table "keep_me" in schema "dev"`,
		},
		{
			name:    "oracle names the user",
			scope:   migrateclean.Scope{Dialect: "oracle", Schema: "SYSTEM", Tables: []string{"KEEP_ME"}},
			wantErr: `connected database is not clean: found table "KEEP_ME" in schema "SYSTEM"`,
		},
		{
			name: "postgres realm with one schema that is not public",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "zeta"}},
			},
			wantErr: `connected database is not clean: found schema "zeta"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := test.scope.DevRefusal()
			var notClean *migrateclean.NotCleanError
			c.Assert(err, qt.ErrorAs, &notClean)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestGovernsDev_HappyPath is every dialect whose dev database is checked
// before it is reset.
func TestGovernsDev_HappyPath(t *testing.T) {
	for _, dialect := range []string{
		"postgres", "cockroachdb", "yugabytedb", "spanner",
		"mysql", "mariadb", "sqlite", "sqlserver", "clickhouse", "oracle",
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrateclean.GovernsDev(dialect), qt.IsTrue)
		})
	}
}

// TestGovernsDev_FailurePath is a dialect no dev database runs on.
func TestGovernsDev_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(migrateclean.GovernsDev(""), qt.IsFalse)
}

// TestScopeDevRefusal_FailurePathDroppedObject is Ptah's own refusal, past
// the binary's: an object the reset would drop that the binary does not count,
// measured against v1.3.0 on PostgreSQL 18.6 on 2026-09-27. With a search_path
// the binary keeps such an object, and with none it drops it; a dev database
// holding one is refused either way (stokaro/ptah#3808).
func TestScopeDevRefusal_FailurePathDroppedObject(t *testing.T) {
	tests := []struct {
		name    string
		scope   migrateclean.Scope
		wantErr string
	}{
		{
			name: "a view in the connected schema",
			scope: migrateclean.Scope{
				Dialect: "postgres", Schema: "public",
				Dropped: []migrateclean.DroppedObject{{Kind: "view", Schema: "public", Name: "v"}},
			},
			wantErr: `connected database is not clean: found view "v" in connected schema`,
		},
		{
			name: "the first object is named",
			scope: migrateclean.Scope{
				Dialect: "postgres", Schema: "app",
				Dropped: []migrateclean.DroppedObject{
					{Kind: "function", Schema: "app", Name: "a"},
					{Kind: "view", Schema: "app", Name: "b"},
				},
			},
			wantErr: `connected database is not clean: found function "a" in connected schema`,
		},
		{
			name: "a type in an otherwise empty public at realm scope",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "public"}},
				Dropped: []migrateclean.DroppedObject{{Kind: "type", Schema: "public", Name: "mood"}},
			},
			wantErr: `connected database is not clean: found type "mood" in schema "public"`,
		},
		{
			name: "a large object belongs to no schema",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Dropped: []migrateclean.DroppedObject{{Kind: "large object", Name: "16385"}},
			},
			wantErr: `connected database is not clean: found large object "16385"`,
		},
		{
			// The binary's sentence comes first: a table is named before
			// anything else the reset would drop.
			name: "a table beside a view is named as the binary names it",
			scope: migrateclean.Scope{
				Dialect: "postgres", Schema: "public", Tables: []string{"t"},
				Dropped: []migrateclean.DroppedObject{{Kind: "view", Schema: "public", Name: "a"}},
			},
			wantErr: `connected database is not clean: found table "t" in connected schema`,
		},
		{
			name: "a schema beside public is named as the binary names it",
			scope: migrateclean.Scope{
				Dialect: "postgres", Realm: true,
				Schemas: []migrateclean.RealmSchema{{Name: "extra"}, {Name: "public"}},
				Dropped: []migrateclean.DroppedObject{{Kind: "view", Schema: "public", Name: "a"}},
			},
			wantErr: `connected database is not clean: found schema "extra"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.scope.DevRefusal(), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestScopeDevRefusal_HappyPathDroppedObject is the control: the same object
// on a connection with no dialect is not judged, and a scope with nothing
// dropped is clean.
func TestScopeDevRefusal_HappyPathDroppedObject(t *testing.T) {
	tests := []struct {
		name  string
		scope migrateclean.Scope
	}{
		{
			name: "no dialect",
			scope: migrateclean.Scope{
				Schema:  "public",
				Dropped: []migrateclean.DroppedObject{{Kind: "view", Schema: "public", Name: "v"}},
			},
		},
		{
			name:  "nothing dropped",
			scope: migrateclean.Scope{Dialect: "postgres", Schema: "public", Dropped: make([]migrateclean.DroppedObject, 0)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.scope.DevRefusal(), qt.IsNil)
		})
	}
}
