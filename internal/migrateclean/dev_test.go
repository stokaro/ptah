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
			// A dialect the binary was not measured on is not judged.
			name:  "clickhouse with a table",
			scope: migrateclean.Scope{Dialect: "clickhouse", Schema: "dev", Tables: []string{"t"}},
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
