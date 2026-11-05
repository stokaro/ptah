package pgschemastate_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/pgschemastate"
)

// claimedPublic is public as PostgreSQL 15 and later create it: owned by
// pg_database_owner, which holds both privileges, with USAGE for PUBLIC and
// the comment every database starts with.
var claimedPublic = dbreset.SchemaState{
	Name:  "public",
	Owner: "pg_database_owner",
	Privileges: []dbreset.SchemaPrivilege{
		{Grantee: "PUBLIC", Privilege: "USAGE"},
		{Grantee: "pg_database_owner", Privilege: "CREATE"},
		{Grantee: "pg_database_owner", Privilege: "USAGE"},
	},
	Comment: "standard public schema",
}

// TestCreate_GivesTheSchemaItsState pins the statements that create a schema
// again with what its claim recorded.
func TestCreate_GivesTheSchemaItsState(t *testing.T) {
	tests := []struct {
		name  string
		state dbreset.SchemaState
		want  []string
	}{
		{
			name:  "the default access list and no comment",
			state: dbreset.SchemaState{Name: "app", Owner: "app_owner", DefaultAccess: true},
			want: []string{
				`CREATE SCHEMA "app" AUTHORIZATION "app_owner"`,
				`COMMENT ON SCHEMA "app" IS NULL`,
			},
		},
		{
			name:  "an access list of its own and a comment",
			state: claimedPublic,
			want: []string{
				`CREATE SCHEMA "public" AUTHORIZATION "pg_database_owner"`,
				`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM PUBLIC`,
				`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "pg_database_owner"`,
				`GRANT USAGE ON SCHEMA "public" TO PUBLIC`,
				`GRANT CREATE ON SCHEMA "public" TO "pg_database_owner"`,
				`GRANT USAGE ON SCHEMA "public" TO "pg_database_owner"`,
				`COMMENT ON SCHEMA "public" IS 'standard public schema'`,
			},
		},
		{
			name: "a grant option and a quote in the comment",
			state: dbreset.SchemaState{
				Name: "app", Owner: "app_owner",
				Privileges: []dbreset.SchemaPrivilege{{Grantee: "reader", Privilege: "USAGE", GrantOption: true}},
				Comment:    "it's the app",
			},
			want: []string{
				`CREATE SCHEMA "app" AUTHORIZATION "app_owner"`,
				`REVOKE ALL PRIVILEGES ON SCHEMA "app" FROM PUBLIC`,
				`REVOKE ALL PRIVILEGES ON SCHEMA "app" FROM "app_owner"`,
				`REVOKE ALL PRIVILEGES ON SCHEMA "app" FROM "reader"`,
				`GRANT USAGE ON SCHEMA "app" TO "reader" WITH GRANT OPTION`,
				`COMMENT ON SCHEMA "app" IS 'it''s the app'`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgschemastate.Create(test.state), qt.DeepEquals, test.want)
		})
	}
}

// TestReturn_GivesTheSchemaBackItsClaimedState pins the statements that
// return a schema a reset emptied in place to its claimed state, and that a
// schema which already holds it gets none.
func TestReturn_GivesTheSchemaBackItsClaimedState(t *testing.T) {
	revoked := claimedPublic
	revoked.Privileges = claimedPublic.Privileges[1:]
	commented := claimedPublic
	commented.Comment = "mine"
	granted := claimedPublic
	granted.Privileges = append([]dbreset.SchemaPrivilege{{Grantee: "app", Privilege: "CREATE"}}, claimedPublic.Privileges...)
	owned := claimedPublic
	owned.Owner = "app"
	owned.Privileges = []dbreset.SchemaPrivilege{
		{Grantee: "PUBLIC", Privilege: "USAGE"},
		{Grantee: "app", Privilege: "CREATE"},
		{Grantee: "app", Privilege: "USAGE"},
	}
	defaultApp := dbreset.SchemaState{Name: "app", Owner: "app_owner", DefaultAccess: true}
	spelledApp := dbreset.SchemaState{Name: "app", Owner: "app_owner", Privileges: []dbreset.SchemaPrivilege{
		{Grantee: "app_owner", Privilege: "USAGE"},
		{Grantee: "app_owner", Privilege: "CREATE"},
	}}
	accessStatements := []string{
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM PUBLIC`,
		`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "pg_database_owner"`,
	}
	grants := []string{
		`GRANT USAGE ON SCHEMA "public" TO PUBLIC`,
		`GRANT CREATE ON SCHEMA "public" TO "pg_database_owner"`,
		`GRANT USAGE ON SCHEMA "public" TO "pg_database_owner"`,
	}
	tests := []struct {
		name    string
		claim   dbreset.SchemaState
		current dbreset.SchemaState
		want    []string
	}{
		{name: "a schema that holds its state", claim: claimedPublic, current: claimedPublic},
		{name: "the default access list spelled out", claim: defaultApp, current: spelledApp},
		{
			name:    "a revoked privilege",
			claim:   claimedPublic,
			current: revoked,
			want:    slices.Concat(accessStatements, grants),
		},
		{
			name:    "a privilege granted to another role",
			claim:   claimedPublic,
			current: granted,
			want: slices.Concat(accessStatements,
				[]string{`REVOKE ALL PRIVILEGES ON SCHEMA "public" FROM "app"`}, grants),
		},
		{
			name:    "a changed comment",
			claim:   claimedPublic,
			current: commented,
			want:    []string{`COMMENT ON SCHEMA "public" IS 'standard public schema'`},
		},
		{
			name:    "a changed owner, whose privileges moved with the schema",
			claim:   claimedPublic,
			current: owned,
			want:    []string{`ALTER SCHEMA "public" OWNER TO "pg_database_owner"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgschemastate.Return(test.claim, test.current), qt.DeepEquals, test.want)
		})
	}
}
