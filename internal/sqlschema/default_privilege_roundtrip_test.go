package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/parser"
)

// TestDefaultPrivilege_RenderedStatementReadsBackAsTheSameDeclaration drives the
// whole loop a default privilege travels: declaration, rendered PostgreSQL, the
// SQL frontend, and the model again.
//
// The loop is the product's own path rather than a test contrivance. `ptah db
// read` renders the statement and a .sql schema source is read by this package,
// so a break anywhere in the loop means Ptah cannot read what Ptah just wrote --
// and the way that surfaces is a comparison reporting a change on every run
// against a database that already has the default.
//
// A comment is not part of the loop: PostgreSQL has no place to keep one on a
// default privilege, so the renderer emits it as a leading SQL comment and the
// frontend steps over it like any other. The rows leave it out rather than
// assert a loss.
func TestDefaultPrivilege_RenderedStatementReadsBackAsTheSameDeclaration(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.DefaultPrivilege
		want     schemamodel.DefaultPrivilege
	}{
		{
			name: "one privilege",
			declared: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
			want: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
		},
		{
			// The statement the renderer splits. Grantability is per privilege,
			// so a declaration mixing the two renders as two statements, and
			// the read has to fold them back into one object -- two objects
			// here would be compared against the one merged catalog row
			// forever. The plain privileges come back first because that is the
			// order the two statements are written in.
			name: "mixed grantability folds back into one object",
			declared: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "SELECT"},
					{Privilege: "UPDATE", WithOption: true},
				},
			},
			want: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []schemamodel.PrivilegeGrant{
					{Privilege: "SELECT"},
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "UPDATE", WithOption: true},
				},
			},
		},
		{
			// PUBLIC is a keyword rather than a role name, so the renderer
			// leaves it unquoted and the read must not turn it into a role
			// nobody created.
			name: "granted to public",
			declared: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TYPES",
				Grantee:    "PUBLIC",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
			want: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TYPES",
				Grantee:    "PUBLIC",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
		},
		{
			// Every identity component renders quoted here. Read back with the
			// quotes still on, the declaration would match no catalog row and
			// the statement would be re-issued on every run.
			name: "an identity that has to be quoted",
			declared: schemamodel.DefaultPrivilege{
				Grantor:    "App Owner",
				Schema:     "App Schema",
				ObjectType: "SEQUENCES",
				Grantee:    "App Reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
			want: schemamodel.DefaultPrivilege{
				Grantor:    "App Owner",
				Schema:     "App Schema",
				ObjectType: "SEQUENCES",
				Grantee:    "App Reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
		},
		{
			// Canonicalize owns the spellings, and the loop has to agree with
			// it at both ends: the render upper-cases what the author wrote and
			// the read reports what PostgreSQL would.
			name: "a lower-case declaration",
			declared: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "functions",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "execute"}},
			},
			want: schemamodel.DefaultPrivilege{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "FUNCTIONS",
				Grantee:    "app_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "EXECUTE"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := renderer.RenderSQL("postgres", modelast.FromDefaultPrivilege(test.declared))
			c.Assert(err, qt.IsNil)

			read := parseToDatabase(c, rendered)
			schemamodel.Finalize(&read)

			c.Assert(read.DefaultPrivileges, qt.DeepEquals, []schemamodel.DefaultPrivilege{test.want})
		})
	}
}

// TestDefaultPrivilege_ReadKeepsTheObjectsDeclaredBesideIt is the control for
// the switch in ToDatabase, which fails closed: a node kind with no case there
// returns ErrUnmodeledStatement and ToDatabase discards everything it built.
//
// So the assertion that matters is not only that the default privilege arrives.
// It is that the table declared beside it arrives too -- the shape a missing
// case takes is a whole schema read that fails, in a package nobody editing the
// parser would think to look at.
func TestDefaultPrivilege_ReadKeepsTheObjectsDeclaredBesideIt(t *testing.T) {
	c := qt.New(t)

	database := parseToDatabase(c, `CREATE TABLE app.orders (id BIGINT PRIMARY KEY);
ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;`)

	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "orders")
	c.Assert(database.DefaultPrivileges, qt.DeepEquals, []schemamodel.DefaultPrivilege{{
		Grantor:    "app_owner",
		Schema:     "app",
		ObjectType: "TABLES",
		Grantee:    "app_reader",
		Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
	}})
}

// TestDefaultPrivilege_ClusterWideFormIsRefused pins where a .sql source loses
// the statement the model cannot hold.
//
// A default with no IN SCHEMA applies in every schema in the database, which no
// field here records. Refusing it in the parser puts the failure in front of the
// author; reading it would put an unrenderable declaration into a desired schema
// and lose it further down.
func TestDefaultPrivilege_ClusterWideFormIsRefused(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner GRANT SELECT ON TABLES TO app_reader;").Parse()

	c.Assert(err, qt.ErrorMatches, `ALTER DEFAULT PRIVILEGES requires IN SCHEMA: .*`)
	c.Assert(statements, qt.IsNil)
}

// TestDefaultPrivilege_SchemaScopedFormReachesTheModel is the control for the
// refusal above: the same statement carrying the clause the model needs. Without
// it, a grammar that refused every ALTER DEFAULT PRIVILEGES would pass.
func TestDefaultPrivilege_SchemaScopedFormReachesTheModel(t *testing.T) {
	c := qt.New(t)

	database := parseToDatabase(c,
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;")

	c.Assert(database.DefaultPrivileges, qt.HasLen, 1)
	c.Assert(database.DefaultPrivileges[0].Schema, qt.Equals, "app")
}
