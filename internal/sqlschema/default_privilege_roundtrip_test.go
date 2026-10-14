package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
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
		declared defaultPrivilegeDeclaration
		want     defaultPrivilegeDeclaration
	}{
		{
			name: "one privilege",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
			},
		},
		{
			// The statement the renderer splits. Grantability is per privilege,
			// so a declaration mixing the two renders as two statements, and
			// the read has to fold them back into one object -- two objects
			// here would be compared against the one merged catalog row
			// forever. The owner returns privileges in canonical order.
			name: "mixed grantability folds back into one object",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []pgdefaultprivilege.Grant{
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "SELECT"},
					{Privilege: "UPDATE", WithOption: true},
				},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TABLES",
				Grantee:    "app_writer",
				Privileges: []pgdefaultprivilege.Grant{
					{Privilege: "INSERT", WithOption: true},
					{Privilege: "SELECT"},
					{Privilege: "UPDATE", WithOption: true},
				},
			},
		},
		{
			// PUBLIC is a keyword rather than a role name, so the renderer
			// leaves it unquoted and the read must not turn it into a role
			// nobody created.
			name: "granted to public",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TYPES",
				Grantee:    "PUBLIC",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "TYPES",
				Grantee:    "PUBLIC",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
			},
		},
		{
			// Every identity component renders quoted here. Read back with the
			// quotes still on, the declaration would match no catalog row and
			// the statement would be re-issued on every run.
			name: "an identity that has to be quoted",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "App Owner",
				Schema:     "App Schema",
				ObjectType: "SEQUENCES",
				Grantee:    "App Reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "App Owner",
				Schema:     "App Schema",
				ObjectType: "SEQUENCES",
				Grantee:    "App Reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
			},
		},
		{
			// No schema is the global default, and the statement leaves IN
			// SCHEMA out rather than writing an empty one.
			name: "the global default",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				ObjectType: "SCHEMAS",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE", WithOption: true}},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				ObjectType: "SCHEMAS",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE", WithOption: true}},
			},
		},
		{
			// Canonicalize owns the spellings, and the loop has to agree with
			// it at both ends: the render upper-cases what the author wrote and
			// the read reports what PostgreSQL would.
			name: "a lower-case declaration",
			declared: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "functions",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "execute"}},
			},
			want: defaultPrivilegeDeclaration{
				Grantor:    "app_owner",
				Schema:     "app",
				ObjectType: "FUNCTIONS",
				Grantee:    "app_reader",
				Privileges: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			identity := pgdefaultprivilege.Identity{Grantor: test.declared.Grantor, Schema: test.declared.Schema, ObjectType: test.declared.ObjectType, Grantee: test.declared.Grantee}
			ref := must.Must(pgdefaultprivilege.Ref(identity, identifier.ForDialect("postgres")))
			object := must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: test.declared.Privileges, Revoked: test.declared.Revoked}))
			rendered, err := builtin.GetOrderedCreateStatements(&schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object))}, "postgres")
			c.Assert(err, qt.IsNil)

			read := readDefaultPrivilegeSQL(c, strings.Join(rendered, "\n"))
			schemamodel.Finalize(&read)

			c.Assert(recordedDefaultPrivileges(c, read), qt.DeepEquals, []defaultPrivilegeDeclaration{test.want})
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

	database := readDefaultPrivilegeSQL(c, `CREATE TABLE app.orders (id BIGINT PRIMARY KEY);
ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;`)

	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "orders")
	c.Assert(recordedDefaultPrivileges(c, database), qt.ContentEquals, []defaultPrivilegeDeclaration{{
		Grantor:    "app_owner",
		Schema:     "app",
		ObjectType: "TABLES",
		Grantee:    "app_reader",
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	}})
}

// TestDefaultPrivilege_GlobalFormReachesTheModel reads the statement without
// IN SCHEMA, the global default, as a declaration with no schema. A revoke of a
// built-in privilege is kept as Revoked, which is what a new database needs to
// be told (stokaro/ptah#3772).
func TestDefaultPrivilege_GlobalFormReachesTheModel(t *testing.T) {
	c := qt.New(t)

	database := readDefaultPrivilegeSQL(c, `ALTER DEFAULT PRIVILEGES FOR ROLE app_owner GRANT SELECT ON TABLES TO app_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE app_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`)
	schemamodel.Finalize(&database)

	c.Assert(recordedDefaultPrivileges(c, database), qt.ContentEquals, []defaultPrivilegeDeclaration{
		{
			Grantor: "app_owner", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
		},
		{
			Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC",
			Privileges: nil, Revoked: []string{"EXECUTE"},
		},
	})
}

// TestDefaultPrivilege_SchemaScopedFormReachesTheModel is the control for the
// global form: the same statement with IN SCHEMA keeps its schema, so a reader
// that dropped the clause would fail here.
func TestDefaultPrivilege_SchemaScopedFormReachesTheModel(t *testing.T) {
	c := qt.New(t)

	database := readDefaultPrivilegeSQL(c,
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;")

	objects := recordedDefaultPrivileges(c, database)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Schema, qt.Equals, "app")
}

// defaultPrivilegeDeclaration is the complete identity and ACL the SQL reader
// exposes through the owner boundary.
type defaultPrivilegeDeclaration struct {
	Grantor, Schema, ObjectType, Grantee string
	Privileges                           []pgdefaultprivilege.Grant
	Revoked                              []string
}

func readDefaultPrivilegeSQL(c *qt.C, text string) schemamodel.Database {
	c.Helper()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	return database
}

func recordedDefaultPrivileges(c *qt.C, database schemamodel.Database) []defaultPrivilegeDeclaration {
	c.Helper()
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	var result []defaultPrivilegeDeclaration
	for _, object := range objects {
		identity, err := pgdefaultprivilege.IdentityOf(object.Ref)
		c.Assert(err, qt.IsNil)
		value, ok := object.Value.(*pgdefaultprivilege.Desired)
		c.Assert(ok, qt.IsTrue)
		result = append(result, defaultPrivilegeDeclaration{Grantor: identity.Grantor, Schema: identity.Schema, ObjectType: identity.ObjectType, Grantee: identity.Grantee, Privileges: value.Privileges, Revoked: value.Revoked})
	}
	return result
}
