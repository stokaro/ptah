package schemafile_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

// These tests cover how a SQL schema file names a role (stokaro/ptah#3574).
// PostgreSQL folds an unquoted name to lower case, so `TO App_A` names the role
// `app_a`. Kept as written, the name was rendered quoted and named a role
// nobody created.

// roleCaseSchema declares a role, a grant and a revoke, a default privilege, a
// policy and a role comment, each naming the role the way the argument spells
// it.
func roleCaseSchema(role string) string {
	return fmt.Sprintf(`CREATE ROLE %[1]s;
COMMENT ON ROLE %[1]s IS 'application';
CREATE TABLE docs (id integer PRIMARY KEY);
GRANT SELECT, INSERT ON docs TO %[1]s;
REVOKE INSERT ON docs FROM %[1]s;
ALTER DEFAULT PRIVILEGES FOR ROLE %[1]s IN SCHEMA public GRANT SELECT ON TABLES TO %[1]s;
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON docs TO %[1]s, PUBLIC USING (true);
`, role)
}

func loadRoleCaseSchema(c *qt.C, dialect, body string) *schemamodel.Database {
	c.Helper()
	path := writeSchemaFile(c, c.TempDir(), "schema.sql", body)
	db, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)
	return db
}

// roleNames is every place the model records the role, in a fixed order, so a
// test sees at once whether the file names one role or several. Granted is
// what is left after the REVOKE: SELECT alone only when the REVOKE names the
// role the GRANT named.
type roleNames struct {
	Role, Comment, Grant, Granted, DefaultGrantor, DefaultGrantee, Policy string
}

func recordedRoleNames(c *qt.C, db *schemamodel.Database) roleNames {
	c.Helper()
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.DefaultPrivileges, qt.HasLen, 1)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	return roleNames{
		Role:           db.Roles[0].Name,
		Comment:        db.Roles[0].Comment,
		Grant:          db.Grants[0].Role,
		Granted:        strings.Join(db.Grants[0].Privileges, ", "),
		DefaultGrantor: db.DefaultPrivileges[0].Grantor,
		DefaultGrantee: db.DefaultPrivileges[0].Grantee,
		Policy:         db.RLSPolicies[0].ToRoles,
	}
}

// TestLoadAll_PostgresRoleNamesFoldLikeTheServer reads the same role in every
// position a schema file names one. Each position lands on the name the server
// resolves, so the file names one role wherever it mentions it. The expected
// names are what each engine's pg_roles reported for the same CREATE ROLE:
// PostgreSQL 18.6 and YugabyteDB 2026.1 lower unquoted ASCII letters only,
// CockroachDB 26.3.2 lowers every letter of every role name.
func TestLoadAll_PostgresRoleNamesFoldLikeTheServer(t *testing.T) {
	tests := []struct {
		dialect string
		name    string
		role    string
		want    string
	}{
		{dialect: platform.Postgres, name: "unquoted mixed case folds", role: "App_A", want: "app_a"},
		{dialect: platform.Postgres, name: "quoted keeps its case", role: `"App_A"`, want: "App_A"},
		{dialect: platform.Postgres, name: "lower case is unchanged", role: "app_a", want: "app_a"},
		{dialect: platform.Postgres, name: "unquoted folds ASCII only", role: "Ärger_A", want: "Ärger_a"},
		{dialect: platform.YugabyteDB, name: "unquoted mixed case folds", role: "App_A", want: "app_a"},
		{dialect: platform.YugabyteDB, name: "quoted keeps its case", role: `"App_A"`, want: "App_A"},
		{dialect: platform.YugabyteDB, name: "unquoted folds ASCII only", role: "Ärger_A", want: "Ärger_a"},
		{dialect: platform.CockroachDB, name: "unquoted mixed case folds", role: "App_A", want: "app_a"},
		{dialect: platform.CockroachDB, name: "quoted folds too", role: `"App_A"`, want: "app_a"},
		{dialect: platform.CockroachDB, name: "non-ASCII folds too", role: `"Ärger_A"`, want: "ärger_a"},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			db := loadRoleCaseSchema(c, test.dialect, roleCaseSchema(test.role))
			c.Assert(recordedRoleNames(c, db), qt.DeepEquals, roleNames{
				Role: test.want, Comment: "application", Grant: test.want, Granted: "SELECT",
				DefaultGrantor: test.want, DefaultGrantee: test.want, Policy: test.want + ", PUBLIC",
			})
		})
	}
}

// TestLoadAll_PostgresQuotedPublicGrantee reads a grantee spelled "Public".
// PostgreSQL keeps the quoted case, so it names a role; CockroachDB lowers it
// to the PUBLIC keyword, and GRANT ... TO "PUBLIC" there wrote the ACL entry
// for every role.
func TestLoadAll_PostgresQuotedPublicGrantee(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: platform.Postgres, want: "Public"},
		{dialect: platform.YugabyteDB, want: "Public"},
		{dialect: platform.CockroachDB, want: "PUBLIC"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			db := loadRoleCaseSchema(c, test.dialect, `CREATE TABLE docs (id integer PRIMARY KEY);
GRANT SELECT ON docs TO "Public";
`)
			c.Assert(db.Grants, qt.HasLen, 1)
			c.Assert(db.Grants[0].Role, qt.Equals, test.want)
		})
	}
}

// TestLoadAll_PostgresRoleRendersTheFoldedName is the rendering half: the
// statements the file becomes name the folded role, so they reach the role a
// server holds for the unquoted name.
func TestLoadAll_PostgresRoleRendersTheFoldedName(t *testing.T) {
	c := qt.New(t)
	db := loadRoleCaseSchema(c, platform.Postgres, roleCaseSchema("App_A"))

	rendered := strings.Join(renderPostgres(c, db), "\n")

	c.Assert(rendered, qt.Contains, `CREATE POLICY "p" ON "docs" TO "app_a", PUBLIC`)
	c.Assert(rendered, qt.Contains, `GRANT SELECT ON TABLE "docs" TO "app_a";`)
	c.Assert(rendered, qt.Not(qt.Contains), `"App_A"`)
}

// TestLoadAll_DialectNeutralRoleNamesKeepTheirCase is the control: without a
// PostgreSQL-family dialect the reader cannot say how a server folds, so a name
// stays as written.
func TestLoadAll_DialectNeutralRoleNamesKeepTheirCase(t *testing.T) {
	c := qt.New(t)
	db := loadRoleCaseSchema(c, "", roleCaseSchema("App_A"))
	c.Assert(recordedRoleNames(c, db), qt.DeepEquals, roleNames{
		Role: "App_A", Comment: "application", Grant: "App_A", Granted: "SELECT",
		DefaultGrantor: "App_A", DefaultGrantee: "App_A", Policy: "App_A, PUBLIC",
	})
}
