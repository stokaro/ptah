//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// These tests apply a schema file that names an existing role, and compare it
// back (stokaro/ptah#3574).
//
// The role is created the way a person or a migration creates it, so the server
// holds it under the name it folds to. A file naming it unquoted in mixed case
// has to reach that role: kept as written, the name was rendered quoted and the
// server refused the statement with `role "..." does not exist`. A quoted name
// is the control, because the server keeps its case and so must the file.

var roleCaseSpellings = []struct {
	name string
	// quote wraps the role name the same way in the CREATE ROLE and in the
	// schema file.
	quote string
}{
	{name: "unquoted mixed case", quote: ""},
	{name: "quoted mixed case", quote: `"`},
}

// applyRoleCaseSchema creates a throwaway schema and a role spelled with quote,
// applies the schema file the body template describes, and compares the file
// with what the server read back. The template's %[1]s is the schema and %[2]s
// the role as spelled.
func applyRoleCaseSchema(c *qt.C, engine dbtarget.Engine, quote, template string) *difftypes.SchemaDiff {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	schema := fmt.Sprintf("ptah_role_case_%d", suffix)
	role := fmt.Sprintf("%sPtah_Case_%d%s", quote, suffix, quote)
	c.Cleanup(func() {
		cleanup := context.Background()
		_, dropSchema := conn.ExecContext(cleanup, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		c.Check(dropSchema, qt.IsNil)
		_, dropRole := conn.ExecContext(cleanup, "DROP ROLE IF EXISTS "+role)
		c.Check(dropRole, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + pgx.Identifier{schema}.Sanitize(),
		"CREATE ROLE " + role,
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(template, schema, role)), 0o600), qt.IsNil)
	dialect := conn.Info().Dialect
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(desired, dialect)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schema})
	c.Assert(err, qt.IsNil)
	return schemadiff.CompareWithDialect(desired, live, dialect)
}

// TestRoleCase_LivePolicyNamesTheRoleTheServerHolds applies a policy whose TO
// clause names the role.
func TestRoleCase_LivePolicyNamesTheRoleTheServerHolds(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}
	for _, engine := range engines {
		for _, spelling := range roleCaseSpellings {
			t.Run(engine.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				diff := applyRoleCaseSchema(c, engine.engine, spelling.quote, `CREATE TABLE %[1]s.docs (id integer PRIMARY KEY);
ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_role ON %[1]s.docs TO %[2]s USING (true);
`)

				c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
				c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.RLSPoliciesModified))
			})
		}
	}
}

// TestRoleCase_LiveGrantNamesTheRoleTheServerHolds applies a table grant to the
// role. CockroachDB is absent: reading a table grant back fails there on a
// grantor the engine does not report (stokaro/ptah#3589).
func TestRoleCase_LiveGrantNamesTheRoleTheServerHolds(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}
	for _, engine := range engines {
		for _, spelling := range roleCaseSpellings {
			t.Run(engine.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				diff := applyRoleCaseSchema(c, engine.engine, spelling.quote, `CREATE TABLE %[1]s.docs (id integer PRIMARY KEY);
GRANT SELECT ON %[1]s.docs TO %[2]s;
`)

				c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.GrantsAdded))
				c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.GrantsRemoved))
			})
		}
	}
}
