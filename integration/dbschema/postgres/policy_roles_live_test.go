//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// These tests apply a SQL schema file whose policy names its roles one way,
// read the database back, and compare the file with what the server holds
// (stokaro/ptah#3572). Each engine of the PostgreSQL family applies a policy
// with no TO clause to PUBLIC and reports it that way, so an omitted clause and
// the catalog's PUBLIC have to compare equal, and the order and separator of a
// role list must not matter. A different role has to stay a change.

var policyRoleEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// policyRoleFixture is one throwaway schema and two roles on one engine.
type policyRoleFixture struct {
	conn    *dbschema.DatabaseConnection
	dialect string
	schema  string
	roleA   string
	roleB   string
}

func newPolicyRoleFixture(c *qt.C, engine dbtarget.Engine) policyRoleFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano()
	f := policyRoleFixture{
		conn:    conn,
		dialect: conn.Info().Dialect,
		schema:  fmt.Sprintf("ptah_policy_roles_%d", suffix),
		roleA:   fmt.Sprintf("ptah_policy_a_%d", suffix),
		roleB:   fmt.Sprintf("ptah_policy_b_%d", suffix),
	}
	c.Cleanup(func() {
		cleanup := context.Background()
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + pgx.Identifier{f.schema}.Sanitize() + " CASCADE",
			"DROP ROLE IF EXISTS " + pgx.Identifier{f.roleA}.Sanitize(),
			"DROP ROLE IF EXISTS " + pgx.Identifier{f.roleB}.Sanitize(),
		} {
			_, dropErr := conn.ExecContext(cleanup, statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
		c.Check(conn.Close(), qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + pgx.Identifier{f.schema}.Sanitize(),
		"CREATE ROLE " + pgx.Identifier{f.roleA}.Sanitize(),
		"CREATE ROLE " + pgx.Identifier{f.roleB}.Sanitize(),
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return f
}

// load reads a schema file declaring one table with one policy, whose TO clause
// is the argument. ROLE_A and ROLE_B stand for the fixture's roles. The policy
// admits every row: each engine prints `true` back as written, so the only
// clause left for the comparison to disagree on is TO. A predicate over a
// column is printed differently by each engine, and this comparison runs
// without a server to normalize the declared one.
func (f policyRoleFixture) load(c *qt.C, toClause string) *schemamodel.Database {
	c.Helper()
	toClause = strings.NewReplacer("ROLE_A", f.roleA, "ROLE_B", f.roleB).Replace(toClause)
	body := fmt.Sprintf(`CREATE TABLE %[1]s.docs (id integer PRIMARY KEY, tenant text NOT NULL);
ALTER TABLE %[1]s.docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY docs_tenant ON %[1]s.docs %[2]s USING (true);
`, f.schema, toClause)
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	db, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: f.dialect})
	c.Assert(err, qt.IsNil)
	return db
}

func (f policyRoleFixture) apply(c *qt.C, desired *schemamodel.Database) {
	c.Helper()
	statements, err := renderer.GetOrderedCreateStatements(desired, f.dialect)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

func (f policyRoleFixture) read(c *qt.C) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return live
}

func (f policyRoleFixture) compare(c *qt.C, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	return schemadiff.CompareWithDialect(desired, f.read(c), f.dialect)
}

// TestPolicyRoles_LiveSchemaFileConverges applies each spelling and compares
// the same file back. Nothing is left to plan for the policy.
func TestPolicyRoles_LiveSchemaFileConverges(t *testing.T) {
	spellings := []struct {
		name     string
		toClause string
	}{
		{name: "omitted", toClause: ""},
		{name: "PUBLIC", toClause: "TO PUBLIC"},
		{name: "public in lower case", toClause: "TO public"},
		{name: "one role", toClause: "TO ROLE_A"},
		{name: "two roles", toClause: "TO ROLE_A, ROLE_B"},
		{name: "two roles in the other order", toClause: "TO ROLE_B, ROLE_A"},
		{name: "PUBLIC beside a role", toClause: "TO PUBLIC, ROLE_A"},
	}
	for _, engine := range policyRoleEngines {
		for _, spelling := range spellings {
			t.Run(engine.name+"/"+spelling.name, func(t *testing.T) {
				c := qt.New(t)
				f := newPolicyRoleFixture(c, engine.engine)
				desired := f.load(c, spelling.toClause)
				f.apply(c, desired)

				diff := f.compare(c, desired)

				c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
				c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
				c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.RLSPoliciesModified))
			})
		}
	}
}

// TestPolicyRoles_LiveADifferentRolePlansAChange is the control for the test
// above: the fold must not make a real difference disappear. A policy the
// server applies to PUBLIC and a declaration naming one role differ, in both
// directions.
func TestPolicyRoles_LiveADifferentRolePlansAChange(t *testing.T) {
	directions := []struct {
		name     string
		applied  string
		declared string
	}{
		{name: "applied without TO, declared with a role", applied: "", declared: "TO ROLE_A"},
		{name: "applied with a role, declared without TO", applied: "TO ROLE_A", declared: ""},
		{name: "applied with one role, declared with two", applied: "TO ROLE_A", declared: "TO ROLE_A, ROLE_B"},
	}
	for _, engine := range policyRoleEngines {
		for _, direction := range directions {
			t.Run(engine.name+"/"+direction.name, func(t *testing.T) {
				c := qt.New(t)
				f := newPolicyRoleFixture(c, engine.engine)
				f.apply(c, f.load(c, direction.applied))

				diff := f.compare(c, f.load(c, direction.declared))

				c.Assert(diff.RLSPoliciesModified, qt.HasLen, 1)
				_, rolesChanged := diff.RLSPoliciesModified[0].Changes["to_roles"]
				c.Assert(rolesChanged, qt.IsTrue, qt.Commentf("%+v", diff.RLSPoliciesModified[0].Changes))
			})
		}
	}
}

// TestPolicyRoles_LiveReaderReportsPUBLICBesideARole reads a policy written
// `TO PUBLIC, role` back as PUBLIC. PostgreSQL and YugabyteDB store only PUBLIC
// for it; CockroachDB keeps both entries, and a join over pg_roles, which has no
// row for PUBLIC, read that one back as the role alone -- a narrower grant than
// the one in force.
func TestPolicyRoles_LiveReaderReportsPUBLICBesideARole(t *testing.T) {
	for _, engine := range policyRoleEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPolicyRoleFixture(c, engine.engine)
			f.apply(c, f.load(c, "TO PUBLIC, ROLE_A"))

			live := f.read(c)

			c.Assert(live.RLSPolicies, qt.HasLen, 1)
			c.Assert(live.RLSPolicies[0].ToRoles, qt.Equals, "PUBLIC")
		})
	}
}
