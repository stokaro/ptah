//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/jackc/pgx/v5"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// The catalog, planner and source owners must agree on type dependencies and
// reference spellings. Each source converges on the live definition, and the
// reverse plan removes every type it creates, including generated array types.
func TestRangeSourcesConvergeLive(t *testing.T) {
	for _, test := range []struct{ name, body, setup string }{
		{name: "catalog options", body: `CREATE TYPE %s.span AS RANGE (SUBTYPE = numeric, SUBTYPE_OPCLASS = pg_catalog.numeric_ops, SUBTYPE_DIFF = pg_catalog.numrange_subdiff); COMMENT ON TYPE %s.span IS 'numeric span';`},
		{name: "domain over range", body: `CREATE TYPE %s.span AS RANGE (SUBTYPE = numeric); CREATE DOMAIN %s.wrapped AS %s.span; CREATE TABLE %s.items (id integer PRIMARY KEY, amount %s.wrapped);`},
		{name: "range over domain", body: `CREATE DOMAIN %s.quantity AS numeric; CREATE TYPE %s.span AS RANGE (SUBTYPE = %s.quantity);`},
		{name: "mixed dependency chain", body: `CREATE DOMAIN %s.base AS numeric; CREATE TYPE %s.z_inner AS RANGE (SUBTYPE = %s.base); CREATE DOMAIN %s.wrapper AS %s.z_inner; CREATE TYPE %s.a_outer AS RANGE (SUBTYPE = %s.wrapper); CREATE TYPE %s.box AS (value %s.a_outer);`},
		{name: "quoted range name", body: `CREATE TYPE %s." span "" " AS RANGE (SUBTYPE = numeric); COMMENT ON TYPE %s." span "" " IS 'quoted span';`},
		{name: "quoted subtype array", body: `CREATE DOMAIN %s."Quantity" AS numeric; CREATE TYPE %s.span AS RANGE (SUBTYPE = %s."Quantity"[]);`},
		{name: "catalog collation", body: `CREATE TYPE %s.span AS RANGE (SUBTYPE = text, COLLATION = "C");`},
		{name: "custom collation", setup: `CREATE COLLATION %s." Custom " FROM pg_catalog."C";`, body: `CREATE TYPE %s.span AS RANGE (SUBTYPE = text, COLLATION = %s." Custom ");`},
		{name: "custom operator class", setup: `CREATE OPERATOR CLASS %s." Numeric " FOR TYPE numeric USING btree AS OPERATOR 1 < (numeric, numeric), OPERATOR 2 <= (numeric, numeric), OPERATOR 3 = (numeric, numeric), OPERATOR 4 >= (numeric, numeric), OPERATOR 5 > (numeric, numeric), FUNCTION 1 numeric_cmp(numeric,numeric);`, body: `CREATE TYPE %s.span AS RANGE (SUBTYPE = numeric, SUBTYPE_OPCLASS = %s." Numeric ");`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTypeSourceFixture(c)
			f.apply(c, f.qualify(test.setup))
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE SCHEMA "+f.quotedSchema()+"; "+f.qualify(test.body)), "postgres", nil, sqlschema.Options{Owners: f.runtime.SQL()})
			c.Assert(err, qt.IsNil)
			before := must.Must(dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema}))
			diff, err := schemadiff.CompareWithDatabase(c.Context(), f.conn, &desired, before, nil, f.runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: f.runtime, Diff: diff, DesiredSchema: &desired, CurrentSchema: before, Dialect: "postgres", Capabilities: f.conn.Info().Capabilities})
			c.Assert(err, qt.IsNil)
			f.apply(c, must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Forward.Nodes...)))
			f.assertConverged(c, &desired)
			f.assertExportsConverged(c)
			f.apply(c, must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Reverse.Nodes...)))
			var remaining int
			err = f.conn.QueryRowContext(c.Context(), `SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=$1`, f.schema).Scan(&remaining)
			c.Assert(err, qt.IsNil)
			c.Assert(remaining, qt.Equals, 0)
		})
	}
}

// A replacement must cross owner boundaries in both directions: range to
// domain/composite, and domain to range. Rollback restores the initial schema.
func TestRangeRebuildDependenciesConvergeLive(t *testing.T) {
	for _, test := range []struct{ name, before, after string }{
		{name: "range replacement", before: `CREATE TYPE %s.span AS RANGE (SUBTYPE = numeric); CREATE DOMAIN %s.wrapped AS %s.span; CREATE TYPE %s.box AS (value %s.wrapped);`, after: `CREATE TYPE %s.span AS RANGE (SUBTYPE = int8); CREATE DOMAIN %s.wrapped AS %s.span NOT NULL; CREATE TYPE %s.box AS (value %s.wrapped, label text);`},
		{name: "domain replacement", before: `CREATE DOMAIN %s.base AS numeric; CREATE TYPE %s.span AS RANGE (SUBTYPE = %s.base); CREATE DOMAIN %s.wrapped AS %s.span;`, after: `CREATE DOMAIN %s.base AS bigint; CREATE TYPE %s.span AS RANGE (SUBTYPE = %s.base); COMMENT ON TYPE %s.span IS 'declared'; CREATE DOMAIN %s.wrapped AS %s.span NOT NULL;`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTypeSourceFixture(c)
			f.apply(c, f.qualify(test.before))
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE SCHEMA "+f.quotedSchema()+"; "+f.qualify(test.after)), "postgres", nil, sqlschema.Options{Owners: f.runtime.SQL()})
			c.Assert(err, qt.IsNil)
			current := must.Must(dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema}))
			original := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), current, "postgres", f.runtime))
			diff := must.Must(schemadiff.CompareWithDatabase(c.Context(), f.conn, &desired, current, nil, f.runtime))
			plan := must.Must(generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: f.runtime, Diff: diff, DesiredSchema: &desired, CurrentSchema: current, Dialect: "postgres", Capabilities: f.conn.Info().Capabilities}))
			f.apply(c, must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Forward.Nodes...)))
			f.assertConverged(c, &desired)
			f.assertExportsConverged(c)
			f.apply(c, must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Reverse.Nodes...)))
			f.assertConverged(c, original)
		})
	}
}

type typeSourceFixture struct {
	conn    *dbschema.DatabaseConnection
	runtime *engine.Runtime
	schema  string
}

func newTypeSourceFixture(c *qt.C) typeSourceFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	f := typeSourceFixture{conn: conn, runtime: must.Must(builtin.New()), schema: fmt.Sprintf("ptah_type_%d", time.Now().UnixNano())}
	f.apply(c, "CREATE SCHEMA "+f.quotedSchema())
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+f.quotedSchema()+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	return f
}

func (f typeSourceFixture) quotedSchema() string { return pgx.Identifier{f.schema}.Sanitize() }
func (f typeSourceFixture) qualify(sql string) string {
	return strings.ReplaceAll(sql, "%s", f.quotedSchema())
}

func (f typeSourceFixture) apply(c *qt.C, sql string) {
	c.Helper()
	for _, statement := range sqlutil.SplitStatementsForDialect("postgres", sql) {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func (f typeSourceFixture) assertConverged(c *qt.C, desired *schemamodel.Database) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), f.conn, desired, live, nil, f.runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("remaining changes: %#v", diff))
}

func (f typeSourceFixture) assertExportsConverged(c *qt.C) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	declared, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), live, "postgres", f.runtime)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), declared, goschematogo.Options{Annotations: f.runtime.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	fromGo, err := goschema.ParseSource(f.runtime.Annotations(), files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	f.assertConverged(c, &fromGo)
	hcl, err := atlashclrender.RenderInspected(declared, f.schema, atlashclrender.Options{HCL: f.runtime.HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	fromHCL, err := atlashcl.ParseWithOptions(c.Context(), hcl.Data, "range.hcl", atlashcl.Options{HCL: f.runtime.HCL()})
	c.Assert(err, qt.IsNil)
	f.assertConverged(c, fromHCL)
}
