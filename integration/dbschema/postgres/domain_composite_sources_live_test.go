//go:build integration

package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// itemsTable is a table whose amount column is typed by the domain, with the
// column's own constraint.
func itemsTable(column string) string {
	return `CREATE TABLE %s.items (id integer PRIMARY KEY, amount %s.amount` + column + `);`
}

// Domain constraints stay on their type. Reading effective column nullability
// would copy the domain's NOT NULL to the column and leave a repeated diff.
// A row without before starts from an empty schema.
func TestDomainSourcesKeepConstraintPlacementLive(t *testing.T) {
	for _, test := range []struct {
		name, before, after, defaultSQL string
		notNull, columnNotNull          bool
	}{
		{name: "create constrained domain", after: `CREATE DOMAIN %s.amount AS numeric(10,2) DEFAULT round(4.5, 1) NOT NULL CHECK (VALUE > 0); COMMENT ON DOMAIN %s.amount IS 'positive';` + itemsTable(""),
			defaultSQL: "round(4.5, 1)", notNull: true},
		{name: "change constraints", before: `CREATE DOMAIN %s.amount AS integer DEFAULT 1 CHECK (VALUE >= 0);` + itemsTable(""),
			after: `CREATE DOMAIN %s.amount AS integer DEFAULT 7 NOT NULL CHECK (VALUE > 0); COMMENT ON DOMAIN %s.amount IS 'positive';` + itemsTable(""), defaultSQL: "7", notNull: true},
		{name: "add first constraints", before: `CREATE DOMAIN %s.amount AS integer;` + itemsTable(""),
			after: `CREATE DOMAIN %s.amount AS integer DEFAULT 7 NOT NULL CHECK (VALUE > 0);` + itemsTable(""), defaultSQL: "7", notNull: true},
		{name: "relax domain nullability", before: `CREATE DOMAIN %s.amount AS integer DEFAULT 7 NOT NULL CHECK (VALUE > 0);` + itemsTable(""),
			after: `CREATE DOMAIN %s.amount AS integer;` + itemsTable(""), defaultSQL: "7"},
		{name: "retain column constraint", before: `CREATE DOMAIN %s.amount AS integer NOT NULL;` + itemsTable(" NOT NULL"),
			after: `CREATE DOMAIN %s.amount AS integer;` + itemsTable(" NOT NULL"), columnNotNull: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTypeSourceFixture(c)
			f.apply(c, f.qualify(test.before))
			current := must.Must(dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema}))
			reverse := applyTypeDeclaration(c, f, current, test.after)
			var domainNotNull, columnNotNull bool
			var defaultSQL string
			err := f.conn.QueryRowContext(c.Context(), `SELECT t.typnotnull, COALESCE(t.typdefault, ''), a.attnotnull FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace JOIN pg_class r ON r.relnamespace=n.oid AND r.relname='items' JOIN pg_attribute a ON a.attrelid=r.oid AND a.attname='amount' WHERE n.nspname=$1 AND t.typname='amount'`, f.schema).Scan(&domainNotNull, &defaultSQL, &columnNotNull)
			c.Assert(err, qt.IsNil)
			c.Assert(domainNotNull, qt.Equals, test.notNull)
			c.Assert(columnNotNull, qt.Equals, test.columnNotNull)
			c.Assert(defaultSQL, qt.Equals, test.defaultSQL)
			assertTypeRestoration(c, f, current, reverse)
		})
	}
}

// Attribute names and order come from the live catalog, so a comparator that
// folds quoted names cannot turn an unapplied change into a passing round trip.
func TestCompositeSourcesKeepAttributesLive(t *testing.T) {
	for _, test := range []struct {
		name, before, after string
		attributes          []string
	}{
		{name: "quoted names", after: `CREATE TYPE %s.record AS (" x:y,z " numeric(10,2), """label""" text);`, attributes: []string{" x:y,z ", `"label"`}},
		{name: "append attribute", before: `CREATE TYPE %s.record AS (n integer);`, after: `CREATE TYPE %s.record AS (n integer, label text);`, attributes: []string{"n", "label"}},
		{name: "drop attribute", before: `CREATE TYPE %s.record AS (n integer, label text);`, after: `CREATE TYPE %s.record AS (n integer);`, attributes: []string{"n"}},
		{name: "quoted case", before: `CREATE TYPE %s.record AS (n integer);`, after: `CREATE TYPE %s.record AS ("N" integer);`, attributes: []string{"N"}},
		{name: "replace attribute type", before: `CREATE TYPE %s.record AS (n integer);`, after: `CREATE TYPE %s.record AS (n bigint);`, attributes: []string{"n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTypeSourceFixture(c)
			f.apply(c, f.qualify(test.before))
			current := must.Must(dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema}))
			reverse := applyTypeDeclaration(c, f, current, test.after)
			rows, err := f.conn.QueryContext(c.Context(), `SELECT a.attname FROM pg_attribute a JOIN pg_type t ON t.typrelid=a.attrelid JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=$1 AND t.typname='record' AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, f.schema)
			c.Assert(err, qt.IsNil)
			var names []string
			for rows.Next() {
				var name string
				c.Assert(rows.Scan(&name), qt.IsNil)
				names = append(names, name)
			}
			c.Assert(rows.Err(), qt.IsNil)
			c.Assert(rows.Close(), qt.IsNil)
			c.Assert(names, qt.DeepEquals, test.attributes)
			assertTypeRestoration(c, f, current, reverse)
		})
	}
}

func applyTypeDeclaration(c *qt.C, f typeSourceFixture, current *catalog.Database, body string) string {
	c.Helper()
	desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE SCHEMA "+f.quotedSchema()+"; "+f.qualify(body)), "postgres", nil, sqlschema.Options{Owners: f.runtime.SQL()})
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDatabase(c.Context(), f.conn, &desired, current, nil, f.runtime))
	plan := must.Must(generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: f.runtime, Diff: diff, DesiredSchema: &desired, CurrentSchema: current, Dialect: "postgres", Capabilities: f.conn.Info().Capabilities}))
	f.apply(c, must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Forward.Nodes...)))
	f.assertConverged(c, &desired)
	f.assertExportsConverged(c)
	return must.Must(builtin.RenderSQLWithCapabilities("postgres", f.conn.Info().Capabilities, plan.Reverse.Nodes...))
}

func assertTypeRestoration(c *qt.C, f typeSourceFixture, current *catalog.Database, reverse string) {
	c.Helper()
	f.apply(c, reverse)
	original := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), current, "postgres", f.runtime))
	f.assertConverged(c, original)
	restored := must.Must(dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema}))
	c.Assert(must.Must(restored.FeatureObjects.All()), qt.DeepEquals, must.Must(current.FeatureObjects.All()))
}
