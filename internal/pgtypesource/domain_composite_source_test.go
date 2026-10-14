package pgtypesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
)

func TestDomainCompositeSourcesKeepDefinitions(t *testing.T) {
	for _, test := range []struct {
		name, sql, schema, typeName string
		want                        schemaext.Value
	}{
		{name: "domain options", sql: `CREATE DOMAIN app.amount AS numeric(10,2) DEFAULT (round(4.5, 1)) NOT NULL CHECK (VALUE > 0); COMMENT ON DOMAIN app.amount IS 'positive amount';`, schema: "app", typeName: "amount",
			want: &pgtype.DesiredDomain{BaseType: "numeric(10,2)", DefaultExpr: "(round(4.5, 1))", NotNull: true, Check: "VALUE > 0", Comment: "positive amount"}},
		{name: "optional AS and null default", sql: `CREATE DOMAIN amount integer DEFAULT NULL; COMMENT ON DOMAIN amount IS 'old'; COMMENT ON DOMAIN public.amount IS NULL;`, typeName: "amount", want: &pgtype.DesiredDomain{BaseType: "integer", DefaultExpr: "NULL"}},
		{name: "case expression", sql: `CREATE DOMAIN flag boolean DEFAULT CASE WHEN 1 IS NOT NULL THEN TRUE ELSE FALSE END NOT NULL;`, typeName: "flag", want: &pgtype.DesiredDomain{BaseType: "boolean", DefaultExpr: "CASE WHEN 1 IS NOT NULL THEN TRUE ELSE FALSE END", NotNull: true}},
		{name: "nullable default", sql: `CREATE DOMAIN d integer DEFAULT 7 NULL;`, typeName: "d", want: &pgtype.DesiredDomain{BaseType: "integer", DefaultExpr: "7"}},
		{name: "null operand", sql: `CREATE DOMAIN d integer DEFAULT 7 + NULL NULL;`, typeName: "d", want: &pgtype.DesiredDomain{BaseType: "integer", DefaultExpr: "7 + NULL"}},
		{name: "null predicate", sql: `CREATE DOMAIN d boolean DEFAULT 1 IS NOT NULL NULL;`, typeName: "d", want: &pgtype.DesiredDomain{BaseType: "boolean", DefaultExpr: "1 IS NOT NULL"}},
		{name: "quoted collation identifier", sql: `CREATE TYPE t AS (value "COLLATE");`, typeName: "t", want: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "value", Type: `"COLLATE"`}}}},
		{name: "composite quoted fields", sql: `CREATE TYPE " App "." Record " AS ("x,y:z" numeric(10,2), " spaced " timestamp with time zone, vals "App"."Entry"[]); COMMENT ON TYPE " App "." Record " IS 'record';`, schema: " App ", typeName: " Record ",
			want: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "x,y:z", Type: "numeric(10,2)"}, {Name: " spaced ", Type: "timestamp with time zone"}, {Name: "vals", Type: `"App"."Entry"[]`}}, Comment: "record"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			original, _, err := sqlschema.ReadWith(c.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.IsNil)
			objects := must.Must(original.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, 1)
			ref := pgtype.Ref(test.want.Kind(), test.schema, test.typeName)
			c.Assert(objects[0].Ref, qt.DeepEquals, ref)
			c.Assert(objects[0].Value, qt.DeepEquals, test.want)
			files, err := goschematogo.Render(c.Context(), &original, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			fromGo, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			hcl, err := atlashclrender.Render(&original, atlashclrender.Options{HCL: runtime.HCL()})
			c.Assert(err, qt.IsNil)
			fromHCL, err := atlashcl.ParseWithOptions(hcl.Data, "type.hcl", atlashcl.Options{HCL: runtime.HCL()})
			c.Assert(err, qt.IsNil)
			composer, ok := test.want.(schemaext.ComposableDeclaration)
			c.Assert(ok, qt.IsTrue)
			for _, read := range []*schemamodel.Database{&fromGo, fromHCL} {
				actual, found, err := read.FeatureObjects.Get(ref)
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				combined, err := composer.ComposeDeclaration(actual.Value)
				c.Assert(err, qt.IsNil)
				c.Assert(combined, qt.DeepEquals, test.want)
				c.Assert(read.FeatureCoverage.Lookup(test.want.Kind(), ref).State, qt.Equals, schemaext.Complete)
			}
		})
	}
}

func TestDomainSQLRefusesUnrepresentedClauses(t *testing.T) {
	for _, test := range []struct{ sql, want string }{
		{sql: `CREATE DOMAIN d text COLLATE "C";`, want: `.*unsupported domain clause "COLLATE".*`},
		{sql: `CREATE DOMAIN d integer CONSTRAINT positive CHECK (VALUE > 0);`, want: `.*unsupported domain clause "CONSTRAINT".*`},
		{sql: `CREATE DOMAIN d integer CHECK (VALUE > 0) CHECK (VALUE < 10);`, want: `.*declares CHECK more than once.*`},
		{sql: `CREATE DOMAIN d integer NOT NULL NULL;`, want: `.*declares NULL more than once.*`},
		{sql: `CREATE DOMAIN d numeric(4,2;`, want: `.*expected a complete domain type or default.*`},
		{sql: `CREATE TYPE t AS (value text COLLATE "C");`, want: `.*composite attribute "value": COLLATE is not represented.*`},
		{sql: `CREATE TYPE t AS (n numeric(4,2),);`, want: `.*expected a composite attribute after comma.*`},
	} {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)
			got, _, err := sqlschema.ReadWith(c.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
