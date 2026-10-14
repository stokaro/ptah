package pgtypesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgtypesource"
	"ptah.run/internal/sqlschema"
)

func oneRange(c *qt.C, database *schemamodel.Database) (schemaext.Object, *pgtype.DesiredRange) {
	c.Helper()
	objects := must.Must(database.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	value, ok := objects[0].Value.(*pgtype.DesiredRange)
	c.Assert(ok, qt.IsTrue)
	c.Assert(database.FeatureCoverage.Lookup(pgtype.RangeKind, objects[0].Ref).State, qt.Equals, schemaext.Complete)
	return objects[0], value
}

func TestRangeAnnotationsPreserveIntentAndScope(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(annotation.NewSet(pgtypesource.Annotations()))
	database, err := goschema.ParseSource(owners, "range.go", `package entities
//ptah:schema:range name="span" schema="app" subtype="numeric" subtype_diff="" canonical="app.canon" comment="quoted \"range\"" dialects="postgres,yugabytedb"
type Types struct{}
`)
	c.Assert(err, qt.IsNil)
	object, value := oneRange(c, &database)
	c.Assert(object.Ref, qt.DeepEquals, pgtype.Ref(pgtype.RangeKind, "app", "span"))
	c.Assert(object.Targets, qt.DeepEquals, []string{"postgres", "yugabytedb"})
	c.Assert(value.Cleared, qt.DeepEquals, []string{pgtype.RangeSubtypeDiff})
	c.Assert(value.Canonical, qt.Equals, "app.canon")
	c.Assert(value.Clears(pgtype.RangeCollation), qt.IsFalse)
	files, err := goschematogo.Render(c.Context(), &database, goschematogo.Options{Annotations: owners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reloaded, err := goschema.ParseSource(owners, files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	afterObject, afterValue := oneRange(c, &reloaded)
	c.Assert(afterObject.Ref, qt.DeepEquals, object.Ref)
	c.Assert(afterObject.Targets, qt.DeepEquals, object.Targets)
	value.StructName, afterValue.StructName = "", ""
	c.Assert(afterValue, qt.DeepEquals, value)
	again, err := goschematogo.Render(c.Context(), &reloaded, goschematogo.Options{Annotations: owners, SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, files)
}

func TestRangeHCLExportReload(t *testing.T) {
	c := qt.New(t)
	database, err := atlashcl.ParseWithOptions([]byte(`schema "app" {}
range "app" "span" {
  subtype = numeric
  subtype_diff = ""
  comment = "spans"
}
`), "range.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	object, value := oneRange(c, database)
	c.Assert(object.Ref, qt.DeepEquals, pgtype.Ref(pgtype.RangeKind, "app", "span"))
	c.Assert(value.Cleared, qt.DeepEquals, []string{pgtype.RangeSubtypeDiff})
	rendered, err := atlashclrender.Render(database, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	reloaded, err := atlashcl.ParseWithOptions(rendered.Data, "reloaded.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	afterObject, afterValue := oneRange(c, reloaded)
	c.Assert(afterObject, qt.DeepEquals, object)
	c.Assert(afterValue, qt.DeepEquals, value)
}

func TestRangeSQLDeclarationsAndComments(t *testing.T) {
	for _, test := range []struct {
		name, text, schema, typeName string
		want                         pgtype.DesiredRange
	}{
		{name: "plain subtype", text: `CREATE TYPE span AS RANGE (SUBTYPE = numeric);`, typeName: "span", want: pgtype.DesiredRange{Subtype: "numeric"}},
		{name: "quoted names and options", text: `CREATE TYPE "App"."Span" AS RANGE (SUBTYPE = numeric, SUBTYPE_OPCLASS = pg_catalog.numeric_ops, SUBTYPE_DIFF = pg_catalog.numeric_sub);
COMMENT ON TYPE "App"."Span" IS 'it''s a range';`, schema: "App", typeName: "Span", want: pgtype.DesiredRange{Subtype: "numeric", SubtypeOpClass: "pg_catalog.numeric_ops", SubtypeDiff: "pg_catalog.numeric_sub", Comment: "it's a range"}},
		{name: "null clears comment", text: `CREATE TYPE span AS RANGE (SUBTYPE = numeric); COMMENT ON TYPE span IS 'old'; COMMENT ON TYPE span IS NULL;`, typeName: "span", want: pgtype.DesiredRange{Subtype: "numeric"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(c.Context(), []byte(test.text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			object, value := oneRange(c, &database)
			c.Assert(object.Ref, qt.DeepEquals, pgtype.Ref(pgtype.RangeKind, test.schema, test.typeName))
			c.Assert(value, qt.DeepEquals, &test.want)
		})
	}
}

func TestRangeSQLRefusesUnknownAndDuplicateOptions(t *testing.T) {
	for _, test := range []struct{ name, text, want string }{
		{name: "unknown", text: `CREATE TYPE span AS RANGE (SUBTYPE = numeric, MULTIRANGE_TYPE_NAME = spans);`, want: `.*unsupported range option "multirange_type_name".*`},
		{name: "duplicate", text: `CREATE TYPE span AS RANGE (SUBTYPE = numeric, SUBTYPE = int4);`, want: `.*range option "subtype" is declared more than once.*`},
		{name: "missing subtype", text: `CREATE TYPE span AS RANGE (SUBTYPE_DIFF = numeric_sub);`, want: `.*range type requires a subtype.*`},
		{name: "empty option", text: `CREATE TYPE span AS RANGE (SUBTYPE = );`, want: `.*expected a value for range option "subtype".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(c.Context(), []byte(test.text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
