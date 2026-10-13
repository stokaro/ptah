package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

func propertyDocument(c *qt.C) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(`
 CREATE FUNCTION f(a integer) RETURNS integer LANGUAGE sql LEAKPROOF STRICT PARALLEL SAFE AS $$SELECT a$$;
 CREATE FUNCTION f(a text) RETURNS text LANGUAGE sql NOT LEAKPROOF CALLED ON NULL INPUT PARALLEL RESTRICTED AS $$SELECT a$$;
 `), "postgres", nil, sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	c.Assert(err, qt.IsNil)
	return &database
}
func declaredProperties(c *qt.C, database *schemamodel.Database) map[string]pgroutine.Properties {
	c.Helper()
	result := make(map[string]pgroutine.Properties)
	for _, function := range database.Functions {
		value, _, err := pgroutine.PropertiesOf(function.Facets)
		c.Assert(err, qt.IsNil)
		result[function.Parameters] = value
	}
	return result
}
func TestPostgresFunctionProperties_ConversionAndOverloads(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := propertyDocument(c)
	c.Assert(declaredProperties(c, desired), qt.DeepEquals, map[string]pgroutine.Properties{
		"a integer": {Leakproof: true, Strict: true, Parallel: "SAFE"}, "a text": {Parallel: "RESTRICTED"},
	})
	current, err := goschematodb.ToDBSchema(c.Context(), desired, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	_, found, err := schemaext.FacetAs[*pgroutine.ObservedProperties](current.Functions[0].Facets, pgroutine.PropertiesKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	captured, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(declaredProperties(c, captured), qt.DeepEquals, declaredProperties(c, desired))
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
	statements, err := builtin.GetOrderedCreateStatements(captured, "postgres")
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, "LEAKPROOF STRICT PARALLEL SAFE")
	c.Assert(sql, qt.Contains, "PARALLEL RESTRICTED")
}
func TestPostgresFunctionProperties_GoExportOverloads(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := propertyDocument(c)
	files, err := goschematogo.Render(c.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true, Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(runtime.Annotations(), "schema.go", files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(declaredProperties(c, &parsed), qt.DeepEquals, declaredProperties(c, desired))
}
func TestPostgresFunctionProperties_HCLExportOverloads(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := propertyDocument(c)
	rendered, err := atlashclrender.Render(desired, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.ParseWithOptions(rendered.Data, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(declaredProperties(c, parsed), qt.DeepEquals, declaredProperties(c, desired))
}
func TestPostgresFunctionProperties_YAML(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	parsed, err := yamlschema.Parse(runtime.YAML(), []byte(`functions:
  f:
    params: a integer
    returns: integer
    language: sql
    leakproof: true
    strict: true
    parallel: SAFE
    body: SELECT a
`))
	c.Assert(err, qt.IsNil)
	c.Assert(declaredProperties(c, parsed), qt.DeepEquals, map[string]pgroutine.Properties{"a integer": {Leakproof: true, Strict: true, Parallel: "SAFE"}})
}
func TestPostgresFunctionProperties_Changes(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current pgroutine.Properties
		changes          int
	}{
		{name: "leakproof added", desired: pgroutine.Properties{Leakproof: true}, changes: 1},
		{name: "leakproof removed", current: pgroutine.Properties{Leakproof: true}, changes: 1},
		{name: "strict added", desired: pgroutine.Properties{Strict: true}, changes: 1},
		{name: "strict removed", current: pgroutine.Properties{Strict: true}, changes: 1},
		{name: "parallel added", desired: pgroutine.Properties{Parallel: "SAFE"}, changes: 1},
		{name: "parallel reset", current: pgroutine.Properties{Parallel: "SAFE"}, changes: 1},
		{name: "parallel changed", desired: pgroutine.Properties{Parallel: "RESTRICTED"}, current: pgroutine.Properties{Parallel: "SAFE"}, changes: 1},
		{name: "unsafe means default", desired: pgroutine.Properties{Parallel: "UNSAFE"}},
		{name: "equal", desired: pgroutine.Properties{Strict: true}, current: pgroutine.Properties{Strict: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Parameters: "a integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT a", Facets: must.Must(pgroutine.WithProperties(schemaext.Facets{}, test.desired))}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
			current := &catalog.Database{Functions: []catalog.Function{{Name: "f", Parameters: "a integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT a", Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, test.current))}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "postgres", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, test.changes)
			for _, function := range diff.FunctionsModified {
				c.Assert(function.Changes, qt.HasLen, 0)
				c.Assert(function.FeatureChanges, qt.HasLen, 1)
				change := function.FeatureChanges[0].Value.(*pgroutine.PropertiesChange)
				inverse, err := change.Reverse()
				c.Assert(err, qt.IsNil)
				restored, err := inverse.Reverse()
				c.Assert(err, qt.IsNil)
				c.Assert(restored, qt.DeepEquals, change)
			}
		})
	}
}

func TestPostgresFunctionProperties_ExportRefusesUnknownOverload(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := propertyDocument(c)
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Function("f", "integer")
	desired.FeatureCoverage = must.Must(pgroutine.PropertiesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: pgroutine.PropertiesKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the catalog cannot describe this overload"}}}))
	files, err := goschematogo.Render(t.Context(), desired, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true, Dialect: "postgres"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(files, qt.HasLen, 0)
	_, err = atlashclrender.Render(desired, atlashclrender.Options{HCL: runtime.HCL(), Dialect: "postgres"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
func TestPostgresFunctionProperties_SQLFailurePath(t *testing.T) {
	for _, clause := range []string{"LEAKPROOF LEAKPROOF", "STRICT CALLED ON NULL INPUT", "RETURNS NULL ON NULL INPUT STRICT", "PARALLEL SAFE PARALLEL RESTRICTED", "PARALLEL FAST", "CALLED ON INPUT"} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			_, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE FUNCTION f(a integer) RETURNS integer LANGUAGE sql "+clause+" AS $$SELECT a$$;"), "postgres", nil, sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}
