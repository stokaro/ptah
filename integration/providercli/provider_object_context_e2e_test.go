//go:build integration

package providercli_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/migration/planner"
)

const dependentObjectKind schemaext.Kind = "example.org/fixture/dependent-object"

func dependentObject(c *qt.C, runtime *engine.Runtime, representation schemaext.Representation, name string) schemaext.Object {
	c.Helper()
	identity, found := runtime.Codecs().Identity(dependentObjectKind, representation)
	c.Assert(found, qt.IsTrue)
	wire := map[string]any{"format": 1, "engine": "fixture", "features": []string{"tables", "columns", "schema.common-table-identity.v1", "planning.object-context", "planning.lifecycle", "render.text-units"}, "required_features": make([]string, 0), "payload": []byte(`{"action":"create"}`), "subject": map[string]any{"engine": "fixture", "kind": string(dependentObjectKind), "schema": "main", "name": name}}
	data, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{{Format: 1, Owner: identity.Owner, Kind: identity.Kind, Representation: representation, Version: identity.Version, Definition: identity.Definition, Payload: data}})
	c.Assert(err, qt.IsNil)
	subject := objectidentity.ID{Kind: objectidentity.Kind(dependentObjectKind), Schema: objectidentity.Part{Source: "main", Normalized: "main"}, Name: objectidentity.Part{Source: name, Normalized: name}}
	return schemaext.Object{Ref: subject, Value: values[0].(schemaext.Value)}
}

func dependentDiff(c *qt.C, runtime *engine.Runtime, name string) *difftypes.SchemaDiff {
	c.Helper()
	desired, err := schemaext.NewObjects(dependentObject(c, runtime, schemaext.Desired, name))
	c.Assert(err, qt.IsNil)
	current, err := schemaext.NewObjects(dependentObject(c, runtime, schemaext.Observed, name))
	c.Assert(err, qt.IsNil)
	rules := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, Features: difftypes.FeatureContext{DesiredObjects: desired, CurrentObjects: current}, TablesRemoved: difftypes.TableRemovals{{Name: "input", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "input", Type: "TABLE", Columns: []catalog.Column{{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1}}}}}}}
}

func TestProviderUnchangedObjectContextReachesThePublicPlanner(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=object-context"})
	diff := dependentDiff(c, runtime, "marker")
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	result, err := planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
	c.Assert(err, qt.IsNil)
	sql, err := result.Result.SQLFragments()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.DeepEquals, []string{`CREATE TABLE "marker" ("id" INTEGER NOT NULL PRIMARY KEY);`, `DROP TABLE "input";`})
	diff.TablesRemoved = nil
	result, err = planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
	c.Assert(err, qt.IsNil)
	sql, err = result.Result.SQLFragments()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.HasLen, 0)
}

func TestProviderObjectContextRefusalDiscardsCommonOperations(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=object-context"})
	for _, name := range []string{"refused", "omitted"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			result, err := planner.GenerateSchemaDiffRenderedPlan(c.Context(), runtime, dependentDiff(c, runtime, name), "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.DeepEquals, planner.RenderedPlan{})
		})
	}
}
