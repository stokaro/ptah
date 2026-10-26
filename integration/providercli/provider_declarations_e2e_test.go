//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

const authoredObjectKind schemaext.Kind = "example.org/fixture/authored-object"

func authoredObject(c *qt.C, runtime *engine.Runtime, name string) schemaext.Object {
	c.Helper()
	identity, found := runtime.Codecs().Identity(authoredObjectKind, schemaext.Desired)
	c.Assert(found, qt.IsTrue)
	wire := map[string]any{"format": 1, "engine": "fixture", "features": []string{"tables", "columns", "schema.declarations", "schema.validate", "render.text-units", "planning.lifecycle"}, "required_features": make([]string, 0), "payload": []byte(`{"action":"create"}`), "subject": map[string]any{"engine": "fixture", "kind": string(authoredObjectKind), "name": name, "schema": "main"}}
	data, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{{Format: 1, Owner: identity.Owner, Kind: identity.Kind, Representation: identity.Representation, Version: identity.Version, Definition: identity.Definition, Payload: data}})
	c.Assert(err, qt.IsNil)
	subject := objectidentity.ID{Kind: objectidentity.Kind(authoredObjectKind), Schema: objectidentity.Part{Source: "main", Normalized: "main"}, Name: objectidentity.Part{Source: name, Normalized: name}}
	return schemaext.Object{Ref: subject, Value: values[0].(schemaext.Value), Targets: []string{"fixture"}}
}

func authoredRequest(c *qt.C, runtime *engine.Runtime, name string) renderer.SchemaRequest {
	c.Helper()
	objects, err := schemaext.NewObjects(authoredObject(c, runtime, name))
	c.Assert(err, qt.IsNil)
	return renderer.SchemaRequest{Target: "fixture", Capabilities: unwrappedOperationTarget(), Identifiers: identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}, Schema: &schemamodel.Database{FeatureObjects: objects, Tables: []schemamodel.Table{{Name: "captured", Schema: "main", StructName: "Captured"}}, Fields: []schemamodel.Field{{StructName: "Captured", Name: "id", Type: "INTEGER", Primary: true, Comment: "retained"}}, Indexes: []schemamodel.Index{{Name: "by_id", TableName: "captured", Fields: []string{"id"}}}, Triggers: []schemamodel.Trigger{{Name: "audit", Table: "captured", Body: "SELECT 1"}}}}
}

func TestProviderRendersCompleteAuthoredSchemaWithoutSession(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=declarations"})
	request := authoredRequest(c, runtime, "standalone")
	rendered, err := runtime.RenderSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Statements, qt.DeepEquals, []string{`CREATE TABLE "captured" ("id" INTEGER NOT NULL PRIMARY KEY);`, `CREATE TABLE "standalone" ("id" INTEGER NOT NULL PRIMARY KEY);`})
	again, err := runtime.RenderSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.DeepEquals, rendered)
	c.Assert(request.Schema.Indexes[0].Name, qt.Equals, "by_id")
}

func TestProviderAuthoredRefusalsDiscardTheCompleteSchema(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=declarations"})
	for _, name := range []string{"refused", "omitted", "cycle"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := runtime.RenderSchema(c.Context(), authoredRequest(c, runtime, name))
			c.Assert(err, qt.IsNotNil)
			c.Assert(rendered, qt.DeepEquals, renderer.SchemaResult{})
		})
	}
	request := authoredRequest(c, runtime, "standalone")
	request.Schema.Tables[0].Name = "unclaimed"
	request.Schema.Indexes[0].TableName = "unclaimed"
	request.Schema.Triggers[0].Table = "unclaimed"
	rendered, err := runtime.RenderSchema(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `.*unclaimed provider table creation.*`)
	c.Assert(rendered, qt.DeepEquals, renderer.SchemaResult{})
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	rendered, err = runtime.RenderSchema(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(rendered, qt.DeepEquals, renderer.SchemaResult{})
}
