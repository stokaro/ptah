//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

const operationModel = schemaext.Kind("example.org/fixture/table-operation")

func operationEnvelope(c *qt.C, runtime *engine.Runtime, name, payload string, features ...string) schemaext.Envelope {
	c.Helper()
	identity, found := runtime.Codecs().Identity(operationModel, schemaext.Operation)
	c.Assert(found, qt.IsTrue)
	envelope := providerModelEnvelope(c, identity, payload)
	var wire map[string]any
	c.Assert(json.Unmarshal(envelope.Payload, &wire), qt.IsNil)
	wire["subject"] = map[string]any{"engine": "fixture", "kind": "table", "name": name}
	wire["features"] = append([]string{"tables", "columns", "schema.common-table-identity.v1"}, features...)
	data, err := json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	envelope.Payload = data
	return envelope
}
func capturedOperation(c *qt.C, runtime *engine.Runtime, name, payload string, features ...string) ast.ExtensionPayload {
	c.Helper()
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{operationEnvelope(c, runtime, name, payload, features...)})
	c.Assert(err, qt.IsNil)
	return values[0].(ast.ExtensionPayload)
}
func unwrappedOperationTarget() capability.Capabilities {
	return capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false})
}

func TestProviderOperationRenderExecuteAndInspect(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "seed")
	create := capturedOperation(c, runtime, "records", `{"action":"create"}`)
	rename := capturedOperation(c, runtime, "records", `{"action":"rename","to":"next"}`)
	common := &ast.CreateTableNode{Name: "common", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}}
	request := renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: []ast.Node{common, &ast.ExtensionStatement{Payload: create}, &ast.AlterTableNode{Name: "records", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: rename}}}}}
	result, err := runtime.Render(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.ExecutableUnits, qt.IsTrue)
	c.Assert(must.Must(result.SQLFragments()), qt.DeepEquals, []string{`CREATE TABLE "common" ("id" INTEGER NOT NULL PRIMARY KEY);`, `CREATE TABLE "records" ("id" INTEGER NOT NULL PRIMARY KEY);`, `ALTER TABLE "records" RENAME TO "next";`})
	for _, sql := range must.Must(result.SQLFragments()) {
		c.Assert(connection.Writer().ExecuteSQL(c.Context(), sql), qt.IsNil)
	}
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "next" }), qt.IsTrue)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "records" }), qt.IsFalse)
	drop := capturedOperation(c, runtime, "next", `{"action":"drop"}`)
	c.Assert(drop.(schemaext.EffectSource).Effect().Impact, qt.Equals, schemaext.Destructive)
	result, err = runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: []ast.Node{&ast.ExtensionStatement{Payload: drop}}})
	c.Assert(err, qt.IsNil)
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), must.Must(result.SQL())), qt.IsNil)
	observed, err = connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "next" }), qt.IsFalse)
	c.Assert(connection.Close(), qt.IsNil)
	encoded, err := runtime.Codecs().Encode(c.Context(), schemaext.Operation, []schemaext.Payload{drop})
	c.Assert(err, qt.IsNil)
	var wire map[string]any
	c.Assert(json.Unmarshal(encoded[0].Payload, &wire), qt.IsNil)
	wire["payload"] = []byte(`{"action":"create"}`)
	encoded[0].Payload, err = json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	restored, err := runtime.Codecs().Decode(c.Context(), encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(restored[0].(schemaext.EffectSource).Effect().Impact, qt.Equals, schemaext.Additive)
	host.Close()
	cloned, err := ast.CloneExtensionPayload(drop)
	c.Assert(err, qt.IsNil)
	c.Assert(cloned.(schemaext.AccessEffectSource).AccessEffect().Access, qt.Equals, schemaext.AccessUnchanged)
	_, err = runtime.Codecs().Encode(c.Context(), schemaext.Operation, []schemaext.Payload{cloned})
	c.Assert(err, qt.IsNil)
	failed, err := runtime.Codecs().Decode(c.Context(), encoded)
	c.Assert(err, qt.IsNotNil)
	c.Assert(failed, qt.IsNil)
}

func TestProviderOperationRenderingRefusesWholeBatch(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	create := capturedOperation(c, runtime, "records", `{"action":"create"}`)
	rename := capturedOperation(c, runtime, "records", `{"action":"rename","to":"next"}`)
	tests := []struct {
		name         string
		nodes        []ast.Node
		capabilities capability.Capabilities
	}{
		{"wrong role", []ast.Node{&ast.ExtensionStatement{Payload: create}, &ast.ExtensionStatement{Payload: rename}}, unwrappedOperationTarget()},
		{"wrong parent", []ast.Node{&ast.ExtensionStatement{Payload: create}, &ast.AlterTableNode{Name: "wrong", Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: rename}}}}, unwrappedOperationTarget()},
		{"missing capability fact", []ast.Node{&ast.ExtensionStatement{Payload: create}}, capability.Capabilities{}},
		{"transactional target", []ast.Node{&ast.ExtensionStatement{Payload: create}}, capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: test.capabilities, Nodes: test.nodes})
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
		})
	}
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err := runtime.Render(ctx, renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: []ast.Node{&ast.ExtensionStatement{Payload: create}}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}

func TestProviderOperationDecodingRejectsInvalidOwnerInput(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	for _, test := range []struct{ name, payload string }{
		{"unknown action", `{"action":"truncate"}`},
		{"unknown field", `{"action":"create","ignored":true}`},
		{"duplicate field", `{"action":"drop","action":"create"}`},
		{"missing rename target", `{"action":"rename"}`},
		{"null target", `{"action":"create","to":null}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{operationEnvelope(c, runtime, "records", test.payload)})
			c.Assert(err, qt.IsNotNil)
			c.Assert(values, qt.IsNil)
		})
	}
}

func TestProviderOperationRenderingRejectsChangedCaptureProfile(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	envelope := operationEnvelope(c, runtime, "records", `{"action":"create"}`)
	var wire map[string]any
	c.Assert(json.Unmarshal(envelope.Payload, &wire), qt.IsNil)
	wire["features"] = make([]string, 0)
	var err error
	envelope.Payload, err = json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{envelope})
	c.Assert(err, qt.IsNil)
	result, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: []ast.Node{&ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}
