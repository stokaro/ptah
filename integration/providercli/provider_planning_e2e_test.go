//go:build integration

package providercli_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/planner/featurehost"
)

const planningChangeModel = schemaext.Kind("example.org/fixture/table-change")

func planningChange(c *qt.C, runtime *engine.Runtime, name, payload string) schemaext.ChangeValue {
	c.Helper()
	identity, found := runtime.Codecs().Identity(planningChangeModel, schemaext.Change)
	c.Assert(found, qt.IsTrue)
	envelope := providerModelEnvelope(c, identity, payload)
	var wire map[string]any
	c.Assert(json.Unmarshal(envelope.Payload, &wire), qt.IsNil)
	wire["subject"] = map[string]any{"engine": "fixture", "kind": "table", "schema": "main", "name": name}
	wire["features"] = []string{"tables", "columns", "schema.common-table-identity.v1"}
	var err error
	envelope.Payload, err = json.Marshal(wire)
	c.Assert(err, qt.IsNil)
	values, err := runtime.Codecs().Decode(c.Context(), []schemaext.Envelope{envelope})
	c.Assert(err, qt.IsNil)
	return values[0].(schemaext.ChangeValue)
}

func providerPlanningRequest(c *qt.C, runtime *engine.Runtime) featureplan.Request {
	c.Helper()
	semantics := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	subject := objectidentity.NewBuilder(semantics).TableParts("main", "planned")
	return featureplan.Request{Target: "fixture", Identifiers: semantics, Capabilities: unwrappedOperationTarget(), Changes: []schemaext.ChangeRecord{{Subject: subject, Value: planningChange(c, runtime, "planned", `{"action":"create"}`)}}}
}

func TestProviderPlanningTransportJoinsGraphAndExecutes(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "seed")
	request := providerPlanningRequest(c, runtime)
	common := &ast.CreateTableNode{Name: "common", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}}
	graph, err := featurehost.NewGraph("ptah.run/common", []ast.Node{common}, nil, featurehost.Windows{Creation: 1, Dependent: 1, Removal: 1})
	c.Assert(err, qt.IsNil)
	request.CommonSteps = graph.CommonSteps()
	accepted, err := runtime.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(accepted.Err(request), qt.IsNil)
	c.Assert(accepted.Changes, qt.HasLen, 1)
	c.Assert(accepted.Changes[0].Subject, qt.DeepEquals, request.Changes[0].Subject)
	c.Assert(accepted.Contributions[0].Steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(accepted.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{accepted.Contributions[0].Steps[0].ID})
	lowered, err := featurehost.Plan(c.Context(), runtime, request, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(lowered.Contributions[0].Steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	nodes, err := graph.Schedule(c.Context(), lowered)
	c.Assert(err, qt.IsNil)
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Fragments, qt.DeepEquals, []string{`CREATE TABLE "common" ("id" INTEGER NOT NULL PRIMARY KEY);`, `CREATE TABLE "planned" ("id" INTEGER NOT NULL PRIMARY KEY);`})
	// This explicit test caller runs each statement without a transaction. It
	// does not qualify the migration executor's transaction requirement handling.
	for _, sql := range rendered.Fragments {
		c.Assert(connection.Writer().ExecuteSQL(c.Context(), sql), qt.IsNil)
	}
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "planned" }), qt.IsTrue)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "common" }), qt.IsTrue)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	envelopes, err := runtime.Codecs().Encode(c.Context(), schemaext.Operation, []schemaext.Payload{accepted.Contributions[0].Steps[0].Payload.Payload})
	c.Assert(err, qt.IsNil)
	c.Assert(envelopes, qt.HasLen, 1)
}

func TestProviderPlanningRefusalAndCancellationDiscardOperations(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	request.Capabilities = capability.Capabilities{}
	result, err := runtime.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Err(request), qt.IsNotNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = runtime.PlanFeatures(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, featureplan.Result{})
}

func TestProviderPlanningAlterUsesCapturedParent(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "planned")
	request := providerPlanningRequest(c, runtime)
	subject := request.Changes[0].Subject
	request.Changes[0].Value = planningChange(c, runtime, "planned", `{"action":"rename","to":"renamed"}`)
	request.Tables = []featureplan.Table{{Subject: subject, Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "planned", Schema: "main"}}, Current: schemacapture.TableObservation{Table: catalog.Table{Name: "planned", Schema: "main"}}}}
	graph, err := featurehost.NewGraph("ptah.run/common", nil, nil, featurehost.Windows{})
	c.Assert(err, qt.IsNil)
	request.CommonSteps = graph.CommonSteps()
	lowered, err := featurehost.Plan(c.Context(), runtime, request, map[objectidentity.Key]string{subject.Key(): "main.planned"})
	c.Assert(err, qt.IsNil)
	nodes, err := graph.Schedule(c.Context(), lowered)
	c.Assert(err, qt.IsNil)
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Fragments, qt.DeepEquals, []string{`ALTER TABLE "planned" RENAME TO "renamed";`})
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), rendered.SQL()), qt.IsNil)
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "renamed" }), qt.IsTrue)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "planned" }), qt.IsFalse)
	request.Tables = nil
	refused, err := runtime.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(refused, qt.DeepEquals, featureplan.Result{})
}

func TestProviderPlanningConflictingCommonEffectsDiscardGraph(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	common := &ast.CreateTableNode{Name: "planned", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER"}}}
	effects := [][]plangraph.Effect{{{Subject: request.Changes[0].Subject, Action: plangraph.Create}}}
	graph, err := featurehost.NewGraph("ptah.run/common", []ast.Node{common}, effects, featurehost.Windows{Creation: 1, Dependent: 1, Removal: 1})
	c.Assert(err, qt.IsNil)
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(c.Context(), runtime, request, nil)
	c.Assert(err, qt.IsNil)
	nodes, err := graph.Schedule(c.Context(), features)
	c.Assert(err, qt.IsNotNil)
	c.Assert(nodes, qt.IsNil)
}
