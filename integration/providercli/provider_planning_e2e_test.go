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
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
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
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`CREATE TABLE "common" ("id" INTEGER NOT NULL PRIMARY KEY);`, `CREATE TABLE "planned" ("id" INTEGER NOT NULL PRIMARY KEY);`})
	// This explicit test caller runs each statement without a transaction. It
	// does not qualify the migration executor's transaction requirement handling.
	for _, sql := range must.Must(rendered.SQLFragments()) {
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
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`ALTER TABLE "planned" RENAME TO "renamed";`})
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), must.Must(rendered.SQL())), qt.IsNil)
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
	effects := []featureplan.CommonStep{{Effects: []plangraph.Effect{{Subject: request.Changes[0].Subject, Action: plangraph.Create}}}}
	graph, err := featurehost.NewGraph("ptah.run/common", []ast.Node{common}, effects, featurehost.Windows{Creation: 1, Dependent: 1, Removal: 1})
	c.Assert(err, qt.IsNil)
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(c.Context(), runtime, request, nil)
	c.Assert(err, qt.IsNil)
	nodes, err := graph.Schedule(c.Context(), features)
	c.Assert(err, qt.IsNotNil)
	c.Assert(nodes, qt.IsNil)
}

func commonPlanningDiff(request featureplan.Request, name string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{IdentifierSemantics: &request.Identifiers, FeatureChanges: request.Changes,
		TablesAdded: difftypes.TableChanges{{Name: name, Table: schemamodel.Table{Name: name},
			Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", Primary: true}}}},
	}
}

func TestProviderCommonPlannerDispatchesAndExecutesCompleteGraph(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "seed")
	request := providerPlanningRequest(c, runtime)
	diff := commonPlanningDiff(request, "common")
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: request.Capabilities})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	c.Assert(ast.OwnerTransactions(nodes[1]), qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
	c.Assert(planner.RequiresNoTransaction("fixture", nodes), qt.IsTrue)
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`CREATE TABLE "common" ("id" INTEGER NOT NULL PRIMARY KEY);`, `CREATE TABLE "planned" ("id" INTEGER NOT NULL PRIMARY KEY);`})
	// Execution is explicit here. This checks the production planner and its
	// transaction classification, without claiming migration history support.
	for _, sql := range must.Must(rendered.SQLFragments()) {
		c.Assert(connection.Writer().ExecuteSQL(c.Context(), sql), qt.IsNil)
	}
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "common" }), qt.IsTrue)
	c.Assert(slices.ContainsFunc(observed.Tables, func(table catalog.Table) bool { return table.Name == "planned" }), qt.IsTrue)
}

func TestProviderCommonPlannerCapturesUnchangedParents(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	subject := request.Changes[0].Subject
	request.Changes[0].Value = planningChange(c, runtime, "planned", `{"action":"rename","to":"renamed"}`)
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &request.Identifiers, FeatureChanges: request.Changes,
		TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{{Subject: subject,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "planned", Schema: "main"}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "planned", Schema: "main"}},
		}}},
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: request.Capabilities})
	c.Assert(err, qt.IsNil)
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`ALTER TABLE "planned" RENAME TO "renamed";`})
	diff.TablePreparation = nil
	nodes, err = planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: request.Capabilities})
	c.Assert(err, qt.IsNotNil)
	c.Assert(nodes, qt.IsNil)
}

func TestProviderCommonPlannerRefusesBeforeReturningCommonPrefix(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	required := planningChange(c, runtime, "planned", `{"action":"create","transaction":"required"}`)
	for _, test := range []struct {
		name    string
		common  string
		value   schemaext.ChangeValue
		caps    capability.Capabilities
		message string
		kind    error
	}{
		{"transaction required", "common", required, request.Capabilities, ".*requires a transaction.*", ptaherr.ErrUnsupportedFeature},
		{"unknown capabilities", "common", request.Changes[0].Value, capability.Capabilities{}, ".*explicit nontransactional DDL.*", ptaherr.ErrUnsupportedFeature},
		{"common collision", "planned", request.Changes[0].Value, request.Capabilities, ".*", ptaherr.ErrInvalidSchemaDiff},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := commonPlanningDiff(request, test.common)
			diff.FeatureChanges = slices.Clone(diff.FeatureChanges)
			diff.FeatureChanges[0].Value = test.value
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: test.caps})
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(err, qt.ErrorIs, test.kind)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestProviderPlanningAccountsForUnchangedParentModels(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	request.Changes = nil
	subject := objectidentity.NewBuilder(request.Identifiers).TableParts("main", "common")
	request.Tables = []featureplan.Table{{Subject: subject, Action: featureplan.CreateTable,
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "common", Schema: "main"}}}}
	result, err := runtime.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Parents[0].Subject, qt.DeepEquals, subject)
	c.Assert(result.Parents[0].Kind, qt.Equals, schemaext.Kind("example.org/fixture/retention"))
	c.Assert(result.Parents[0].Action, qt.Equals, featureplan.CreateTable)
	request.Capabilities = capability.Capabilities{}
	refused, err := runtime.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(refused.Parents, qt.HasLen, 0)
}

func columnRewriteDiff(request featureplan.Request) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{IdentifierSemantics: &request.Identifiers,
		TablesModified: []difftypes.TableDiff{{TableName: "planned",
			ColumnsAdded: difftypes.ColumnChanges{{Name: "added", Type: "REAL", Nullable: true}},
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "planned"},
				Fields: []schemamodel.Field{{Name: "Id", Type: "INTEGER", Primary: true}, {Name: "added", Type: "TEXT"}}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "planned"}},
		}},
	}
}

func TestProviderCommonPlannerRewritesExactColumnOperand(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "planned")
	request := providerPlanningRequest(c, runtime)
	diff := columnRewriteDiff(request)
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: request.Capabilities})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(ast.OwnerOperations(nodes[0]), qt.HasLen, 1)
	c.Assert(ast.OwnerTransactions(nodes[0]), qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
	c.Assert(planner.RequiresNoTransaction("fixture", nodes), qt.IsTrue)
	rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: request.Capabilities, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	// The common operand differs from the final declaration. The replacement
	// must preserve that operand and emit it exactly once.
	c.Assert(must.Must(rendered.SQLFragments()), qt.DeepEquals, []string{`ALTER TABLE "planned" ADD COLUMN "added" REAL;`})
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), must.Must(rendered.SQL())), qt.IsNil)
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 1)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(observed.Tables[0].Columns[1].Name, qt.Equals, "added")
	c.Assert(observed.Tables[0].Columns[1].DataType, qt.Equals, "REAL")
	c.Assert(observed.Tables[0].Columns[1].IsNullable, qt.Equals, "YES")
}

func TestProviderCommonPlannerRefusesUnsupportedRewriteOperand(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "operation-models")
	request := providerPlanningRequest(c, runtime)
	diff := columnRewriteDiff(request)
	diff.TablesModified[0].ColumnsAdded[0].Primary = true
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, diff, "fixture", planext.Options{Capabilities: request.Capabilities})
	c.Assert(err, qt.ErrorMatches, `(?s).*unsupported added column properties.*`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}

func desiredProviderColumn() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "planned", StructName: "Planned"}},
		Fields: []schemamodel.Field{
			{StructName: "Planned", Name: "Id", Type: "INTEGER", Primary: true},
			{StructName: "Planned", Name: "added", Type: "REAL", Nullable: true},
		},
	}
}

func TestProviderPreparedApplyPreservesLiveFactsAndTransactionRequirements(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "planned")
	caps := connection.Info().Capabilities
	c.Assert(caps.Established(capability.TransactionalDDL), qt.IsTrue)
	c.Assert(caps.Has(capability.TransactionalDDL), qt.IsFalse)
	c.Assert(caps.Established(capability.ForeignKeys), qt.IsFalse)
	plan, err := atlasschema.PrepareApply(c.Context(), connection, atlasschema.ApplyRuntimeOptions{
		Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.PreflightExecution(must.Must(plan.SQL())), qt.ErrorMatches, ".*requires execution outside a transaction.*")
	c.Assert(plan.Execute(c.Context()), qt.ErrorMatches, ".*requires execution outside a transaction.*")
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
	plan, err = atlasschema.PrepareApply(c.Context(), connection, atlasschema.ApplyRuntimeOptions{
		Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Execute(c.Context()), qt.IsNil)
	observed, err = connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(observed.Tables[0].Columns[1].Name, qt.Equals, "added")
}

func TestProviderSavedApplyPreservesTransactionRequirements(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "operation-models")
	connection := observedConnection(c, host, "planned")
	plan, err := atlasschema.PreparePlanFile(c.Context(), connection, atlasschema.PlanFileOptions{Runtime: runtime, Desired: desiredProviderColumn()})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].Transactions, qt.DeepEquals, []plangraph.Transaction{plangraph.TransactionForbidden})
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	decoded, _, err := atlasschema.DecodePlanFile(encoded, "provider.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.VerifyPlanTarget(c.Context(), connection, decoded, runtime), qt.IsNil)
	c.Assert(decoded.Execute(c.Context(), connection, migrator.MigrationTxModeFile), qt.ErrorMatches, ".*requires execution outside a transaction.*")
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
	c.Assert(decoded.Execute(c.Context(), connection, migrator.MigrationTxModeNone), qt.IsNil)
	observed, err = connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 2)
}
