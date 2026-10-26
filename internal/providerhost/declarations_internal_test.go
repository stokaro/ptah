package providerhost

// White-box testing required: a renderer's SQL cannot reveal whether private
// wire capture or parent context invents observed state.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

func TestDeclarationCaptureRetainsCompleteAuthoredTables(t *testing.T) {
	c := qt.New(t)
	planning, _ := planningFixture(c)
	service := modelDeclarationPlanner{target: planning.target, declaration: &protocol.ModelDeclarations{Id: "authored"}}
	request := featureplan.DeclarationRequest{Target: "fixture", Identifiers: identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}, Tables: []schemacapture.TableDeclaration{{Table: schemamodel.Table{Name: "records", Schema: "main"}, Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", DefaultSet: true}}, Indexes: []schemamodel.Index{{Name: "by_id", Fields: []string{"id"}}}, Triggers: []schemamodel.Trigger{{Name: "audit", Table: "records", Body: "SELECT 1"}}}}}
	wire, err := service.declarationRequest(request)
	c.Assert(err, qt.IsNil)
	c.Assert(wire.Tables, qt.HasLen, 1)
	c.Assert(wire.Tables[0].Declaration.Indexes[0].Name, qt.Equals, "by_id")
	c.Assert(wire.Tables[0].Declaration.Triggers[0].Body, qt.Equals, "SELECT 1")
	c.Assert(wire.Tables[0].Declaration.Columns[0].DefaultSet, qt.IsTrue)
	wire.Tables[0].Declaration.Indexes[0].Fields[0] = "changed"
	c.Assert(request.Tables[0].Indexes[0].Fields[0], qt.Equals, "id")
}

func TestAuthoredParentContextKeepsIndexesAndTriggers(t *testing.T) {
	c := qt.New(t)
	target := &target{host: &Host{}, engine: &protocol.Engine{Id: "fixture", DefaultSchema: "main"}}
	schema := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "records", Schema: "main", StructName: "Record"}}, Fields: []schemamodel.Field{{StructName: "Record", Name: "id", Type: "INTEGER"}}, Indexes: []schemamodel.Index{{TableName: "records", Name: "by_id", Fields: []string{"id"}}}, Triggers: []schemamodel.Trigger{{Table: "records", Name: "audit", Body: "SELECT 1"}}}
	creation, err := target.authoredCreationContext(renderer.SchemaRequest{Target: "fixture", Schema: schema, Identifiers: identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact}})
	c.Assert(err, qt.IsNil)
	c.Assert(creation.plan.Policy, qt.DeepEquals, featureplan.Policy{TableRebuildRequest: planext.DefaultTableRebuildRequest})
	c.Assert(creation.plan.Tables, qt.HasLen, 1)
	c.Assert(creation.plan.Tables[0].Current.HasTable(), qt.IsFalse)
	c.Assert(creation.plan.Tables[0].Desired.Indexes, qt.DeepEquals, schema.Indexes)
	c.Assert(creation.plan.Tables[0].Desired.Triggers, qt.DeepEquals, schema.Triggers)
	c.Assert(creation.names, qt.HasLen, 1)
	c.Assert(rejectUnclaimedChanges(creation.nodes), qt.IsNotNil)
}
