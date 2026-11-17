package providerhost

// White-box testing required: the private planner and AST lowering share a
// rendering entry point with native operations. Refusal must precede any RPC.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func TestNativeTargetRefusesCommonSchemaServices(t *testing.T) {
	c := qt.New(t)
	selected := &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{description: &protocol.Description{}}}
	runtime, err := engine.New(engine.Provider{ID: "example.org/fixture", Targets: []engine.Target{selected.registration()}})
	c.Assert(err, qt.IsNil)
	_, err = runtime.RenderSchema(c.Context(), renderer.SchemaRequest{Target: "fixture"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = runtime.ValidateSchema(c.Context(), schemavalidation.Request{Target: "fixture"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = runtime.PrepareTables(c.Context(), schemapreparation.Request{Target: "fixture"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = runtime.ProjectTableCreations(c.Context(), schemaprojection.TableCreationRequest{Target: "fixture"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

func TestCommonTableRenderingRequiresEngineContract(t *testing.T) {
	for _, row := range []struct {
		name string
		node ast.Node
	}{
		{"create", &ast.CreateTableNode{Name: "items"}},
		{"drop", &ast.DropTableNode{Name: "items"}},
		{"add column", &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "id", Type: "INTEGER"}}}}},
		{"drop column", &ast.AlterTableNode{Name: "items", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "id"}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			selected := &target{engine: &protocol.Engine{Id: "fixture"}}
			result, err := selected.Render(c.Context(), renderer.Request{Target: "fixture", Nodes: []ast.Node{row.node}})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*"+provider.CommonTableIdentityFeature+".*")
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(result.Fragments, qt.HasLen, 0)
			selected.engine.Features = []string{provider.CommonTableIdentityFeature}
			change, err := selected.change(row.node)
			c.Assert(err, qt.IsNil)
			c.Assert(change.GetObject().GetName(), qt.Equals, "items")
		})
	}
}

func TestCommonTablePlannerRequiresEngineContract(t *testing.T) {
	for _, row := range []struct {
		name string
		diff difftypes.SchemaDiff
	}{
		{"create", difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{}}}},
		{"drop", difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{}}}},
		{"alter", difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			selected := &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{description: &protocol.Description{}}}
			planner := selected.registration().Planner(planext.Options{})
			nodes, err := planner.GenerateMigrationAST(c.Context(), nil, &row.diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*"+provider.CommonTableIdentityFeature+".*")
			c.Assert(nodes, qt.HasLen, 0)
		})
	}
}

// TestCommonTablePlannerIgnoresCounts plans a diff that changes nothing and
// says how much each side holds. The counts of what the comparison paired and
// what an owner compared are for the drift report; a planner that read them as
// a change it cannot carry would refuse every plan that keeps a table or that a
// provider's comparison counted.
func TestCommonTablePlannerIgnoresCounts(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "the pairs of the comparison", diff: &difftypes.SchemaDiff{Matched: difftypes.MatchedCounts{Tables: 1, Columns: 1}}},
		{name: "the counts of an owner", diff: &difftypes.SchemaDiff{FeatureCounts: []schemaext.ObjectCount{{Kind: "example.org/fixture/change", Matched: 1}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := &target{engine: &protocol.Engine{Id: "fixture", Features: []string{provider.CommonTableIdentityFeature}}, host: &Host{description: &protocol.Description{}}}
			runtime, err := engine.New(engine.Provider{ID: "example.org/fixture", Targets: []engine.Target{selected.registration()}})
			c.Assert(err, qt.IsNil)
			planner := selected.registration().Planner(planext.Options{})

			nodes, err := planner.GenerateMigrationAST(c.Context(), runtime, test.diff)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 0)
		})
	}
}

func commonTableCoverageFixture(c *qt.C) (*target, *engine.Runtime, *difftypes.SchemaDiff) {
	c.Helper()
	selected := &target{engine: &protocol.Engine{Id: "fixture", Features: []string{provider.CommonTableIdentityFeature}}, host: &Host{description: &protocol.Description{}}}
	runtime, err := engine.New(engine.Provider{ID: "example.org/fixture", Targets: []engine.Target{selected.registration()}})
	c.Assert(err, qt.IsNil)
	limits := coverage.Set{}.With(coverage.Refused(coverage.ObjectOwner)).With(coverage.Refused(coverage.RoleMembership))
	diff := &difftypes.SchemaDiff{CurrentNotDescribed: limits,
		TablesAdded: difftypes.TableChanges{{Name: "items", Table: schemamodel.Table{Name: "items"}, Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER"}}}}}
	return selected, runtime, diff
}

func TestCommonTablePlannerRetainsUnknownAccess(t *testing.T) {
	c := qt.New(t)
	selected, runtime, diff := commonTableCoverageFixture(c)
	limits := diff.CurrentNotDescribed
	nodes, err := selected.registration().Planner(planext.Options{}).GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(nodes[0].(*ast.CreateTableNode).Name, qt.Equals, "items")
	c.Assert(diff.CurrentNotDescribed, qt.DeepEquals, limits)
}

// TestCommonTablePlannerIgnoresTheConnectedAccount plans a table creation
// from a diff that records the account the read ran as. Every comparison
// against a database records it, whether or not the read could say who it
// was, so a planner that read it as a change would refuse every provider plan
// made from a read.
func TestCommonTablePlannerIgnoresTheConnectedAccount(t *testing.T) {
	for _, test := range []struct {
		name    string
		account string
	}{
		{name: "a recorded account", account: "app@%"},
		{name: "an account the read did not record", account: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected, runtime, diff := commonTableCoverageFixture(c)
			diff.ConnectedAccount = new(test.account)

			nodes, err := selected.registration().Planner(planext.Options{}).GenerateMigrationAST(c.Context(), runtime, diff)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			c.Assert(nodes[0].(*ast.CreateTableNode).Name, qt.Equals, "items")
		})
	}
}

// TestCommonTablePlannerCurrentOwners_FailurePath plans a diff that carries
// the owners a read reported. The wire has no form for an owner a plan gives
// back, so the planner refuses it rather than plan a rebuild that hands a view
// to the role that runs it. The comparison fills the field only for a target
// whose descriptor manages owners, so a provider of an engine Ptah does not
// build in never reaches this refusal; one that serves a built-in name whose
// descriptor manages them is refused here until the wire carries owners.
func TestCommonTablePlannerCurrentOwners_FailurePath(t *testing.T) {
	c := qt.New(t)
	selected, runtime, diff := commonTableCoverageFixture(c)
	diff.CurrentOwners = []difftypes.OwnerRef{{Kind: "VIEW", Name: "recent", Owner: "app"}}

	nodes, err := selected.registration().Planner(planext.Options{}).GenerateMigrationAST(c.Context(), runtime, diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.CurrentOwners`)
	c.Assert(nodes, qt.HasLen, 0)
}

func TestUnknownAccessDoesNotEnableCommonMutations(t *testing.T) {
	for _, test := range []struct {
		name   string
		roles  difftypes.RoleChanges
		grants []difftypes.GrantRef
		want   string
	}{
		{"role mutation", difftypes.RoleChanges{{Name: "reader"}}, nil, "RolesAdded"},
		{"grant mutation", nil, []difftypes.GrantRef{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "items"}}, "GrantsAdded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected, runtime, diff := commonTableCoverageFixture(c)
			limits := diff.CurrentNotDescribed
			diff.RolesAdded, diff.GrantsAdded = test.roles, test.grants
			nodes, err := selected.registration().Planner(planext.Options{}).GenerateMigrationAST(c.Context(), runtime, diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, ".*SchemaDiff\\."+test.want)
			c.Assert(nodes, qt.IsNil)
			c.Assert(diff.CurrentNotDescribed, qt.DeepEquals, limits)
		})
	}
}
