package providerhost

// White-box testing required: the private planner and AST lowering share a
// rendering entry point with native operations. Refusal must precede any RPC.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
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

// TestCommonTablePlannerIgnoresMatchedCounts plans a diff that changes nothing
// and keeps a table. The counts of what the comparison paired are for the
// drift report; a planner that read them as a change it cannot carry refused
// every plan that keeps a table.
func TestCommonTablePlannerIgnoresMatchedCounts(t *testing.T) {
	c := qt.New(t)
	selected := &target{engine: &protocol.Engine{Id: "fixture", Features: []string{provider.CommonTableIdentityFeature}}, host: &Host{description: &protocol.Description{}}}
	runtime, err := engine.New(engine.Provider{ID: "example.org/fixture", Targets: []engine.Target{selected.registration()}})
	c.Assert(err, qt.IsNil)
	planner := selected.registration().Planner(planext.Options{})
	nodes, err := planner.GenerateMigrationAST(c.Context(), runtime, &difftypes.SchemaDiff{Matched: difftypes.MatchedCounts{Tables: 1, Columns: 1}})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
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
