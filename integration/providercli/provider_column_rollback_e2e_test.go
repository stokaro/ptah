//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
)

// TestProviderRollbackRestoresADroppedColumnAsDeclared_HappyPath plans,
// through a provider that owns its tables, a change to main.users that drops
// column label, which the read holds with a retention setting. The rollback
// adds the column back, and an added column is a declaration: its setting is
// the declared form the pre-change declaration converted, not the observed
// form the read reported, which the provider's desired codec refuses. The
// forward diff keeps the observed form.
func TestProviderRollbackRestoresADroppedColumnAsDeclared_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned", "FIXTURE_TABLE_PROJECTIONS=1", "FIXTURE_SCHEMA_VALIDATION=1"})
	rules := comparisonRules()
	column := objectidentity.NewBuilder(rules).ColumnParts("main", "users", "label")
	read, err := schemaext.NewFacets(comparisonModel(c, runtime, schemaext.Observed, comparisonFacet, column, `{"seconds":90}`))
	c.Assert(err, qt.IsNil)
	table := catalog.Table{Name: "users", Schema: "main", Columns: []catalog.Column{
		{Name: "id", DataType: "INTEGER", IsPrimaryKey: true, IsNullable: "NO"},
		{Name: "label", DataType: "TEXT", IsNullable: "YES", Facets: read},
	}}
	declared := schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "users", Schema: "main"},
		Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER", Primary: true}}}
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesModified: []difftypes.TableDiff{{TableName: "main.users",
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "label", Type: "TEXT", Nullable: true, Facets: read}},
		Desired:        declared, Current: schemacapture.TableObservation{Table: table}}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff,
		DesiredSchema: &schemamodel.Database{}, CurrentSchema: &catalog.Database{Tables: []catalog.Table{table}}, Dialect: "fixture",
		Capabilities: unwrappedOperationTarget()})

	c.Assert(err, qt.IsNil)
	restored := plan.Reverse.Diff.TablesModified[0].ColumnsAdded
	c.Assert(restored, qt.HasLen, 1)
	desired, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, restored[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(desired, qt.HasLen, 1)
	c.Assert(plan.Reverse.Nodes, qt.HasLen, 1)
	operations := ast.OwnerOperations(plan.Reverse.Nodes[0])
	c.Assert(operations, qt.HasLen, 1)
	c.Assert(operations[0].Kind(), qt.Equals, schemaext.Kind("example.org/fixture/table-operation"))
	observed, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Observed, diff.TablesModified[0].ColumnsRemoved[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.HasLen, 1)
}
