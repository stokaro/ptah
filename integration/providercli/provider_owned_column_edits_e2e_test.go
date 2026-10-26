//go:build integration

package providercli_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

func ownedColumnEdit(name string) *difftypes.SchemaDiff {
	result := ownedColumnsDiff(nil, nil)
	table := &result.TablesModified[0]
	table.TableName = "main." + name
	table.Desired.Table.Name = name
	table.Current.Table.Name = name
	field := schemamodel.Field{Name: "value", Type: "TEXT", Nullable: true}
	table.Desired.Fields = []schemamodel.Field{field}
	table.Current.Table.Columns = []catalog.Column{{Name: "value", DataType: "INTEGER", IsNullable: "YES"}}
	table.ColumnsModified = []difftypes.ColumnDiff{{ColumnName: "value", Desired: field, Changes: map[string]string{"type": "INTEGER -> TEXT"}}}
	return result
}

func TestProviderOwnedColumnEditsReachParentAssessment(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned-edit"})
	for _, row := range []struct {
		name string
		want []string
	}{
		{"edited", []string{`ALTER TABLE "edited" RENAME TO "accepted";`}},
		{"noop", nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, ownedColumnEdit(row.name), "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, len(row.want))
			rendered, err := runtime.Render(c.Context(), renderer.Request{Target: "fixture", Capabilities: unwrappedOperationTarget(), Nodes: nodes})
			c.Assert(err, qt.IsNil)
			sql, err := rendered.SQLFragments()
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(sql, "\n"), qt.Equals, strings.Join(row.want, "\n"))
		})
	}
}

func TestProviderOwnedColumnEditRefusalsDiscardPlan(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=owned-edit"})
	for _, name := range []string{"refused", "omitted"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffASTWithOptions(c.Context(), runtime, ownedColumnEdit(name), "fixture", planext.Options{Capabilities: unwrappedOperationTarget()})
			c.Assert(err, qt.IsNotNil)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
