package diffreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/diffreport"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestCategoriesNamesEveryChangedObject(t *testing.T) {

	tests := []struct {
		name string
		diff *types.SchemaDiff
		want []diffreport.Category
	}{
		{
			name: "plain names",
			diff: &types.SchemaDiff{RLSEnabledTablesAdded: []string{"other.secured", "public.p"}},
			want: []diffreport.Category{
				{Name: "rls_enabled_tables_added", Objects: []string{"other.secured", "public.p"}},
			},
		},
		{
			name: "object references carry their qualifying context",
			diff: &types.SchemaDiff{
				IndexesAdded:  []types.IndexRef{{Name: "idx_users_email", TableName: "public.users"}},
				TriggersAdded: []types.TriggerRef{{TriggerName: "trg_audit", TableName: "public.users"}},
				GrantsAdded: []types.GrantRef{
					{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "other.granted"},
				},
			},
			want: []diffreport.Category{
				{Name: "indexes_added", Objects: []string{"idx_users_email public.users"}},
				{Name: "triggers_added", Objects: []string{"trg_audit public.users"}},
				{Name: "grants_added", Objects: []string{"app SELECT TABLE other.granted"}},
			},
		},
		{
			name: "per-object diffs are named by their object",
			diff: &types.SchemaDiff{
				TablesModified: []types.TableDiff{{TableName: "products", ColumnsAdded: []string{"price"}}},
				EnumsModified:  []types.EnumDiff{{EnumName: "status", ValuesAdded: []string{"archived"}}},
			},
			want: []diffreport.Category{
				{Name: "tables_modified", Objects: []string{"products"}},
				{Name: "enums_modified", Objects: []string{"status"}},
			},
		},
		{
			name: "categories are reported in diff field order",
			diff: &types.SchemaDiff{
				ConstraintsRemoved: []string{"uq_products_sku"},
				TablesAdded:        []string{"public.orders"},
			},
			want: []diffreport.Category{
				{Name: "tables_added", Objects: []string{"public.orders"}},
				{Name: "constraints_removed", Objects: []string{"uq_products_sku"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(diffreport.Categories(test.diff), qt.DeepEquals, test.want)
		})
	}
}

// TestCategoriesIgnoresNonChangeFields pins the other half of the reflection
// rule: a diff carries fields that describe how it was produced, and reporting
// those as differences would make a synced schema look modified.
func TestCategoriesIgnoresNonChangeFields(t *testing.T) {

	tests := []struct {
		name string
		diff *types.SchemaDiff
	}{
		{name: "empty diff", diff: &types.SchemaDiff{}},
		{
			name: "identifier semantics only",
			diff: &types.SchemaDiff{IdentifierSemantics: &identifier.Semantics{}},
		},
		{name: "nil diff", diff: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(diffreport.Categories(test.diff), qt.HasLen, 0)
		})
	}
}

func TestCategoryCountAndNames(t *testing.T) {
	c := qt.New(t)

	categories := diffreport.Categories(&types.SchemaDiff{
		TablesAdded:           []string{"public.orders", "public.order_items"},
		RLSEnabledTablesAdded: []string{"other.secured"},
	})

	c.Assert(categories, qt.HasLen, 2)
	c.Assert(categories[0].Count(), qt.Equals, 2)
	c.Assert(categories[1].Count(), qt.Equals, 1)
	c.Assert(diffreport.Names(categories), qt.DeepEquals, []string{"tables_added", "rls_enabled_tables_added"})
}
