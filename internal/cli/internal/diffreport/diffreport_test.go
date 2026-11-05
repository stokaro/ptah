package diffreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/cli/internal/diffreport"
)

func TestCategoriesNamesEveryChangedObject(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []diffreport.Category
	}{
		{
			name: "plain names",
			diff: &difftypes.SchemaDiff{RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "other.secured"}, {Table: "public.p"}}},
			want: []diffreport.Category{
				{Name: "rls_enabled_tables_added", Objects: []string{"other.secured", "public.p"}},
			},
		},
		{
			name: "object references carry their qualifying context",
			diff: &difftypes.SchemaDiff{
				IndexesAdded:  difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_email", Fields: []string{"email"}}, TableName: "public.users"}},
				TriggersAdded: []difftypes.TriggerRef{{TriggerName: "trg_audit", TableName: "public.users"}},
				GrantsAdded: []difftypes.GrantRef{
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
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{TableName: "products", ColumnsAdded: difftypes.ColumnChanges{{Name: "price"}}}},
				EnumsModified:  []difftypes.EnumDiff{{EnumName: "status", ValuesAdded: []string{"archived"}}},
			},
			want: []diffreport.Category{
				{Name: "tables_modified", Objects: []string{"products"}},
				{Name: "enums_modified", Objects: []string{"status"}},
			},
		},
		{
			name: "a role is named by its name alone, its password left out",
			diff: &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "app", Password: "pw-SENTINEL", Login: true}}},
			want: []diffreport.Category{
				{Name: "roles_added", Objects: []string{"app"}},
			},
		},
		{
			name: "categories are reported in diff field order",
			diff: &difftypes.SchemaDiff{
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "uq_products_sku"}},
				TablesAdded:        difftypes.TableChanges{{Name: "public.orders"}},
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
		diff *difftypes.SchemaDiff
	}{
		{name: "empty diff", diff: &difftypes.SchemaDiff{}},
		{
			name: "identifier semantics only",
			diff: &difftypes.SchemaDiff{IdentifierSemantics: &identifier.Semantics{}},
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

	categories := diffreport.Categories(&difftypes.SchemaDiff{
		TablesAdded:           difftypes.TableChanges{{Name: "public.orders"}, {Name: "public.order_items"}},
		RLSEnabledTablesAdded: difftypes.RLSEnabledTableChanges{{Table: "other.secured"}},
	})

	c.Assert(categories, qt.HasLen, 2)
	c.Assert(categories[0].Count(), qt.Equals, 2)
	c.Assert(categories[1].Count(), qt.Equals, 1)
	c.Assert(diffreport.Names(categories), qt.DeepEquals, []string{"tables_added", "rls_enabled_tables_added"})
}

// The old and new values of a changed column reach the report under the table, in
// the order of the kinds' names whatever order a map hands them in. A comment and a
// constraint name are not in that map, and an empty one still reads as a value.
func TestCategories_ListsTheValuesOfAChangedColumn_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{
		{
			TableName: "app.t",
			ColumnsModified: []difftypes.ColumnDiff{
				{ColumnName: "k", Changes: map[string]string{"type": "int8 -> int4", "default": "1 -> 2", "nullable": "false -> true"}},
				{ColumnName: "note", CommentChange: &difftypes.CommentChange{Desired: "new"}},
				{ColumnName: "n", NotNullConstraintNameChange: &difftypes.NotNullConstraintNameChange{Current: "a", Desired: "b"}},
				{ColumnName: "nothing"},
			},
		},
		{TableName: "added_only", ColumnsAdded: difftypes.ColumnChanges{{Name: "x"}}},
	}}

	categories := diffreport.Categories(diff)

	c.Assert(categories, qt.HasLen, 1)
	c.Assert(categories[0].Name, qt.Equals, "tables_modified")
	c.Assert(categories[0].Objects, qt.DeepEquals, []string{"app.t", "added_only"})
	c.Assert(categories[0].Values, qt.DeepEquals, []string{
		"app.t.k: default: 1 -> 2, nullable: false -> true, type: int8 -> int4",
		`app.t.note: comment: "" -> "new"`,
		`app.t.n: not_null_constraint_name: "a" -> "b"`,
	})
}

// A category that is not a list of modified tables has no column values.
func TestCategories_ListsNoValuesOutsideModifiedTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "fresh"}}}

	categories := diffreport.Categories(diff)

	c.Assert(categories, qt.HasLen, 1)
	c.Assert(categories[0].Values, qt.IsNil)
}
