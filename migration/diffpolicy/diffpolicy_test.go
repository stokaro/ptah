package diffpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/diffpolicy"
)

func TestParseChangeKind(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    planext.ChangeKind
		wantErr bool
	}{
		{name: "drop_table", input: "drop_table", want: planext.DropTable},
		{name: "drop_column", input: "drop_column", want: planext.DropColumn},
		{name: "drop_index", input: "drop_index", want: planext.DropIndex},
		{name: "drop_enum", input: "drop_enum", want: planext.DropEnum},
		{name: "uppercase and spaces", input: "  DROP_TABLE ", want: planext.DropTable},
		{name: "unknown", input: "drop_universe", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := diffpolicy.ParseChangeKind(tt.input)
			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestSkipSet(t *testing.T) {
	c := qt.New(t)

	empty := diffpolicy.NewSkipSet()
	c.Assert(empty.Empty(), qt.IsTrue)
	c.Assert(empty.Has(planext.DropTable), qt.IsFalse)

	set := diffpolicy.NewSkipSet(planext.DropTable, planext.DropTable, planext.DropEnum)
	c.Assert(set.Empty(), qt.IsFalse)
	c.Assert(set.Has(planext.DropTable), qt.IsTrue)
	c.Assert(set.Has(planext.DropEnum), qt.IsTrue)
	c.Assert(set.Has(planext.DropColumn), qt.IsFalse)
}

func TestApplyNilOrEmptyReturnsInput(t *testing.T) {
	c := qt.New(t)

	got, skipped := diffpolicy.Apply(nil, diffpolicy.NewSkipSet(planext.DropTable))
	c.Assert(got, qt.IsNil)
	c.Assert(skipped, qt.HasLen, 0)

	diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "users"}}}
	got, skipped = diffpolicy.Apply(diff, nil)
	c.Assert(got, qt.Equals, diff)
	c.Assert(skipped, qt.HasLen, 0)
}

func TestApplyDropTableRemovesDependents(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "users"}},
		TablesModified: []difftypes.TableDiff{
			{TableName: "orders", ColumnsRemoved: difftypes.ColumnChanges{{Name: "note"}}},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
			{Name: "idx_orders_total", TableName: "orders"},
		},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{
			{Name: "uq_users_email", TableName: "users", Type: "UNIQUE"},
			{Name: "chk_orders_total", TableName: "orders", Type: "CHECK"},
		},
		TriggersRemoved: []difftypes.TriggerRef{
			{TriggerName: "trg_users", TableName: "users"},
			{TriggerName: "trg_orders", TableName: "orders"},
		},
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{
			{PolicyName: "p_users", TableName: "users"},
			{PolicyName: "p_orders", TableName: "orders"},
		},
		RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "users"}, {Table: "orders"}},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
		},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(planext.DropTable))

	// The dropped table and only its dependents are removed; the kept table's
	// removals (orders) and the schema-level grant survive.
	c.Assert(got.TablesRemoved.Names(), qt.HasLen, 0)
	c.Assert(got.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_orders_total", TableName: "orders"},
	})
	c.Assert(got.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"chk_orders_total"})
	c.Assert(got.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(got.TriggersRemoved, qt.HasLen, 1)
	c.Assert(got.TriggersRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(got.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(got.RLSPoliciesRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(got.RLSEnabledTablesRemoved.Names(), qt.DeepEquals, []string{"orders"})
	c.Assert(got.GrantsRemoved, qt.HasLen, 2)

	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(skipped[0].Kind, qt.Equals, planext.DropTable)
	c.Assert(skipped[0].Object, qt.Equals, "users")

	// The input diff must not be mutated.
	c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"users"})
	c.Assert(diff.GrantsRemoved, qt.HasLen, 3)
}

func TestApplyDropColumn(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{
			{TableName: "users", ColumnsAdded: difftypes.ColumnChanges{{Name: "age"}}, ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy"}, {Name: "old"}}},
		},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(planext.DropColumn))

	c.Assert(got.TablesModified[0].ColumnsRemoved, qt.HasLen, 0)
	c.Assert(got.TablesModified[0].ColumnsAdded.Names(), qt.DeepEquals, []string{"age"})
	c.Assert(skipped, qt.HasLen, 2)
	c.Assert(skipped[0].Object, qt.Equals, "users.legacy")
	c.Assert(skipped[1].Object, qt.Equals, "users.old")
	// Input not mutated.
	c.Assert(diff.TablesModified[0].ColumnsRemoved.Names(), qt.DeepEquals, []string{"legacy", "old"})
}

func TestApplyDropIndexPreservesReplacements(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		// idx_rebuild is dropped and recreated (a replacement); idx_gone is a
		// genuine standalone removal.
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: "idx_rebuild", Fields: []string{"email"}}, TableName: "users"},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_rebuild", TableName: "users"},
			{Name: "idx_gone", TableName: "legacy"},
		},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(planext.DropIndex))

	c.Assert(got.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_rebuild", TableName: "users"},
	})
	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(skipped[0].Kind, qt.Equals, planext.DropIndex)
	c.Assert(skipped[0].Object, qt.Equals, "legacy.idx_gone")
}

func TestApplyDropEnum(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{EnumsRemoved: difftypes.EnumChanges{{Name: "status"}, {Name: "kind"}}}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(planext.DropEnum))

	c.Assert(got.EnumsRemoved.Names(), qt.HasLen, 0)
	c.Assert(skipped, qt.HasLen, 2)
	c.Assert(diff.EnumsRemoved.Names(), qt.DeepEquals, []string{"status", "kind"})
}
