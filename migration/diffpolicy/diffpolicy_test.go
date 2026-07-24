package diffpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/diffpolicy"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestParseChangeKind(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    diffpolicy.ChangeKind
		wantErr bool
	}{
		{name: "drop_table", input: "drop_table", want: diffpolicy.DropTable},
		{name: "drop_column", input: "drop_column", want: diffpolicy.DropColumn},
		{name: "drop_index", input: "drop_index", want: diffpolicy.DropIndex},
		{name: "drop_enum", input: "drop_enum", want: diffpolicy.DropEnum},
		{name: "uppercase and spaces", input: "  DROP_TABLE ", want: diffpolicy.DropTable},
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
	c.Assert(empty.Has(diffpolicy.DropTable), qt.IsFalse)

	set := diffpolicy.NewSkipSet(diffpolicy.DropTable, diffpolicy.DropTable, diffpolicy.DropEnum)
	c.Assert(set.Empty(), qt.IsFalse)
	c.Assert(set.Has(diffpolicy.DropTable), qt.IsTrue)
	c.Assert(set.Has(diffpolicy.DropEnum), qt.IsTrue)
	c.Assert(set.Has(diffpolicy.DropColumn), qt.IsFalse)
}

func TestApplyNilOrEmptyReturnsInput(t *testing.T) {
	c := qt.New(t)

	got, skipped := diffpolicy.Apply(nil, diffpolicy.NewSkipSet(diffpolicy.DropTable))
	c.Assert(got, qt.IsNil)
	c.Assert(skipped, qt.HasLen, 0)

	diff := &types.SchemaDiff{TablesRemoved: []string{"users"}}
	got, skipped = diffpolicy.Apply(diff, nil)
	c.Assert(got, qt.Equals, diff)
	c.Assert(skipped, qt.HasLen, 0)
}

func TestApplyDropTableRemovesDependents(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesRemoved: []string{"users"},
		TablesModified: []types.TableDiff{
			{TableName: "orders", ColumnsRemoved: []string{"note"}},
		},
		IndexesRemoved: []string{"idx_users_email", "idx_orders_total"},
		IndexesRemovedWithTables: []types.IndexRemovalInfo{
			{Name: "idx_users_email", TableName: "users"},
			{Name: "idx_orders_total", TableName: "orders"},
		},
		ConstraintsRemoved: []string{"uq_users_email", "chk_orders_total"},
		ConstraintsRemovedWithTables: []types.ConstraintRemovalInfo{
			{Name: "uq_users_email", TableName: "users", Type: "UNIQUE"},
			{Name: "chk_orders_total", TableName: "orders", Type: "CHECK"},
		},
		TriggersRemoved: []types.TriggerRef{
			{TriggerName: "trg_users", TableName: "users"},
			{TriggerName: "trg_orders", TableName: "orders"},
		},
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "p_users", TableName: "users"},
			{PolicyName: "p_orders", TableName: "orders"},
		},
		RLSEnabledTablesRemoved: []string{"users", "orders"},
		GrantsRemoved: []types.GrantRef{
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
		},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropTable))

	// The dropped table and only its dependents are removed; the kept table's
	// removals (orders) and the schema-level grant survive.
	c.Assert(got.TablesRemoved, qt.HasLen, 0)
	c.Assert(got.IndexesRemoved, qt.DeepEquals, []string{"idx_orders_total"})
	c.Assert(got.IndexesRemovedWithTables, qt.HasLen, 1)
	c.Assert(got.IndexesRemovedWithTables[0].TableName, qt.Equals, "orders")
	c.Assert(got.ConstraintsRemoved, qt.DeepEquals, []string{"chk_orders_total"})
	c.Assert(got.ConstraintsRemovedWithTables, qt.HasLen, 1)
	c.Assert(got.TriggersRemoved, qt.HasLen, 1)
	c.Assert(got.TriggersRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(got.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(got.RLSPoliciesRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(got.RLSEnabledTablesRemoved, qt.DeepEquals, []string{"orders"})
	c.Assert(got.GrantsRemoved, qt.HasLen, 2)

	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(skipped[0].Kind, qt.Equals, diffpolicy.DropTable)
	c.Assert(skipped[0].Object, qt.Equals, "users")

	// The input diff must not be mutated.
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"users"})
	c.Assert(diff.GrantsRemoved, qt.HasLen, 3)
}

func TestApplyDropColumn(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{TableName: "users", ColumnsAdded: []string{"age"}, ColumnsRemoved: []string{"legacy", "old"}},
		},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropColumn))

	c.Assert(got.TablesModified[0].ColumnsRemoved, qt.HasLen, 0)
	c.Assert(got.TablesModified[0].ColumnsAdded, qt.DeepEquals, []string{"age"})
	c.Assert(skipped, qt.HasLen, 2)
	c.Assert(skipped[0].Object, qt.Equals, "users.legacy")
	c.Assert(skipped[1].Object, qt.Equals, "users.old")
	// Input not mutated.
	c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.DeepEquals, []string{"legacy", "old"})
}

func TestApplyDropIndexPreservesReplacements(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		// idx_rebuild is dropped and recreated (a replacement); idx_gone is a
		// genuine standalone removal.
		IndexesAdded:   []string{"idx_rebuild"},
		IndexesRemoved: []string{"idx_rebuild", "idx_gone"},
	}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex))

	c.Assert(got.IndexesRemoved, qt.DeepEquals, []string{"idx_rebuild"})
	c.Assert(skipped, qt.HasLen, 1)
	c.Assert(skipped[0].Kind, qt.Equals, diffpolicy.DropIndex)
	c.Assert(skipped[0].Object, qt.Equals, "idx_gone")
}

func TestApplyDropEnum(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{EnumsRemoved: []string{"status", "kind"}}

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropEnum))

	c.Assert(got.EnumsRemoved, qt.HasLen, 0)
	c.Assert(skipped, qt.HasLen, 2)
	c.Assert(diff.EnumsRemoved, qt.DeepEquals, []string{"status", "kind"})
}
