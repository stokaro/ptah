package atlasschema_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/diffpolicy"
)

// skipping is a diff policy that skips kinds and nothing else.
func skipping(kinds ...planext.ChangeKind) atlasschema.DiffPolicy {
	return atlasschema.DiffPolicy{SkipChangeKinds: kinds}
}

// everyKindRemoved is a diff holding one removal of each kind the policy can
// skip: a table, a column of a kept table, a standalone index and an enum.
func everyKindRemoved() *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{
		TablesRemoved:  difftypes.TableRemovals{{Name: "legacy"}},
		TablesModified: []difftypes.TableDiff{{TableName: "t", ColumnsRemoved: difftypes.ColumnChanges{{Name: "b"}}}},
		EnumsRemoved:   difftypes.EnumChanges{{Name: "status"}},
	}
	diff.SetIndexRemovals([]difftypes.IndexRef{{Name: "t_a", TableName: "t"}})
	return diff
}

// removals names what diff still removes, one entry per change, so a row can
// state the whole remaining plan as data.
func removals(diff *difftypes.SchemaDiff) []string {
	var names []string
	for _, name := range diff.TablesRemoved.Names() {
		names = append(names, "table "+name)
	}
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			names = append(names, "column "+table.TableName+"."+column.Name)
		}
	}
	for _, ref := range diff.IndexRemovals() {
		names = append(names, "index "+ref.TableName+"."+ref.Name)
	}
	for _, name := range diff.EnumsRemoved.Names() {
		names = append(names, "enum "+name)
	}
	return names
}

// TestApplyDiffPolicy_OmitsEveryKindThePolicyNames pins that each kind the
// project file can name reaches the filter. A policy that carried the table
// drop alone would leave the column, index and enum rows planning the drop the
// project asked to keep.
func TestApplyDiffPolicy_OmitsEveryKindThePolicyNames(t *testing.T) {
	tests := []struct {
		name        string
		kinds       []planext.ChangeKind
		wantOmitted []diffpolicy.SkippedChange
		wantLeft    []string
	}{
		{
			name:     "nothing skipped",
			wantLeft: []string{"table legacy", "column t.b", "index t.t_a", "enum status"},
		},
		{
			name:        "drop_table",
			kinds:       []planext.ChangeKind{planext.DropTable},
			wantOmitted: []diffpolicy.SkippedChange{{Kind: planext.DropTable, Object: "legacy"}},
			wantLeft:    []string{"column t.b", "index t.t_a", "enum status"},
		},
		{
			name:        "drop_column",
			kinds:       []planext.ChangeKind{planext.DropColumn},
			wantOmitted: []diffpolicy.SkippedChange{{Kind: planext.DropColumn, Object: "t.b"}},
			wantLeft:    []string{"table legacy", "index t.t_a", "enum status"},
		},
		{
			name:        "drop_index",
			kinds:       []planext.ChangeKind{planext.DropIndex},
			wantOmitted: []diffpolicy.SkippedChange{{Kind: planext.DropIndex, Object: "t.t_a"}},
			wantLeft:    []string{"table legacy", "column t.b", "enum status"},
		},
		{
			name:        "drop_enum",
			kinds:       []planext.ChangeKind{planext.DropEnum},
			wantOmitted: []diffpolicy.SkippedChange{{Kind: planext.DropEnum, Object: "status"}},
			wantLeft:    []string{"table legacy", "column t.b", "index t.t_a"},
		},
		{
			name:  "every kind",
			kinds: diffpolicy.AllChangeKinds(),
			wantOmitted: []diffpolicy.SkippedChange{
				{Kind: planext.DropTable, Object: "legacy"},
				{Kind: planext.DropColumn, Object: "t.b"},
				{Kind: planext.DropIndex, Object: "t.t_a"},
				{Kind: planext.DropEnum, Object: "status"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff := everyKindRemoved()

			got, omitted := atlasschema.ApplyDiffPolicy(diff, skipping(tt.kinds...), platform.Postgres)

			c.Assert(omitted, qt.DeepEquals, tt.wantOmitted)
			c.Assert(removals(got), qt.DeepEquals, tt.wantLeft)
			c.Assert(removals(diff), qt.DeepEquals, []string{"table legacy", "column t.b", "index t.t_a", "enum status"})
		})
	}
}

// TestDiffPolicy_CompareOptions_HappyPath pins which removals the comparison
// is told the policy deletes. The SQLite virtual-table guard reads the three
// kinds a SQLite diff can carry; an enum drop is not one of them.
func TestDiffPolicy_CompareOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name                                 string
		kinds                                []planext.ChangeKind
		wantTables, wantColumns, wantIndexes bool
	}{
		{name: "nothing skipped"},
		{name: "drop_table", kinds: []planext.ChangeKind{planext.DropTable}, wantTables: true},
		{name: "drop_column", kinds: []planext.ChangeKind{planext.DropColumn}, wantColumns: true},
		{name: "drop_index", kinds: []planext.ChangeKind{planext.DropIndex}, wantIndexes: true},
		{name: "drop_enum", kinds: []planext.ChangeKind{planext.DropEnum}},
		{name: "every kind", kinds: diffpolicy.AllChangeKinds(), wantTables: true, wantColumns: true, wantIndexes: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := skipping(tt.kinds...).CompareOptions()

			c.Assert(got.SkipTableDrops, qt.Equals, tt.wantTables)
			c.Assert(got.SkipColumnDrops, qt.Equals, tt.wantColumns)
			c.Assert(got.SkipIndexDrops, qt.Equals, tt.wantIndexes)
		})
	}
}

// TestReportOmitted_HappyPath pins the line written for each change the policy
// left out: the plan comment's words, without the comment marker.
func TestReportOmitted_HappyPath(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	atlasschema.ReportOmitted(&out, []diffpolicy.SkippedChange{
		{Kind: planext.DropColumn, Object: "t.b"},
		{Kind: planext.DropIndex, Object: "t.t_a"},
	})

	c.Assert(out.String(), qt.Equals, "SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n"+
		"SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n")
}

func TestApplyDiffPolicy_SkipDropTableFiltersOnlyDroppedTableRemovals(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "old_users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "old_users_email_idx", TableName: "old_users"},
			{Name: "posts_title_idx", TableName: "posts"},
		},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{
			{Name: "old_users_account_fk", TableName: "old_users", Type: "FOREIGN KEY"},
			{Name: "posts_author_fk", TableName: "posts", Type: "FOREIGN KEY"},
		},
		TriggersRemoved: []difftypes.TriggerRef{
			{TriggerName: "old_users_audit", TableName: "old_users"},
			{TriggerName: "posts_audit", TableName: "posts"},
		},
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{
			{PolicyName: "old_users_policy", TableName: "old_users"},
			{PolicyName: "posts_policy", TableName: "posts"},
		},
		RLSEnabledTablesRemoved: difftypes.RLSEnabledTableChanges{{Table: "old_users"}, {Table: "posts"}},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "old_users"},
			{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "posts"},
			{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "old_users"},
		},
	}

	got, omitted := atlasschema.ApplyDiffPolicy(diff, skipping(planext.DropTable), platform.Postgres)

	c.Assert(omitted, qt.DeepEquals, []diffpolicy.SkippedChange{{Kind: planext.DropTable, Object: "old_users"}})
	c.Assert(got.TablesRemoved.Names(), qt.IsNil)
	c.Assert(got.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "posts_title_idx", TableName: "posts"},
	})
	c.Assert(got.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"posts_author_fk"})
	c.Assert(got.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{
		{Name: "posts_author_fk", TableName: "posts", Type: "FOREIGN KEY"},
	})
	c.Assert(got.TriggersRemoved, qt.DeepEquals, []difftypes.TriggerRef{
		{TriggerName: "posts_audit", TableName: "posts"},
	})
	c.Assert(got.RLSPoliciesRemoved, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "posts_policy", TableName: "posts"},
	})
	c.Assert(got.RLSEnabledTablesRemoved.Names(), qt.DeepEquals, []string{"posts"})
	c.Assert(got.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "posts"},
		{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "old_users"},
	})
	c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"old_users"})
	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "old_users_email_idx", TableName: "old_users"},
		{Name: "posts_title_idx", TableName: "posts"},
	})
	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"old_users_account_fk", "posts_author_fk"})
	c.Assert(diff.TriggersRemoved, qt.DeepEquals, []difftypes.TriggerRef{
		{TriggerName: "old_users_audit", TableName: "old_users"},
		{TriggerName: "posts_audit", TableName: "posts"},
	})
	c.Assert(diff.RLSPoliciesRemoved, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "old_users_policy", TableName: "old_users"},
		{PolicyName: "posts_policy", TableName: "posts"},
	})
	c.Assert(diff.RLSEnabledTablesRemoved.Names(), qt.DeepEquals, []string{"old_users", "posts"})
	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "old_users"},
		{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "posts"},
		{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "old_users"},
	})
}
