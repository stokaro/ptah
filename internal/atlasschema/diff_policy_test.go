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

func TestDiffPolicy_AllowsTableRebuild(t *testing.T) {
	tests := []struct {
		name    string
		policy  atlasschema.DiffPolicy
		dialect string
		want    bool
	}{
		{name: "asked for every dialect", policy: atlasschema.DiffPolicy{AllowTableRebuild: true}, dialect: platform.ClickHouse, want: true},
		{name: "asked for the dialect by name", policy: atlasschema.DiffPolicy{TableRebuildDialects: []string{platform.ClickHouse}}, dialect: platform.ClickHouse, want: true},
		{name: "asked for another dialect only", policy: atlasschema.DiffPolicy{TableRebuildDialects: []string{platform.ClickHouse}}, dialect: platform.Postgres},
		{name: "asked for nothing", policy: atlasschema.DiffPolicy{}, dialect: platform.ClickHouse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.policy.AllowsTableRebuild(test.dialect), qt.Equals, test.want)
		})
	}
}

func TestDiffPolicy_ForDialect_HappyPath(t *testing.T) {
	labeled := atlasschema.DialectDiffPolicy{
		SkipChangeKinds: []planext.ChangeKind{planext.DropTable}, ConcurrentIndexDrop: true, Blocks: []string{"concurrent_index", "skip"},
	}
	unlabeled := atlasschema.DiffPolicy{
		SkipChangeKinds: []planext.ChangeKind{planext.DropColumn}, ConcurrentIndexCreate: true, TableRebuildDialects: []string{platform.SQLite},
	}
	tests := []struct {
		name    string
		policy  atlasschema.DiffPolicy
		dialect string
		want    atlasschema.DiffPolicy
	}{
		{
			name:    "the labeled block's policy for its dialect",
			policy:  withDialects(unlabeled, map[string]atlasschema.DialectDiffPolicy{platform.SQLite: labeled}),
			dialect: platform.SQLite,
			want: atlasschema.DiffPolicy{
				SkipChangeKinds: []planext.ChangeKind{planext.DropTable}, ConcurrentIndexDrop: true, TableRebuildDialects: []string{platform.SQLite},
			},
		},
		{name: "the unlabeled policy where no block is labeled", policy: unlabeled, dialect: platform.SQLite, want: unlabeled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := test.policy.ForDialect(test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			again, err := got.ForDialect(test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, test.want)
		})
	}
}

// A policy block labeled for another dialect is refused, since the community
// binary applies it to every dialect, even beside a block for the plan's own.
func TestDiffPolicy_ForDialect_FailurePath(t *testing.T) {
	skip := atlasschema.DialectDiffPolicy{SkipChangeKinds: []planext.ChangeKind{planext.DropTable}, Blocks: []string{"skip"}}
	both := atlasschema.DialectDiffPolicy{Blocks: []string{"concurrent_index", "skip"}}
	tests := []struct {
		name     string
		dialects map[string]atlasschema.DialectDiffPolicy
		wantErr  string
	}{
		{
			name:     "a block for another dialect",
			dialects: map[string]atlasschema.DialectDiffPolicy{platform.Postgres: skip},
			wantErr: `a labeled diff block holds a policy for another dialect: the diff "postgres" block of the project file holds a skip block, ` +
				`which Ptah applies only to a plan for postgres, and this plan is for "sqlite"\. ` +
				`The community binary ignores the label and applies the block to every dialect\. ` +
				`Move the block into an unlabeled diff block to apply it to every dialect, or into the env of the postgres database`,
		},
		{
			name:     "a block for the plan's dialect and one for another",
			dialects: map[string]atlasschema.DialectDiffPolicy{platform.SQLite: skip, platform.ClickHouse: both},
			wantErr:  `.*the diff "clickhouse" block of the project file holds a concurrent_index block and a skip block, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy := withDialects(atlasschema.DiffPolicy{}, test.dialects)

			got, err := policy.ForDialect(platform.SQLite)

			c.Assert(err, qt.ErrorIs, atlasschema.ErrDiffPolicyForAnotherDialect)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, atlasschema.DiffPolicy{})
		})
	}
}

func withDialects(policy atlasschema.DiffPolicy, dialects map[string]atlasschema.DialectDiffPolicy) atlasschema.DiffPolicy {
	policy.Dialects = dialects
	return policy
}
