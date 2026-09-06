package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestApplyDiffPolicy_SkipDropTableFiltersOnlyDroppedTableRemovals(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"old_users"},
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

	got := atlasschema.ApplyDiffPolicy(diff, atlasschema.DiffPolicy{SkipDropTable: true})

	c.Assert(got.TablesRemoved, qt.IsNil)
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
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"old_users"})
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
