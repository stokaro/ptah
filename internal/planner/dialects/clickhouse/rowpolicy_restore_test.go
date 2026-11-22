package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
)

// heldOn is what the read of the database d1 records, under representation,
// about the policy vf on each of the views named, which it cannot describe: a
// coverage record naming each policy.
func heldOn(representation schemaext.Representation, views ...string) schemaext.Coverage {
	subjects := make([]schemaext.SubjectCoverage, 0, len(views))
	for _, view := range views {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", view, "vf"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: `the policy is on "` + view + `" (engine View)`}})
	}
	return must.Must(chschema.RowPolicyCoverage(representation, schemaext.Knowledge{State: schemaext.Complete}, subjects))
}

// vfOnV is the definition of vf on v the read keeps beside the record, named
// as the read names it, without the database.
func vfOnV() schemaext.Object {
	return must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "v", "vf"),
		chschema.ObservedRowPolicy{Filter: new("id > 100"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{All: true, Except: []string{"guest"}}}))
}

// vfOnW is the definition of a policy also named vf, on the view w, with a
// filter of its own.
func vfOnW() schemaext.Object {
	return must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "w", "vf"),
		chschema.ObservedRowPolicy{Filter: new("id < 10"), Composition: chschema.Permissive, Roles: chschema.RoleSelection{All: true}}))
}

// rollbackOfV is the feature context a rollback hands its planner when the
// database it restores held vf on v: the record on the desired side, and the
// definition the read kept.
func rollbackOfV() difftypes.FeatureContext {
	return difftypes.FeatureContext{DesiredCoverage: heldOn(schemaext.Desired, "v"), ContextObjects: must.Must(schemaext.NewObjects(vfOnV()))}
}

// TestGenerateMigrationAST_RecreatesAViewsRowPolicies plans the creation of
// views with a desired side that holds policies on them the model cannot
// describe, which only a rollback hands the planner. A view or materialized
// view created from nothing gets each of its policies back from the
// definition the read kept on that relation, ahead of every CREATE: a policy
// that fails then leaves no view showing the rows it hid. A view that takes
// over from a materialized view of the name kept the policy, and a forward
// plan, whose desired side holds no such policy, creates none.
func TestGenerateMigrationAST_RecreatesAViewsRowPolicies(t *testing.T) {
	onV := "CREATE ROW POLICY `vf` ON `d1`.`v` USING (id > 100) AS RESTRICTIVE TO ALL EXCEPT `guest`"
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a view the rollback creates",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}}, Features: rollbackOfV()},
			want: []string{onV, "CREATE VIEW `d1`.`v` AS"}},
		{name: "a materialized view the rollback creates",
			diff: &difftypes.SchemaDiff{MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}}, Features: rollbackOfV()},
			want: []string{onV, "CREATE MATERIALIZED VIEW `d1`.`v` ENGINE = MergeTree ORDER BY tuple() AS"}},
		{name: "two views, each holding a policy of the same name",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}, {Name: "d1.w", Body: "SELECT 1 AS id"}},
				Features: difftypes.FeatureContext{DesiredCoverage: heldOn(schemaext.Desired, "v", "w"), ContextObjects: must.Must(schemaext.NewObjects(vfOnV(), vfOnW()))}},
			want: []string{onV, "CREATE ROW POLICY `vf` ON `d1`.`w` USING (id < 10) AS PERMISSIVE TO ALL", "CREATE VIEW `d1`.`v` AS", "CREATE VIEW `d1`.`w` AS"}},
		{name: "a view that takes over from a materialized view",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}},
				MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "d1.v"}}, Features: rollbackOfV()},
			want: []string{"DROP VIEW IF EXISTS `d1`.`v`", "CREATE VIEW `d1`.`v` AS"}},
		{name: "a view a forward plan creates",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}},
				Features: difftypes.FeatureContext{CurrentCoverage: heldOn(schemaext.Observed, "v"), ContextObjects: must.Must(schemaext.NewObjects(vfOnV()))}},
			want: []string{"CREATE VIEW `d1`.`v` AS"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			heads := planHeads(c, test.diff)

			c.Assert(heads, qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_NamesARowPolicyItCannotRecreate plans a rollback
// that creates v again while the read kept no definition of the policy vf on
// it. The plan names vf and the reason in a comment rather than leaving it
// out.
func TestGenerateMigrationAST_NamesARowPolicyItCannotRecreate(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}},
		Features: difftypes.FeatureContext{DesiredCoverage: heldOn(schemaext.Desired, "v")}}

	nodes, err := clickhouse.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("clickhouse", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `-- CLICKHOUSE: row policy "vf" on "v" is not created again: the read kept no definition of it (the policy is on "v" (engine View))`)
	c.Assert(planHeads(c, diff), qt.DeepEquals, []string{"CREATE VIEW `d1`.`v` AS"})
}
