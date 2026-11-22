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

// heldOnV is what the read of the database d1 records about the policy vf on
// the view v, which it cannot describe, under representation: a coverage
// record naming the policy.
func heldOnV(representation schemaext.Representation) schemaext.Coverage {
	return must.Must(chschema.RowPolicyCoverage(representation, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("d1", "v", "vf"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: `the policy is on "v" (engine View)`}}}))
}

// vfDefinition is the definition of vf the read keeps beside the record, named
// as the read names it, without the database.
func vfDefinition() schemaext.Objects {
	return must.Must(schemaext.NewObjects(must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "v", "vf"),
		chschema.ObservedRowPolicy{Filter: new("id > 100"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{All: true, Except: []string{"guest"}}}))))
}

// rollbackOfV is the feature context a rollback hands its planner when the
// database it restores held vf on v: the record on the desired side, and the
// definition the read kept.
func rollbackOfV() difftypes.FeatureContext {
	return difftypes.FeatureContext{DesiredCoverage: heldOnV(schemaext.Desired), ContextObjects: vfDefinition()}
}

// TestGenerateMigrationAST_RecreatesAViewsRowPolicies plans the creation of v
// with a desired side that holds a policy on v the model cannot describe,
// which only a rollback hands the planner. A view or materialized view
// created from nothing gets the policy back from the definition the read
// kept, after its CREATE. A view that takes over from a materialized view of
// the name kept the policy, and a forward plan, whose desired side holds no
// such policy, creates none.
func TestGenerateMigrationAST_RecreatesAViewsRowPolicies(t *testing.T) {
	policy := "CREATE ROW POLICY `vf` ON `d1`.`v` USING (id > 100) AS RESTRICTIVE TO ALL EXCEPT `guest`"
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{name: "a view the rollback creates",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}}, Features: rollbackOfV()},
			want: []string{"CREATE VIEW `d1`.`v` AS", policy}},
		{name: "a materialized view the rollback creates",
			diff: &difftypes.SchemaDiff{MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}}, Features: rollbackOfV()},
			want: []string{"CREATE MATERIALIZED VIEW `d1`.`v` ENGINE = MergeTree ORDER BY tuple() AS", policy}},
		{name: "a view that takes over from a materialized view",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}},
				MaterializedViewsRemoved: difftypes.MaterializedViewChanges{{Name: "d1.v"}}, Features: rollbackOfV()},
			want: []string{"DROP VIEW IF EXISTS `d1`.`v`", "CREATE VIEW `d1`.`v` AS"}},
		{name: "a view a forward plan creates",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "d1.v", Body: "SELECT 1 AS id"}},
				Features: difftypes.FeatureContext{CurrentCoverage: heldOnV(schemaext.Observed), ContextObjects: vfDefinition()}},
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
		Features: difftypes.FeatureContext{DesiredCoverage: heldOnV(schemaext.Desired)}}

	nodes, err := clickhouse.New().GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("clickhouse", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `-- CLICKHOUSE: row policy "vf" on "v" is not created again: the read kept no definition of it (the policy is on "v" (engine View))`)
	c.Assert(planHeads(c, diff), qt.DeepEquals, []string{"CREATE VIEW `d1`.`v` AS"})
}
