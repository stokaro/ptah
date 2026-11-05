package providerhost

// White-box testing required: the rename step the host plans for a provider
// that owns the table lifecycle is a private placeholder whose graph metadata
// decides what crosses the wire, and no exported call returns it before the
// provider claims it. integration/providercli covers the transport.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// renamePlanner plans for a provider that owns the table lifecycle, reports
// the indexes its tables hold and renames them in place.
func renamePlanner() commonPlanner {
	planner := indexPlanner()
	planner.renames = true
	return planner
}

// renamedRecords keeps main.records, whose index records_by_mail the
// declaration names records_email with one definition.
func renamedRecords() *difftypes.SchemaDiff {
	diff := preparedRecords()
	diff.TablePreparation.Prepared[0].Current.Indexes = []catalog.Index{{Name: "records_by_mail", TableName: "records", Schema: "main", Columns: []string{"email"}}}
	diff.IndexesRenamed = []difftypes.IndexRename{{TableName: "main.records", From: "records_by_mail", To: "records_email"}}
	return diff
}

// A renamed index is one step of its table that the owner claims. It names
// the rename and reads the table, drops the held name and creates the
// declared one, so a step on either index orders against it. The table is an
// altered parent, so the owner receives both captured sides.
func TestOwnedLifecycleRenamesAnIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	nodes, err := renamePlanner().GenerateMigrationAST(c.Context(), runtime, renamedRecords())
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(runtime.request.Tables, qt.HasLen, 1)
	c.Assert(runtime.request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
	renames := slices.DeleteFunc(slices.Clone(runtime.request.CommonSteps), func(step featureplan.CommonStep) bool { return step.RenamedIndex == nil })
	c.Assert(renames, qt.HasLen, 1)
	step := renames[0]
	table := runtime.request.Tables[0].Subject
	index := func(name string) objectidentity.ID {
		return planningChild(table, objectidentity.NewBuilder(ownedIndexRules()).IndexParts("main", "records", name))
	}
	c.Assert(step.Parent, qt.DeepEquals, table)
	c.Assert(step.RenamedIndex, qt.DeepEquals, &ast.RenameIndexOperation{From: "records_by_mail", To: "records_email"})
	c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{
		{Subject: table, Action: plangraph.Read},
		{Subject: index("records_by_mail"), Action: plangraph.Drop},
		{Subject: index("records_email"), Action: plangraph.Create},
	})
}

// A rename step the owner leaves unclaimed refuses the plan by name, as an
// unclaimed index addition does, rather than rendering a placeholder.
func TestOwnedLifecycleRenameLeftUnclaimed_FailurePath(t *testing.T) {
	c := qt.New(t)
	err := rejectUnclaimedChanges([]ast.Node{&pendingIndexRename{table: "main.records", from: "records_by_mail", to: "records_email"}})
	c.Assert(err, qt.ErrorMatches, `.*unclaimed provider rename of index records_by_mail of table main\.records to records_email`)
}

// Without planning.index-renames the planner has no step for a pair, so the
// rename stays refused, and a rename of a table the plan creates or drops is
// a diff the comparison never builds.
func TestOwnedLifecycleRenamesAnIndex_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, err := indexPlanner().commonObjectSteps(renamedRecords())
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexesRenamed`)

	created := renamedRecords()
	created.TablePreparation = nil
	created.TablesAdded = difftypes.TableChanges{{Name: "main.records", Table: schemamodel.Table{Name: "records", Schema: "main"}}}
	steps, err := renamePlanner().indexChanges(created)
	c.Assert(err, qt.ErrorMatches, `provider planner received a rename of index records_by_mail of table main\.records, which the plan does not keep`)
	c.Assert(steps.renames, qt.HasLen, 0)

	removed := renamedRecords()
	removed.TablePreparation = nil
	removed.TablesRemoved = difftypes.TableRemovals{{Name: "main.records", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}}}}
	steps, err = renamePlanner().indexChanges(removed)
	c.Assert(err, qt.ErrorMatches, `provider planner received a rename .*, which the plan does not keep`)
	c.Assert(steps.renames, qt.HasLen, 0)
}
