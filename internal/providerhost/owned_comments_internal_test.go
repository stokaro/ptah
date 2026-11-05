package providerhost

// White-box testing required: the comment steps the host plans for a
// provider that owns the table lifecycle are private adapter state, and
// whether a comment reaches that provider or is dropped on the way is decided
// before anything crosses the wire. integration/providercli covers the
// transport.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// commentPlanner plans for a provider that owns the table lifecycle, reports
// the indexes its tables hold and carries comments on the common-object wire.
func commentPlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true, indexes: true, commonObjects: true, comments: true}
}

// lifecycleClaimer claims every table, column and index step the host hands
// it, as an owner of the table lifecycle does, and leaves the common-object
// steps, comments included, to the host, ordered after its own steps of the
// table that do more than read and drop, as the YDB provider orders them.
type lifecycleClaimer struct {
	*engine.Runtime
	request featureplan.Request
}

func (r *lifecycleClaimer) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	r.request = request
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/fixture"}
	var claimed []featureplan.CommonStep
	for _, common := range request.CommonSteps {
		if common.Parent.Name.Source == "" || common.CommonObject != nil {
			continue
		}
		claimed = append(claimed, common)
		id := plangraph.StepID{Owner: contribution.Owner, Name: common.ID.Name}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: id.Name}, Phase: featureplan.PhaseLifecycle},
			Effects: common.Effects, Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "Changes the table"}})
		result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common.ID}, Replacement: id})
	}
	// The claimed steps keep the host's order, so a removal comes before the
	// addition that replaces an index under its name.
	for n := 1; n < len(contribution.Steps); n++ {
		contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: contribution.Steps[n-1].ID, After: contribution.Steps[n].ID})
	}
	for _, common := range request.CommonSteps {
		if common.CommonObject == nil {
			continue
		}
		for n, step := range contribution.Steps {
			if claimed[n].Parent == common.Parent && !removesOnly(claimed[n].Effects) {
				contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: step.ID, After: common.ID})
			}
		}
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// removesOnly reports effects that read and drop, and drop something, as a
// column's removal reads its table and drops the column. The host places a
// removal after the comments, so a comment ordered after one would close a
// cycle.
func removesOnly(effects []plangraph.Effect) bool {
	dropped := false
	for _, effect := range effects {
		switch effect.Action {
		case plangraph.Drop:
			dropped = true
		case plangraph.Read:
		default:
			return false
		}
	}
	return dropped
}

// commentsOf lists the comment steps of a plan in order, each as the kind of
// object it comments, the object and the comment.
func commentsOf(nodes []ast.Node) []string {
	var result []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.ObjectCommentNode:
			result = append(result, string(typed.Object)+" "+typed.Table+"/"+typed.Name+": "+typed.Comment)
		case *ast.AlterTableNode:
			if comment, isComment := commentOperation(typed); isComment {
				result = append(result, "TABLE "+typed.Name+"/"+comment.Column+": "+comment.Comment)
			}
		}
	}
	return result
}

// TestOwnedLifecycleCreationCommentsAreSteps_HappyPath plans the comments of a
// table the plan creates, of its columns and of its indexes as steps after the
// creation: the owner takes them off the declaration, since the wire carries
// them, and without the steps they would never be set.
func TestOwnedLifecycleCreationCommentsAreSteps_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	rules := ownedIndexRules()
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded: difftypes.TableChanges{{Name: "main.users", Table: schemamodel.Table{Name: "users", Schema: "main", Comment: "People who sign in"},
			Fields: []schemamodel.Field{{Name: "id", Type: "BIGINT", Comment: "Key"}, {Name: "label", Type: "TEXT"}}}},
		IndexesAdded: difftypes.IndexChanges{
			{TableName: "main.users", Index: schemamodel.Index{Name: "users_by_label", Fields: []string{"label"}, Comment: "Lookup by name"}},
			{TableName: "main.users", Index: schemamodel.Index{Name: "users_by_id", Fields: []string{"id"}}},
		},
	}
	nodes, err := commentPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 4)
	c.Assert(nodes[0], qt.Satisfies, func(node ast.Node) bool { _, claimed := node.(*ast.ExtensionStatement); return claimed })
	c.Assert(commentsOf(nodes), qt.DeepEquals, []string{"TABLE main.users/: People who sign in", "TABLE main.users/id: Key", "INDEX main.users/users_by_label: Lookup by name"})
	// The owner still receives the complete declaration.
	c.Assert(runtime.request.Tables, qt.HasLen, 1)
	c.Assert(runtime.request.Tables[0].Desired.Table.Comment, qt.Equals, "People who sign in")
	c.Assert(runtime.request.Tables[0].Desired.Indexes[0].Comment, qt.Equals, "Lookup by name")
	// Each comment is a step of its table that reads it and changes the
	// comment, a resource apart from the object the owner creates.
	table := runtime.request.Tables[0].Subject
	var changed []string
	comments := slices.DeleteFunc(slices.Clone(runtime.request.CommonSteps), func(step featureplan.CommonStep) bool { return step.CommonObject == nil })
	for _, step := range comments {
		c.Assert(step.Parent, qt.DeepEquals, table)
		c.Assert(step.Effects, qt.HasLen, 2)
		c.Assert(step.Effects[0], qt.DeepEquals, plangraph.Effect{Subject: table, Action: plangraph.Read})
		c.Assert(step.Effects[1].Action, qt.Equals, plangraph.Alter)
		subject := step.Effects[1].Subject
		changed = append(changed, string(subject.Kind)+" "+subject.Namespace.Values()[0].Source+" "+subject.Name.Source)
	}
	c.Assert(changed, qt.DeepEquals, []string{"comment table users", "comment column id", "comment index users_by_label"})
}

// TestOwnedLifecycleIndexCommentChanges_HappyPath plans each index comment the
// plan changes once, with the comment the index ends with: a changed comment
// of a kept index, the comment of an index added to a kept table, the comment
// of an index the plan drops and adds again under its name, and the comment
// of a dropped index, which an engine that keeps comments apart from the index
// would keep. A renamed index's comment moves to its new name. An index of a
// table the plan drops has none.
func TestOwnedLifecycleIndexCommentChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	diff := preparedRecords()
	diff.TablesRemoved = difftypes.TableRemovals{{Name: "main.old", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "old", Schema: "main"}}}}
	diff.IndexesAdded = difftypes.IndexChanges{
		{TableName: "main.records", Index: schemamodel.Index{Name: "records_added", Fields: []string{"email"}, Comment: "added"}},
		{TableName: "main.records", Index: schemamodel.Index{Name: "records_replaced", Fields: []string{"name"}, Comment: "replaced"}},
	}
	diff.IndexesRemoved = []difftypes.IndexRef{{TableName: "main.records", Name: "records_replaced"}, {TableName: "main.records", Name: "records_gone"}}
	diff.IndexCommentsChanged = []difftypes.IndexCommentChange{
		{TableName: "main.old", Name: "old_email", Current: "left"},
		{TableName: "main.records", Name: "records_email", Current: "before", Desired: "after"},
		{TableName: "main.records", Name: "records_gone", Current: "gone"},
		{TableName: "main.records", Name: "records_new", From: "records_renamed", Current: "moved", Desired: "moved"},
		{TableName: "main.records", Name: "records_replaced", Current: "earlier"},
	}
	nodes, err := commentPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(commentsOf(nodes), qt.DeepEquals, []string{
		"INDEX main.records/records_email: after",
		"INDEX main.records/records_gone: ",
		"INDEX main.records/records_renamed: ",
		"INDEX main.records/records_new: moved",
		"INDEX main.records/records_replaced: replaced",
		"INDEX main.records/records_added: added",
	})
}

// TestOwnedLifecycleAddedColumnCommentIsAStep_HappyPath sets the comment of a
// column the plan adds to a kept table with a step after the addition.
func TestOwnedLifecycleAddedColumnCommentIsAStep_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	diff := ownedColumnDiff(c, []schemamodel.Field{{Name: "note", Type: "TEXT", Nullable: true, Comment: "a note"}}, nil)
	nodes, err := commentPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	c.Assert(commentsOf(nodes), qt.DeepEquals, []string{"TABLE main.records/note: a note"})
}

// TestOwnedLifecycleDroppedColumnCommentIsRemoved_HappyPath removes the
// comment of a column the plan drops from a kept table, which YDB keeps in
// the table's attributes after the column is gone, with a comment step that
// comes before the removal. A dropped column without a comment needs none.
func TestOwnedLifecycleDroppedColumnCommentIsRemoved_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	diff := ownedColumnDiff(c, nil, []schemamodel.Field{{Name: "legacy", Type: "TEXT", Nullable: true, Comment: "Going away"}, {Name: "spare", Type: "TEXT", Nullable: true}})
	nodes, err := commentPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 3)
	c.Assert(commentsOf(nodes[:1]), qt.DeepEquals, []string{"TABLE main.records/legacy: "})
	c.Assert(nodes[1:], qt.All(qt.Satisfies), func(node ast.Node) bool { _, claimed := node.(*ast.ExtensionStatement); return claimed })
}

// TestOwnedLifecycleWithoutTheCommentWire_HappyPath plans no comment step
// for an engine that does not carry comments on the common-object wire: its
// owner receives the comments in the declaration.
func TestOwnedLifecycleWithoutTheCommentWire_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &lifecycleClaimer{Runtime: base}
	rules := ownedIndexRules()
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded: difftypes.TableChanges{{Name: "main.users", Table: schemamodel.Table{Name: "users", Schema: "main", Comment: "People who sign in"},
			Fields: []schemamodel.Field{{Name: "id", Type: "BIGINT", Comment: "Key"}}}},
		IndexesAdded: difftypes.IndexChanges{{TableName: "main.users", Index: schemamodel.Index{Name: "users_by_id", Fields: []string{"id"}, Comment: "by id"}}},
	}
	nodes, err := indexPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(commentsOf(nodes), qt.HasLen, 0)
}

// TestOwnedLifecycleIndexCommentChangesWithoutTheWire_FailurePath keeps an
// index comment change refused for an engine that does not carry comments on
// the common-object wire: there is no step to make it with.
func TestOwnedLifecycleIndexCommentChangesWithoutTheWire_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.IndexCommentsChanged = []difftypes.IndexCommentChange{{TableName: "main.records", Name: "records_email", Current: "before", Desired: "after"}}
	_, err := indexPlanner().commonObjectSteps(diff)
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.IndexCommentsChanged`)
}

// TestIndexCommentReachesTheWire names an index by its table, as the wire
// names a column.
func TestIndexCommentReachesTheWire(t *testing.T) {
	c := qt.New(t)
	change, common, err := commonObjectTarget(everyCommonKind...).commonObjectChange(
		ast.NewObjectComment(ast.CommentedIndex, "items_by_name", "by name").SetTable("app.items"))
	c.Assert(err, qt.IsNil)
	c.Assert(common, qt.IsTrue)
	index := &protocol.Reference{Engine: "fixture", Kind: "index", Schema: "app", Path: []string{"items"}, Name: "items_by_name"}
	c.Assert(change, qt.CmpEquals(protocmp.Transform()), &protocol.Change{Object: index, Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
		Operation: &protocol.CommonObjectChange_SetComment{SetComment: &protocol.ObjectComment{Object: index, Text: "by name"}}}}})
}
