package providerhost

// White-box testing required: the catalog constraints a read produces and the
// captured planning request are private adapter state, and they decide
// whether a foreign key or CHECK change reaches the table's owner, is refused,
// or is dropped on the way and reported synced. The subprocess tests in
// integration/providercli cover the engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
)

// constraintPlanner plans for a provider that owns the table lifecycle,
// reports the foreign keys and CHECKs its tables hold and plans their change.
func constraintPlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true, observedConstraints: true, constraints: true}
}

// recordsForeignKey is main.records' foreign key records_owner_fk, as the
// comparison adds it.
func recordsForeignKey() difftypes.ConstraintAdditions {
	return difftypes.ConstraintAdditions{{Name: "records_owner_fk", TableName: "main.records", Type: "FOREIGN KEY", Columns: []string{"owner_id"},
		ForeignTable: "main.owners", ForeignColumns: []string{"id"}}}
}

// A table whose only change is a foreign key or a CHECK is altered, so its
// owner compares the declared constraints with the held ones.
func TestOwnedLifecycleConstraintChangeAltersItsTable_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		added   difftypes.ConstraintAdditions
		removed difftypes.ConstraintRemovals
	}{
		{"a foreign key added", recordsForeignKey(), nil},
		{"a foreign key removed", nil, difftypes.ConstraintRemovals{{Name: "records_owner_fk", TableName: "main.records", Type: "FOREIGN KEY"}}},
		{"a CHECK replaced", difftypes.ConstraintAdditions{{Name: "records_score_check", TableName: "main.records", Type: "CHECK", CheckExpression: "score >= 1"}},
			difftypes.ConstraintRemovals{{Name: "records_score_check", TableName: "main.records", Type: "check"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := preparedRecords()
			diff.ConstraintsAdded, diff.ConstraintsRemoved = row.added, row.removed
			_, err := constraintPlanner().commonObjectSteps(diff)
			c.Assert(err, qt.IsNil)
			request, _, err := constraintPlanner().planningContext(diff)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Tables, qt.HasLen, 1)
			c.Assert(request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
		})
	}
}

// A constraint of a table the plan creates travels in the creation, and one
// of a table it removes leaves with the table, so neither needs an owner that
// plans constraint changes, and neither alters its table.
func TestOwnedLifecycleConstraintsOfCreatedAndRemovedTables_HappyPath(t *testing.T) {
	rules := ownedIndexRules()
	created := difftypes.TableChanges{{Name: "main.records", Table: schemamodel.Table{Name: "records", Schema: "main"}}}
	removed := difftypes.TableRemovals{{Name: "main.records", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}}}}
	for _, row := range []struct {
		name    string
		planner commonPlanner
		diff    *difftypes.SchemaDiff
		action  featureplan.ParentAction
	}{
		{"a foreign key of a created table", commonPlanner{target: "fixture", assessParents: true, observedConstraints: true},
			&difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesAdded: created, ConstraintsAdded: recordsForeignKey()}, featureplan.CreateTable},
		{"a CHECK of a removed table", commonPlanner{target: "fixture", assessParents: true, observedConstraints: true},
			&difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesRemoved: removed,
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "records_check", TableName: "main.records", Type: "CHECK"}}}, featureplan.DropTable},
		{"the primary key of a removed table", commonPlanner{target: "fixture", assessParents: true, observedKeys: true},
			&difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesRemoved: removed,
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "records_pkey", TableName: "main.records", Type: "PRIMARY KEY"}}}, featureplan.DropTable},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := row.planner.commonObjectSteps(row.diff)
			c.Assert(err, qt.IsNil)
			request, _, err := row.planner.planningContext(row.diff)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Tables, qt.HasLen, 1)
			c.Assert(request.Tables[0].Action, qt.Equals, row.action)
		})
	}
}

// Each planner meets a constraint change of a kept table that it cannot hand
// to an owner, and refuses it rather than plan the table as if nothing
// changed.
func TestConstraintChangeWithoutAnOwnerThatPlansIt_FailurePath(t *testing.T) {
	withForeignKey := func() *difftypes.SchemaDiff {
		diff := preparedRecords()
		diff.ConstraintsAdded = recordsForeignKey()
		return diff
	}
	for _, row := range []struct {
		name    string
		planner commonPlanner
		diff    *difftypes.SchemaDiff
		want    string
	}{
		{"the owner does not plan constraint changes", commonPlanner{target: "fixture", assessParents: true, observedConstraints: true}, withForeignKey(),
			`.*does not support changing constraint records_owner_fk of table main\.records on the fixture engine, whose provider does not plan it`},
		{"the host plans the table's operations", commonPlanner{target: "fixture", assessParents: true, commonTables: true, observedConstraints: true, constraints: true},
			withForeignKey(), `.*does not support changing constraint records_owner_fk of table main\.records on the fixture engine, whose provider does not plan it`},
		{"no read reports constraints", commonPlanner{target: "fixture", assessParents: true, constraints: true}, withForeignKey(),
			`.*does not support SchemaDiff\.ConstraintsAdded`},
		{"a read reports keys alone", commonPlanner{target: "fixture", assessParents: true, observedKeys: true, keys: true}, withForeignKey(),
			`.*does not support SchemaDiff\.ConstraintsAdded`},
		{"a unique constraint", constraintPlanner(), &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "records_email", TableName: "main.records", Type: "UNIQUE"}}},
			`.*does not support SchemaDiff\.ConstraintsRemoved`},
		{"a primary key without the key read", constraintPlanner(), keyOrderChange(preparedRecords()), `.*does not support SchemaDiff\.ConstraintsAdded`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := row.planner.commonObjectSteps(row.diff)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

// A constraint of a table the plan does not hold is refused by its name.
func TestOwnedLifecycleConstraintOfAnUnheldTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "elsewhere_check", TableName: "main.elsewhere", Type: "CHECK"}}
	_, _, err := constraintPlanner().planningContext(diff)
	c.Assert(err, qt.ErrorMatches, `.*a change of constraint elsewhere_check names table "main\.elsewhere", which the provider plan does not hold`)
}

// The declared foreign keys and the columns of a removed one are context a
// planner receives where the engine reports constraints.
func TestOwnedLifecycleForeignKeyContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.DeclaredForeignKeys = []difftypes.ForeignKeyDeclaration{{TableName: "main.records", Name: "records_owner_fk"}}
	diff.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "records_owner_fk", TableName: "main.records", Type: "FOREIGN KEY"}}
	diff.ForeignKeysRemovedWithTables = []difftypes.ForeignKeyRemovalInfo{{Name: "records_owner_fk", TableName: "main.records", Columns: []string{"owner_id"}}}

	_, err := constraintPlanner().commonObjectSteps(diff)

	c.Assert(err, qt.IsNil)
}

// A planner of an engine that reports no constraints keeps the declared
// foreign keys refused: the engine models none.
func TestOwnedLifecycleForeignKeyContext_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.DeclaredForeignKeys = []difftypes.ForeignKeyDeclaration{{TableName: "main.records", Name: "records_owner_fk"}}

	_, err := commonPlanner{target: "fixture", assessParents: true, observedKeys: true, keys: true}.commonObjectSteps(diff)

	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.DeclaredForeignKeys`)
}

// A read that negotiated constraints gives the catalog each foreign key and
// CHECK with its attributes, the singular column names the first members.
func TestReadKeepsForeignKeysAndChecks_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := reader{session: &session{remote: &protocol.Session{Features: []string{provider.ObservedConstraintsFeature}}}}
	constraints := r.constraints(&protocol.Table{Constraints: []*protocol.ObservedConstraint{
		{Name: "posts_owner_fk", TableName: "posts", Schema: "app", Type: "FOREIGN KEY", ColumnNames: []string{"owner_id", "region"},
			ForeignTable: new("owners"), ForeignSchema: "app", ForeignColumns: []string{"id", "region"}, DeleteRule: new("CASCADE"), UpdateRule: new("NO ACTION"),
			Deferrable: true, Initially: "DEFERRED", Match: "FULL", Comment: "owner"},
		{Name: "posts_score_check", TableName: "posts", Schema: "app", Type: "CHECK", CheckClause: new("(score >= '0'::bigint)"), NotEnforced: true, NotValid: true},
	}})
	c.Assert(constraints, qt.DeepEquals, []catalog.Constraint{
		{Name: "posts_owner_fk", TableName: "posts", Schema: "app", Type: "FOREIGN KEY", ColumnName: "owner_id", ColumnNames: []string{"owner_id", "region"},
			ForeignTable: new("owners"), ForeignSchema: "app", ForeignColumn: new("id"), ForeignColumns: []string{"id", "region"}, DeleteRule: new("CASCADE"),
			UpdateRule: new("NO ACTION"), Deferrable: true, Initially: "DEFERRED", Match: "FULL", Comment: "owner"},
		{Name: "posts_score_check", TableName: "posts", Schema: "app", Type: "CHECK", CheckClause: new("(score >= '0'::bigint)"), NotEnforced: true, NotValid: true},
	})
}

// Without the feature the read gives the catalog no foreign key or CHECK.
func TestReadWithoutConstraintsGivesNone_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := reader{session: &session{remote: &protocol.Session{}}}
	constraints := r.constraints(&protocol.Table{Constraints: []*protocol.ObservedConstraint{{Name: "posts_check", TableName: "posts", Type: "CHECK", CheckClause: new("(id > 0)")}}})
	c.Assert(constraints, qt.IsNil)
}
