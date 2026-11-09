package providerhost

// White-box testing required: the catalog key a read produces and the
// captured planning request are private adapter state, and they decide
// whether a key change reaches the table's owner, is refused, or is dropped
// on the way and reported synced. The YDB provider's live tests cover the
// engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// keyPlanner plans for a provider that owns the table lifecycle, reports the
// key its tables hold and plans the change of it.
func keyPlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true, observedKeys: true, keys: true}
}

// keyOrderChange is the change of main.records' key from (b, a) to (a, b),
// as the comparison reports it.
func keyOrderChange(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	diff.ConstraintsAdded = difftypes.ConstraintAdditions{{Name: "records_pkey", TableName: "main.records", Type: "PRIMARY KEY", Columns: []string{"a", "b"}}}
	diff.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "records_pkey", TableName: "main.records", Type: "PRIMARY KEY"}}
	return diff
}

// A table whose only change is its key is altered, so its owner compares the
// declared key with the held one; the addition and the removal each do it.
func TestOwnedLifecycleKeyChangeAltersItsTable_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		added   difftypes.ConstraintAdditions
		removed difftypes.ConstraintRemovals
	}{
		{"both", keyOrderChange(&difftypes.SchemaDiff{}).ConstraintsAdded, keyOrderChange(&difftypes.SchemaDiff{}).ConstraintsRemoved},
		{"addition", difftypes.ConstraintAdditions{{Name: "records_pkey", TableName: "main.records", Type: "PRIMARY KEY", Columns: []string{"a"}}}, nil},
		{"removal", nil, difftypes.ConstraintRemovals{{Name: "records_pkey", TableName: "main.records", Type: "primary key"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := preparedRecords()
			diff.ConstraintsAdded, diff.ConstraintsRemoved = row.added, row.removed
			_, err := keyPlanner().commonObjectSteps(diff)
			c.Assert(err, qt.IsNil)
			request, _, err := keyPlanner().planningContext(diff)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Tables, qt.HasLen, 1)
			c.Assert(request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
		})
	}
}

// Without a key change the prepared table stays context, not an action.
func TestOwnedLifecycleKeptKeyStaysContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, _, err := keyPlanner().planningContext(preparedRecords())
	c.Assert(err, qt.IsNil)
	c.Assert(request.Tables, qt.HasLen, 1)
	c.Assert(request.Tables[0].Action, qt.Equals, featureplan.ParentAction(""))
}

func TestOwnedLifecycleKeyOfAnUnheldTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := preparedRecords()
	diff.ConstraintsRemoved = difftypes.ConstraintRemovals{{Name: "elsewhere_pkey", TableName: "main.elsewhere", Type: "PRIMARY KEY"}}
	_, _, err := keyPlanner().planningContext(diff)
	c.Assert(err, qt.ErrorMatches, `.*a primary key change names table "main\.elsewhere", which the provider plan does not hold`)
}

// Each planner meets a key change, or a change of another constraint, that it
// cannot hand to an owner, and refuses it rather than plan the table as if
// nothing changed.
func TestKeyChangeWithoutAnOwnerThatPlansIt_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		planner commonPlanner
		diff    *difftypes.SchemaDiff
		want    string
	}{
		{"the owner does not plan key changes", commonPlanner{target: "fixture", assessParents: true, observedKeys: true}, keyOrderChange(preparedRecords()),
			`.*does not support changing the primary key of table main\.records on the fixture engine, whose provider does not plan it`},
		{"the host plans the table's operations", commonPlanner{target: "fixture", assessParents: true, commonTables: true, observedKeys: true, keys: true},
			keyOrderChange(preparedRecords()), `.*does not support changing the primary key of table main\.records on the fixture engine, whose provider does not plan it`},
		{"no read reports keys", commonPlanner{target: "fixture", assessParents: true, keys: true}, keyOrderChange(preparedRecords()),
			`.*does not support SchemaDiff\.ConstraintsAdded`},
		{"another constraint added", keyPlanner(), &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "records_email", TableName: "main.records", Type: "UNIQUE"}}},
			`.*does not support SchemaDiff\.ConstraintsAdded`},
		{"another constraint removed", keyPlanner(), &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "records_check", TableName: "main.records", Type: "CHECK"}}},
			`.*does not support SchemaDiff\.ConstraintsRemoved`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := row.planner.commonObjectSteps(row.diff)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

// A foreign key or a CHECK of a table the plan creates travels in the table's
// declaration, which its owner creates the table from, and a constraint of a
// table the plan drops goes with the table: neither is a constraint change of
// a kept table, which stays refused. The declared foreign keys the diff
// carries as context do not refuse the plan either.
func TestOwnedLifecycleConstraintsOfCreatedAndDroppedTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	foreign := schemamodel.Constraint{Name: "orders_customer", Type: "FOREIGN KEY", Table: "orders", Columns: []string{"customer_id"},
		ForeignTable: "customers", ForeignColumns: []string{"id"}}
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded: difftypes.TableChanges{{Name: "main.orders", Table: schemamodel.Table{Name: "orders", Schema: "main"},
			Constraints: []schemamodel.Constraint{foreign}}},
		TablesRemoved: difftypes.TableRemovals{{Name: "main.legacy", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "legacy", Schema: "main"}}}},
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "orders_customer", TableName: "main.orders", Type: "FOREIGN KEY", Columns: []string{"customer_id"}},
			{Name: "orders_total_check", TableName: "main.orders", Type: "CHECK"},
		},
		ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "legacy_check", TableName: "main.legacy", Type: "CHECK"}},
		// Every foreign key the declaration states is context the
		// comparison carries, changed or not.
		DeclaredForeignKeys: []difftypes.ForeignKeyDeclaration{{TableName: "main.orders", Name: "orders_customer", Columns: []string{"customer_id"},
			ForeignTable: "main.customers", ForeignColumn: "id"}},
	}
	_, err := keyPlanner().commonObjectSteps(diff)
	c.Assert(err, qt.IsNil)
	request, _, err := keyPlanner().planningContext(diff)
	c.Assert(err, qt.IsNil)
	c.Assert(request.Tables, qt.HasLen, 2)
	c.Assert(request.Tables[0].Action, qt.Equals, featureplan.CreateTable)
	c.Assert(request.Tables[0].Desired.Constraints, qt.DeepEquals, []schemamodel.Constraint{foreign})
	c.Assert(request.Tables[1].Action, qt.Equals, featureplan.DropTable)
}

// The host that plans a provider's tables itself has no owner to hand a
// created table's foreign key to, so it keeps refusing it.
func TestCreatedTableConstraintWithoutAnOwner_FailurePath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		TablesAdded:      difftypes.TableChanges{{Name: "main.orders", Table: schemamodel.Table{Name: "orders", Schema: "main"}}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "orders_customer", TableName: "main.orders", Type: "FOREIGN KEY"}},
	}
	planner := commonPlanner{target: "fixture", assessParents: true, commonTables: true, observedKeys: true, keys: true}
	_, err := planner.commonObjectSteps(diff)
	c.Assert(err, qt.ErrorMatches, `.*does not support SchemaDiff\.ConstraintsAdded`)
}

// A read that negotiated ordered keys gives the catalog the table's key as a
// constraint, in key order and under the name the read gives it, with the
// key's own attributes.
func TestReadKeepsTheOrderOfAKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := reader{session: &session{remote: &protocol.Session{Features: []string{provider.ObservedPrimaryKeysFeature}}}}
	key := r.primaryKey(&protocol.Table{PrimaryKey: &protocol.ObservedConstraint{Name: "items_pkey", TableName: "items", Schema: "app", Type: "PRIMARY KEY",
		ColumnNames: []string{"b", "a"}, IncludeColumns: []string{"body"}, UsingMethod: new("btree"), Comment: "by b", Deferrable: true, Initially: "DEFERRED"}})
	c.Assert(key, qt.DeepEquals, &catalog.Constraint{Name: "items_pkey", TableName: "items", Schema: "app", Type: "PRIMARY KEY", ColumnName: "b",
		ColumnNames: []string{"b", "a"}, IncludeColumns: []string{"body"}, UsingMethod: new("btree"), Comment: "by b", Deferrable: true, Initially: "DEFERRED"})
}

// Without the feature, and for a table that holds no key, the read gives the
// catalog no key constraint.
func TestReadWithoutAKeyGivesNoConstraint_HappyPath(t *testing.T) {
	c := qt.New(t)
	keyed := &protocol.Table{PrimaryKey: &protocol.ObservedConstraint{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", ColumnNames: []string{"a"}}}
	unlisted := reader{session: &session{remote: &protocol.Session{}}}
	c.Assert(unlisted.primaryKey(keyed), qt.IsNil)
	listed := reader{session: &session{remote: &protocol.Session{Features: []string{provider.ObservedPrimaryKeysFeature}}}}
	c.Assert(listed.primaryKey(&protocol.Table{}), qt.IsNil)
}
