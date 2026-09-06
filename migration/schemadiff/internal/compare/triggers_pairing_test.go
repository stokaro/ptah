package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestTriggers_AQualifiedReadbackIsTheSameTriggerAsAnUnqualifiedDeclaration is
// stokaro/ptah#2436.
//
// A trigger declared on `orders` and read back under `app.orders` came out as
// one addition and one removal, so every run dropped it and created it again --
// succeeding each time, and leaving a window in which the table had no trigger.
// For an audit or an updated_at trigger, writes landing in that window are
// simply not seen.
//
// MySQL is where it bites: its reader reports the database name for everything,
// a Go annotation leaves it bare, and the database name is whatever the
// connection points at, so no static default schema can join them.
func TestTriggers_AQualifiedReadbackIsTheSameTriggerAsAnUnqualifiedDeclaration(t *testing.T) {
	tests := []struct {
		name           string
		dialect        string
		declaredTable  string
		readBackSchema string
		readBackTable  string
	}{
		{
			name:    "mysql, where the schema is the database",
			dialect: platform.MySQL, declaredTable: "orders",
			readBackSchema: "app", readBackTable: "orders",
		},
		{
			name:    "postgres outside the default schema",
			dialect: platform.Postgres, declaredTable: "orders",
			readBackSchema: "reporting", readBackTable: "orders",
		},
		{
			name:    "sql server outside dbo",
			dialect: platform.SQLServer, declaredTable: "orders",
			readBackSchema: "sales", readBackTable: "orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.TriggersWithDialect(
				declaringTrigger(test.declaredTable),
				holdingTrigger(test.readBackSchema, test.readBackTable),
				diff, test.dialect)

			c.Assert(diff.TriggersAdded, qt.HasLen, 0)
			c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
			c.Assert(diff.TriggersModified, qt.HasLen, 0,
				qt.Commentf("the two are one trigger and nothing about it changed"))
		})
	}
}

// TestTriggers_TwoTablesInTwoSchemasAreStillTwoTriggers is the control the
// pairing needs.
//
// Every row above is satisfied by an implementation that folded any two
// triggers sharing a name into one. This declares a trigger on one schema's
// table and reads one back from another's, both named, and requires them to
// stay an addition and a removal: tier 3 supplies a schema nobody wrote down,
// and it never overrules one that is written down.
func TestTriggers_TwoTablesInTwoSchemasAreStillTwoTriggers(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(
		declaringTrigger("reporting.orders"),
		holdingTrigger("app", "orders"),
		diff, platform.Postgres)

	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersAdded[0].TableName, qt.Equals, "reporting.orders")
	c.Assert(diff.TriggersRemoved, qt.HasLen, 1)
	c.Assert(diff.TriggersRemoved[0].TableName, qt.Equals, "app.orders")
	c.Assert(diff.TriggersModified, qt.HasLen, 0)
}

// TestTriggers_ADifferentTriggerOnTheSameTableIsStillTwoTriggers is the other
// half of the control.
//
// The table halves pair; the trigger names do not. Folding on the table alone
// would report one modification where a declaration adds a trigger and the
// database holds a different one.
func TestTriggers_ADifferentTriggerOnTheSameTableIsStillTwoTriggers(t *testing.T) {
	c := qt.New(t)
	desired := declaringTrigger("orders")
	desired.Triggers[0].Name = "touch"
	database := holdingTrigger("app", "orders")
	database.Triggers[0].Name = "audit"
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(desired, database, diff, platform.MySQL)

	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersAdded[0].TriggerName, qt.Equals, "touch")
	c.Assert(diff.TriggersRemoved, qt.HasLen, 1)
	c.Assert(diff.TriggersRemoved[0].TriggerName, qt.Equals, "audit")
}

// TestTriggers_APairedTriggerStillReportsWhatChanged proves the pairing did not
// buy silence.
//
// Every assertion above counts zero of something. This one pairs the same two
// and changes the timing, and requires the modification to be reported -- an
// implementation that paired everything and compared nothing would satisfy the
// rest.
func TestTriggers_APairedTriggerStillReportsWhatChanged(t *testing.T) {
	c := qt.New(t)
	desired := declaringTrigger("orders")
	desired.Triggers[0].Timing = "BEFORE"
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(desired, holdingTrigger("app", "orders"), diff, platform.MySQL)

	c.Assert(diff.TriggersAdded, qt.HasLen, 0)
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].TriggerName, qt.Equals, "touch")
}

// declaringTrigger is a desired schema with one trigger on a named table.
func declaringTrigger(table string) *schemamodel.Database {
	return &schemamodel.Database{Triggers: []schemamodel.Trigger{{
		StructName: "Order", Name: "touch", Table: table,
		Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;",
	}}}
}

// holdingTrigger is a live database reporting one, qualified the way a reader
// does.
func holdingTrigger(schema, table string) *catalog.Database {
	return &catalog.Database{Triggers: []catalog.Trigger{{
		Schema: schema, Name: "touch", Table: table,
		Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;",
	}}}
}

// TestTriggers_TwoDeclarationsForOneTriggerDoNotBothClaimIt is what the
// exact-first pass and the paired guard are for.
//
// A schema spelling the same trigger both ways names one object twice. Exactly
// one of them is the one the database holds, and it is the one spelled as the
// reader spells it -- the first of objectlookup's tiers, which never
// re-interprets a name written the way the diff writes it. The other is an
// addition.
//
// Without the exact-first pass the answer depends on which declaration comes
// first in the slice; without the paired guard both claim the same trigger and
// nothing is reported at all.
func TestTriggers_TwoDeclarationsForOneTriggerDoNotBothClaimIt(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Triggers: []schemamodel.Trigger{
		// Bare first, so an order-dependent implementation gives it the match.
		{StructName: "Order", Name: "touch", Table: "orders",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;"},
		{StructName: "Order", Name: "touch", Table: "app.orders",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;"},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(desired, holdingTrigger("app", "orders"), diff, platform.MySQL)

	// The qualified declaration took it, because it is spelled as the reader
	// spells it. The bare one is the addition.
	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersAdded[0].TableName, qt.Equals, "orders")
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
	c.Assert(diff.TriggersModified, qt.HasLen, 0)
}

// TestTriggers_TheAnswerDoesNotDependOnDeclarationOrder is the same fixture
// with the two declarations swapped.
//
// One object, one answer, whichever way round the slice happens to be. An
// implementation pairing in slice order would give the trigger to whichever
// came first and report the other as an addition -- which reads identically
// here unless both orders are measured.
func TestTriggers_TheAnswerDoesNotDependOnDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Triggers: []schemamodel.Trigger{
		{StructName: "Order", Name: "touch", Table: "app.orders",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;"},
		{StructName: "Order", Name: "touch", Table: "orders",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: "SET @x = 1;"},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.TriggersWithDialect(desired, holdingTrigger("app", "orders"), diff, platform.MySQL)

	c.Assert(diff.TriggersAdded, qt.HasLen, 1)
	c.Assert(diff.TriggersAdded[0].TableName, qt.Equals, "orders")
	c.Assert(diff.TriggersRemoved, qt.HasLen, 0)
}
