package generator

// White-box testing required: what this pins is which trigger the reversal
// hands each direction, and the reversal is unexported. Through the public API
// a rollback that recreates the wrong definition and one that recreates the
// right one are both just SQL that applies.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_ARolledBackTriggerRemovalIsRecreatedFromThePriorSchema
// pins the direction the exchange cannot carry.
//
// A forward removal holds two names, which is all a DROP needs. Reversed it
// becomes an addition, and CREATE TRIGGER needs a definition -- so the operand
// has to be recovered from the pre-change database or the rollback drops a
// trigger it never puts back. Nothing about the plan says so: the DROP in the
// forward direction and the silence in the rollback both look like plans that
// ran.
func TestReverseSchemaDiff_ARolledBackTriggerRemovalIsRecreatedFromThePriorSchema(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)

	prior := &schemamodel.Database{Triggers: []schemamodel.Trigger{{
		StructName: "User", Name: "touch", Table: "public.users",
		Timing: "AFTER", Event: "UPDATE", ForEach: "ROW",
		Body: "BEGIN RETURN NEW; END;",
	}}}

	additions := triggerAdditionsFromRemovals([]difftypes.TriggerRef{{
		TriggerName: "touch", TableName: "users",
	}}, prior, semantics)

	c.Assert(additions, qt.HasLen, 1)
	c.Assert(additions[0].Desired.Body, qt.Equals, "BEGIN RETURN NEW; END;",
		qt.Commentf("the rollback recreates the trigger the database held, resolved across the two spellings"))
}

// TestReverseSchemaDiff_ARolledBackTriggerAdditionCarriesNoOperand is the other
// half of the same exchange.
//
// A forward addition holds a definition. Reversed it becomes a removal, and a
// DROP is written from the two names -- so the definition is dropped rather than
// carried across, because an entry holding a definition nothing reads tells the
// next reader that something does.
func TestReverseSchemaDiff_ARolledBackTriggerAdditionCarriesNoOperand(t *testing.T) {
	c := qt.New(t)

	removals := triggerRemovalsFromAdditions([]difftypes.TriggerRef{{
		TriggerName: "touch", TableName: "users",
		Desired: schemamodel.Trigger{
			Name: "touch", Table: "users", Timing: "AFTER", Event: "UPDATE",
			ForEach: "ROW", Body: "BEGIN RETURN NEW; END;",
		},
	}})

	c.Assert(removals, qt.HasLen, 1)
	c.Assert(removals[0].TriggerName, qt.Equals, "touch")
	c.Assert(removals[0].TableName, qt.Equals, "users")
	c.Assert(removals[0].Desired, qt.DeepEquals, schemamodel.Trigger{},
		qt.Commentf("a DROP is written from the names alone"))
}

// TestReverseSchemaDiff_ARolledBackTriggerModificationReplacesThePriorDefinition
// pins the third direction.
//
// A modification renders CREATE OR REPLACE TRIGGER from its operand, so
// reversing the change map without reversing the operand would have the down
// direction re-apply the definition it is undoing.
func TestReverseSchemaDiff_ARolledBackTriggerModificationReplacesThePriorDefinition(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)

	prior := &schemamodel.Database{Triggers: []schemamodel.Trigger{{
		StructName: "User", Name: "touch", Table: "users",
		Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
		Body: "BEGIN RETURN OLD; END;",
	}}}

	reversed := reverseTriggerDiffs([]difftypes.TriggerDiff{{
		TriggerName: "touch", TableName: "users",
		Changes: map[string]string{"timing": "BEFORE -> AFTER"},
		Desired: schemamodel.Trigger{
			Name: "touch", Table: "users", Timing: "AFTER", Event: "UPDATE",
			ForEach: "ROW", Body: "BEGIN RETURN NEW; END;",
		},
	}}, prior, semantics)

	c.Assert(reversed, qt.HasLen, 1)
	c.Assert(reversed[0].Changes["timing"], qt.Equals, "AFTER -> BEFORE")
	c.Assert(reversed[0].Desired.Timing, qt.Equals, "BEFORE",
		qt.Commentf("the rollback replaces the trigger with the definition the database held"))
	c.Assert(reversed[0].Desired.Body, qt.Equals, "BEGIN RETURN OLD; END;")
}

// TestGenerateDownMigrationSQL_RecreatesATriggerTheUpDirectionDropped drives the
// whole pipeline, which the three tests above deliberately do not.
//
// They call the reversal's helpers directly, so each pins what a helper answers
// and none pins that the reversal calls it. Restoring the plain exchange --
// `TriggersAdded: diff.TriggersRemoved` -- leaves all three green while the
// rollback stops recreating anything, because a removal carries no definition
// and the addition it becomes has nothing to render. This is the row that goes
// red for that.
func TestGenerateDownMigrationSQL_RecreatesATriggerTheUpDirectionDropped(t *testing.T) {
	c := qt.New(t)

	const priorBody = "BEGIN RETURN OLD; END;"

	// The declaration does not name the trigger; the database has it. That is
	// what puts it in TriggersRemoved.
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "SERIAL", Primary: true}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "users"}},
		Triggers: []catalog.Trigger{{
			Schema: "public", Name: "touch", Table: "users",
			Timing: "AFTER", Event: "UPDATE", ForEach: "ROW", Body: priorBody,
		}},
	}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.TriggersRemoved, qt.HasLen, 1)
	c.Assert(upDiff.TriggersAdded, qt.HasLen, 0)

	up, err := generateUpMigrationSQL(upDiff, desired, platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "DROP TRIGGER")

	down, err := generateDownMigrationSQL(
		upDiff, desired, database, platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "CREATE TRIGGER",
		qt.Commentf("the rollback puts back the trigger the up direction dropped\n%s", down))
	c.Assert(down, qt.Contains, priorBody,
		qt.Commentf("with the body the database held\n%s", down))
}

// sharedFunctionSchema declares one function and two triggers on one table:
// `shared` runs the function, `owned` carries its own body, so Ptah generates a
// function for it. The pair is the whole of stokaro/ptah#3722: a DROP takes the
// generated function with its trigger and leaves the declared one alone.
func sharedFunctionSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
		Functions: []schemamodel.Function{{
			Name: "public.normalize_email", Returns: "TRIGGER", Language: "plpgsql",
			Body: "BEGIN NEW.email := lower(NEW.email); RETURN NEW; END;",
		}},
		Triggers: []schemamodel.Trigger{
			{
				Name: "shared", Table: "users", Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
				ExecuteFunction: "public.normalize_email",
			},
			{
				Name: "owned", Table: "users", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
				Body: "NEW.email := lower(NEW.email); RETURN NEW;",
			},
		},
	}
}

// TestGenerateDownMigrationSQL_LeavesASharedTriggerFunctionInPlace drives the
// rollback of an addition through compare, reversal, planner and renderer. The
// rollback drops both triggers, the generated function of the one with a body,
// and no function under a generated name for the one that runs a declared
// function: that trigger never had one.
func TestGenerateDownMigrationSQL_LeavesASharedTriggerFunctionInPlace(t *testing.T) {
	c := qt.New(t)
	desired := sharedFunctionSchema()
	database := &catalog.Database{}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.TriggersAdded, qt.HasLen, 2)

	down, err := generateDownMigrationSQL(upDiff, desired, database, platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, `DROP TRIGGER IF EXISTS "shared" ON "users" CASCADE;`)
	c.Assert(down, qt.Not(qt.Contains), `"ptah_trigger_users_shared"`,
		qt.Commentf("the rollback drops a generated function the trigger never had\n%s", down))
	c.Assert(down, qt.Contains, `DROP TRIGGER IF EXISTS "owned" ON "users" CASCADE;`)
	c.Assert(down, qt.Contains, `DROP FUNCTION IF EXISTS "ptah_trigger_users_owned"();`,
		qt.Commentf("the trigger with a body still takes its generated function\n%s", down))
}

// TestGenerateUpMigrationSQL_DropsATriggerButNotTheFunctionItShares is the
// forward direction: the database holds a trigger that runs a declared
// function, and the desired schema no longer has the trigger. The database
// reader reports ExecuteFunction only for a function that is not the generated
// one, which is what the removal reads.
func TestGenerateUpMigrationSQL_DropsATriggerButNotTheFunctionItShares(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "SERIAL", Primary: true}},
	}
	// The shape the PostgreSQL reader produces for the schema it was pointed
	// at: no schema on the trigger, and the function each trigger runs by its
	// bare name, the generated one included.
	database := &catalog.Database{
		Tables: []catalog.Table{{Name: "users"}},
		Triggers: []catalog.Trigger{
			{
				Name: "shared", Table: "users", Timing: "BEFORE", Event: "INSERT",
				ForEach: "ROW", Body: "BEGIN NEW.email := lower(NEW.email); RETURN NEW; END;",
				ExecuteFunction: "normalize_email",
			},
			{
				Name: "owned", Table: "users", Timing: "BEFORE", Event: "UPDATE",
				ForEach: "ROW", Body: "BEGIN RETURN NEW; END;",
				ExecuteFunction: "ptah_trigger_users_owned",
			},
		},
	}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.TriggersRemoved, qt.HasLen, 2)

	up, err := generateUpMigrationSQL(upDiff, desired, platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, `DROP TRIGGER IF EXISTS "shared" ON "users" CASCADE;`)
	c.Assert(up, qt.Not(qt.Contains), `"ptah_trigger_users_shared"`,
		qt.Commentf("the plan drops a generated function the trigger never had\n%s", up))
	c.Assert(up, qt.Not(qt.Contains), `normalize_email`,
		qt.Commentf("the declared function stays\n%s", up))
	c.Assert(up, qt.Contains, `DROP FUNCTION IF EXISTS "ptah_trigger_users_owned"();`,
		qt.Commentf("a trigger running its generated function still takes it\n%s", up))
}

// TestReverseSchemaDiff_ARolledBackTriggerAdditionKeepsItsExecutedFunction pins
// the one fact of the declaration a removal keeps: which function the trigger
// runs when that function is declared on its own.
func TestReverseSchemaDiff_ARolledBackTriggerAdditionKeepsItsExecutedFunction(t *testing.T) {
	c := qt.New(t)

	removals := triggerRemovalsFromAdditions([]difftypes.TriggerRef{{
		TriggerName: "shared", TableName: "users",
		Desired: schemamodel.Trigger{
			Name: "shared", Table: "users", Timing: "BEFORE", Event: "INSERT",
			ForEach: "ROW", ExecuteFunction: "public.normalize_email",
		},
	}})

	c.Assert(removals, qt.HasLen, 1)
	c.Assert(removals[0].ExecuteFunction, qt.Equals, "public.normalize_email")
	c.Assert(removals[0].Desired, qt.DeepEquals, schemamodel.Trigger{})
}
