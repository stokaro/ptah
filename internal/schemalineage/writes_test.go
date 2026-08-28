package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemalineage"
)

// proceduralRoutine builds a schema holding one PL/pgSQL routine.
func proceduralRoutine(body string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "customers"}},
		Fields: []schemamodel.Field{{StructName: "C", Name: "email", Type: "TEXT"}},
		Functions: []schemamodel.Function{
			{Name: "r", Language: "plpgsql", Body: body},
		},
	}
}

// writeTargets renders a result's writes as "table.column:statement" so a test
// asserts the whole list at once.
func writeTargets(result schemalineage.RoutineResult) []string {
	targets := make([]string, 0, len(result.Writes))
	for _, write := range result.Writes {
		name := write.Table
		if write.Column != "" {
			name += "." + write.Column
		}
		targets = append(targets, name+":"+write.Statement)
	}
	return targets
}

// TestDeriveRoutines_ResolvesTheWritesAProceduralBodyPerforms is what #2394
// asks for on the write side.
//
// The four writing statements name their target plainly, and a body's writes
// are the part of it that can be resolved without the references a read needs.
func TestDeriveRoutines_ResolvesTheWritesAProceduralBodyPerforms(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  UPDATE customers SET country = 'CZ', email = lower(email) WHERE id = 1;
  INSERT INTO audit (actor, action) VALUES (1, 'touch');
  DELETE FROM stale WHERE id = 1;
  TRUNCATE TABLE scratch;
END;`))

	c.Assert(writeTargets(result), qt.DeepEquals, []string{
		"audit.action:insert",
		"audit.actor:insert",
		"customers.country:update",
		"customers.email:update",
		"scratch:truncate",
		"stale:delete",
	})
}

// TestDeriveRoutines_ReachesAWriteInsideControlFlow is the property #2393
// established for the lint rule, asserted here for lineage.
//
// A DELETE inside an IF is a DELETE. A walk that stopped at the top level would
// report the routine as touching nothing inside the branch.
func TestDeriveRoutines_ReachesAWriteInsideControlFlow(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  IF now() > '2020-01-01' THEN
    DELETE FROM audit WHERE actor = 1;
  END IF;
END;`))

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"audit:delete"})
}

// TestDeriveRoutines_AResolvedBodyStillSaysItsReadsAreNot is the honesty
// property the whole package turns on.
//
// Every statement here is classified, so the write list is complete. The reads
// are not derived at all, and a routine reported with writes and no undecided
// entry would let "nothing reads customers.email" be concluded from a body that
// was never read for that question.
func TestDeriveRoutines_AResolvedBodyStillSaysItsReadsAreNot(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
DECLARE total INT;
BEGIN
  total := 0;
  UPDATE customers SET email = lower(email) WHERE id = 1;
  RAISE NOTICE 'done';
  RETURN;
END;`))

	c.Assert(result.Writes, qt.HasLen, 1)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "every statement was classified")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "the columns it reads are not resolved")
}

// TestDeriveRoutines_DynamicSQLMakesTheWriteListIncomplete is the #1270 case.
//
// An EXECUTE composes its statement at run time and can write anything. A write
// list gathered around one must say it is partial, or a caller reads it as the
// complete set of tables the routine touches.
func TestDeriveRoutines_DynamicSQLMakesTheWriteListIncomplete(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  DELETE FROM audit WHERE actor = 1;
  EXECUTE 'DROP TABLE ' || quote_ident(target);
END;`))

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"audit:delete"})
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "EXECUTE composes its statement at run time")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "neither the writes nor the columns it reads are complete")
}

// TestDeriveRoutines_AnUnrecognizedStatementIsNotTreatedAsHarmless covers the
// fail-closed direction.
//
// CALL, MERGE and COPY all write, so a leading word this analysis does not know
// has to widen the answer rather than be skipped.
func TestDeriveRoutines_AnUnrecognizedStatementIsNotTreatedAsHarmless(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  CALL rebuild_everything();
END;`))

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "beginning CALL was not recognized")
}

// TestDeriveRoutines_AVariableAssignmentIsNotAnUnresolvedStatement is the
// control for the previous test.
//
// A fail-closed rule that fires on ordinary PL/pgSQL reports every routine as
// unresolved, which carries the same information as reporting none of them.
// `:=` is two operator tokens to the SQL lexer, which is how this went wrong.
func TestDeriveRoutines_AVariableAssignmentIsNotAnUnresolvedStatement(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
DECLARE total INT;
BEGIN
  total := 0;
END;`))

	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "every statement was classified")
}

// TestDeriveRoutines_ASetClauseDoesNotInventColumnsFromAnExpression pins the
// depth tracking.
//
// `SET a = coalesce(b, c)` assigns one column. Reading the call's arguments as
// assigned columns would report two columns the statement never writes, and a
// wrong write is worse than an unresolved one.
func TestDeriveRoutines_ASetClauseDoesNotInventColumnsFromAnExpression(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  UPDATE customers SET email = coalesce(fallback, backup) WHERE id = 1;
END;`))

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.email:update"})
}

// TestDeriveRoutines_ATruncateListIsRefusedRatherThanHalfRead keeps a dropped
// name from reading as an untouched table.
//
// TRUNCATE accepts a list. Reading the first name and stopping would report one
// table emptied and say nothing at all about the other.
func TestDeriveRoutines_ATruncateListIsRefusedRatherThanHalfRead(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  TRUNCATE TABLE first_table, second_table;
END;`))

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "beginning TRUNCATE was not recognized")
}

// TestDeriveRoutines_AnInsertWithNoColumnListNamesTheTable separates the two
// meanings an empty column carries.
//
// `INSERT INTO t VALUES (...)` writes every column of t. Reporting it with an
// empty column says "the whole table", which is true; inventing a column name
// would not be.
func TestDeriveRoutines_AnInsertWithNoColumnListNamesTheTable(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
BEGIN
  INSERT INTO audit VALUES (1, 'touch');
END;`))

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"audit:insert"})
	c.Assert(result.Writes[0].Column, qt.Equals, "")
}

// TestDeriveRoutines_APlainSQLRoutineReportsNoWrites is the control separating
// the two paths.
//
// The `LANGUAGE sql` path resolves reads and is not asked about writes; a write
// appearing there would mean the procedural classifier ran on a body the read
// path already answered for.
func TestDeriveRoutines_APlainSQLRoutineReportsNoWrites(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "customers"}},
		Fields: []schemamodel.Field{{StructName: "C", Name: "email", Type: "TEXT"}},
		Functions: []schemamodel.Function{
			{Name: "r", Language: "sql", Body: "SELECT email FROM customers"},
		},
	})

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Edges, qt.HasLen, 1)
	c.Assert(result.Undecided, qt.HasLen, 0)
}
