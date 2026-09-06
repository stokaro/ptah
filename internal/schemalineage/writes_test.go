package schemalineage_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemalineage"
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
END;`), "postgres")

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
END;`), "postgres")

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"audit:delete"})
}

// TestDeriveRoutines_AResolvedBodySaysWhatItsReadsRestOn is the honesty
// property the whole package turns on.
//
// Every statement here is classified, so the write list is complete. The reads
// are derived only from statements naming one table, and the entry says so: a
// routine reported with writes and no undecided entry would let "nothing reads
// customers.email" be concluded from a body whose other statements were never
// attributed.
func TestDeriveRoutines_AResolvedBodySaysWhatItsReadsRestOn(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(proceduralRoutine(`
DECLARE total INT;
BEGIN
  total := 0;
  UPDATE customers SET email = lower(email) WHERE id = 1;
  RAISE NOTICE 'done';
  RETURN;
END;`), "postgres")

	c.Assert(result.Writes, qt.HasLen, 1)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "every statement was classified")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "the reads are those of its statements that name one table")
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
END;`), "postgres")

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"audit:delete"})
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "EXECUTE composes its statement at run time")
	c.Assert(result.Undecided[0].Reason, qt.Contains, "the writes are not complete")
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
END;`), "postgres")

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
END;`), "postgres")

	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "every statement was classified")
}

// TestDeriveRoutines_ATriggerFunctionsFieldAssignmentIsNotAnUnresolvedStatement
// covers the shape a trigger function is mostly made of.
//
// PL/pgSQL accepts `=` as well as `:=`, the target is usually qualified, and
// the SQL lexer this borrows splits `:=` into two operator tokens. Each of the
// three on its own reported `NEW.updated_at = now()` as a statement beginning
// NEW that nothing recognized, which would make every trigger function partial.
//
// The assignment is not a write: it changes the row the statement that fired
// the trigger is already writing, and naming a table here would name one this
// body did not choose.
func TestDeriveRoutines_ATriggerFunctionsFieldAssignmentIsNotAnUnresolvedStatement(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "qualified with equals", body: "BEGIN NEW.updated_at = now(); RETURN NEW; END;"},
		{name: "qualified with colon equals", body: "BEGIN NEW.updated_at := now(); RETURN NEW; END;"},
		{name: "plain with equals", body: "BEGIN total = 0; END;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := schemalineage.DeriveRoutines(proceduralRoutine(test.body), "postgres")

			c.Assert(result.Writes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Contains, "every statement was classified")
		})
	}
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
END;`), "postgres")

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
END;`), "postgres")

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
END;`), "postgres")

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
	}, "postgres")

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Edges, qt.HasLen, 1)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// TestDeriveRoutines_AProcedureWithNoLanguageIsAnalyzedByItsDialect covers the
// routines the schema converter started handing this package.
//
// A SQL Server or MySQL routine has no LANGUAGE clause of its own and reaches
// the model recorded as plain SQL. Asked to be a single SELECT it simply fails,
// and the answer said the body was the wrong shape; what it is, is that
// dialect's procedural language, and the dialect is what says which parser
// reads it (stokaro/ptah#2435 made these reachable).
func TestDeriveRoutines_AProcedureWithNoLanguageIsAnalyzedByItsDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		body    string
	}{
		{name: "mysql", dialect: "mysql", body: "BEGIN UPDATE t SET c = 1; END"},
		{name: "mariadb", dialect: "mariadb", body: "BEGIN UPDATE t SET c = 1; END"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := schemalineage.DeriveRoutines(&schemamodel.Database{
				Functions: []schemamodel.Function{
					{Name: "p", Kind: "procedure", Language: "sql", Body: test.body},
				},
			}, test.dialect)

			c.Assert(writeTargets(result), qt.DeepEquals, []string{"t.c:update"})
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Contains, test.dialect)
		})
	}
}

// TestDeriveRoutines_ADialectWithNoRoutineBodyParserSaysSo is the fail-closed
// direction.
//
// Each dialect parses a routine body with its own grammar and there is no
// shared one to fall back on: a body read by the wrong splitter is not a
// conservative answer, it is a wrong one.
func TestDeriveRoutines_ADialectWithNoRoutineBodyParserSaysSo(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "p", Kind: "procedure", Language: "sql", Body: "UPDATE t SET c = 1;"},
		},
	}, "clickhouse")

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "no routine-body analysis exists for the clickhouse dialect")
}

// TestDeriveRoutines_AFunctionKeepsTheReadersOwnReason is the control.
//
// The reason a plain-SQL function did not resolve belongs to the reader that
// tried, and replacing it for every routine would lose the four shapes the
// view half reports by name.
func TestDeriveRoutines_AFunctionKeepsTheReadersOwnReason(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "f", Language: "sql", Body: "SELECT a FROM x JOIN y ON y.id = x.id"},
		},
	}, "postgres")

	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "more than one source")
}

// TestDeriveRoutines_ATSQLBodyResolvesTheWritesItsSplitterModels covers the
// three T-SQL writes whose statements arrive whole.
func TestDeriveRoutines_ATSQLBodyResolvesTheWritesItsSplitterModels(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "insert",
			body: "INSERT INTO audit (actor, action) VALUES (1, 'x');",
			want: []string{"audit.action:insert", "audit.actor:insert"},
		},
		{name: "delete", body: "DELETE FROM audit WHERE actor = 1;", want: []string{"audit:delete"}},
		{name: "truncate", body: "TRUNCATE TABLE scratch;", want: []string{"scratch:truncate"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := schemalineage.DeriveRoutines(&schemamodel.Database{
				Functions: []schemamodel.Function{
					{Name: "p", Kind: "procedure", Language: "sql", Body: test.body},
				},
			}, "sqlserver")

			c.Assert(writeTargets(result), qt.DeepEquals, test.want)
		})
	}
}

// TestDeriveRoutines_ATSQLUpdateResolvesNowThatItArrivesWhole replaces the test
// that pinned its failure.
//
// The T-SQL splitter used to treat every SET as the start of a statement, so
// `UPDATE t SET c = 1` arrived as `raw` = `UPDATE t` and `assignment` = `SET c
// = 1` and neither half was an update. The previous test asserted exactly that
// failure so the repair would be visible here rather than arriving unnoticed --
// and it was: it reddened the moment stokaro/ptah#2451 landed.
func TestDeriveRoutines_ATSQLUpdateResolvesNowThatItArrivesWhole(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "p", Kind: "procedure", Language: "sql", Body: "UPDATE t SET c = 1;"},
		},
	}, "sqlserver")

	c.Assert(writeTargets(result), qt.DeepEquals, []string{"t.c:update"})
}

// TestDeriveRoutines_AMySQLBranchIsUnresolvedRatherThanEmpty is the property
// that separates a modelled nesting from an unmodelled one.
//
// The PL/pgSQL body model carries the statements a control-flow statement holds
// (stokaro/ptah#2393), and the MySQL one does not: an IF arrives as one
// statement whose SQL is the whole branch. Reporting no writes for it would
// credit the routine with touching nothing inside a branch nobody opened, which
// is the confident wrong answer this package exists to avoid.
func TestDeriveRoutines_AMySQLBranchIsUnresolvedRatherThanEmpty(t *testing.T) {
	c := qt.New(t)

	result := schemalineage.DeriveRoutines(&schemamodel.Database{
		Functions: []schemamodel.Function{
			{
				Name: "p", Kind: "procedure", Language: "sql",
				Body: "BEGIN IF x > 0 THEN DELETE FROM audit WHERE actor = 1; END IF; END",
			},
		},
	}, "mysql")

	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Contains, "the if statement's contents could not be read")
}
