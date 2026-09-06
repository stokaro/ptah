//go:build integration

package integration_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleRoutinesPlanAndConvergeE2E is the assertion the functions and
// procedures capability keys could not be flipped without.
//
// The keys promise that a declared routine is planned, rendered, introspected
// and compared, and the failure when one of the four is missing is not a
// compile error: it is a plan that reports the same pending change forever,
// because the reader never sees what the renderer made (stokaro/ptah#1920).
//
// Both lines run the same assertions. PL/SQL is not a 23 feature: the header,
// ALL_PROCEDURES, ALL_ARGUMENTS and ALL_SOURCE answered identically on
// 21.3.0.0.0 and on 23.26.2.0.0, and the only key that separates them for a
// routine is the existence guard on the DROP, which the renderer already takes
// from the preset.
//
// Three facts about the round trip decide whether it converges, and each is
// asserted rather than assumed:
//
//   - ALL_SOURCE stores the statement WITHOUT its CREATE OR REPLACE prefix, so
//     the body is what follows the header's IS. A reader that took the whole
//     text would compare a body against a body-plus-header, forever.
//   - The catalog folds every unquoted name to upper case, so the routine the
//     declaration calls fn_double comes back as FN_DOUBLE.
//   - ALL_ARGUMENTS reports the mode as IN, OUT or IN/OUT, and IN is what a
//     parameter written without one gets.
func TestOracleRoutinesPlanAndConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleRoutines(context.WithoutCancel(ctx), conn)
	defer dropOracleRoutines(context.WithoutCancel(ctx), conn)

	declared := oracleRoutineDeclaration()

	before, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, before, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)

	// Only the statements naming these three routines are executed. The
	// integration contour runs against ONE server, and a plan built from a
	// declaration that mentions three routines asks to drop every other routine
	// on it -- which would delete whatever a parallel test had just created.
	//
	// The filtered set is asserted at THREE, which is the non-vacuity check and
	// the shape the splitter has to get right: three statements for three
	// routines, not one per line of their bodies. The function with a
	// declaration section is what a blind split cut into four.
	creates := oracleStatementsNamingRoutines(statements)
	c.Assert(creates, qt.HasLen, 3)
	for _, statement := range creates {
		c.Assert(statement, qt.Contains, "CREATE OR REPLACE")
		// Executed exactly as planned, terminator included. In PL/SQL the
		// semicolon after END belongs to the block: handing the server the
		// statement without it returns NO error and leaves the routine INVALID,
		// which is why the split keeps it and this does not trim it off again.
		c.Assert(strings.HasSuffix(statement, "END;"), qt.IsTrue)
		execOracle(ctx, c, conn, statement)
	}

	after, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// What the catalog gives back, spelled out. The names are upper case
	// because they were written unquoted; the parameters carry the mode the
	// catalog reports; the bodies are byte for byte what was declared.
	c.Assert(oracleRoutineSummary(after), qt.DeepEquals, []string{
		"FN_DEC function(p in number) returns number DEFINER VOLATILE " +
			"body=[x NUMBER := 0;\nBEGIN\n  x := p * 3;\n  RETURN x;\nEND;]",
		"FN_DOUBLE function(p in number) returns number INVOKER IMMUTABLE " +
			"body=[BEGIN\n  RETURN p * 2;\nEND;]",
		"PR_TOUCH procedure(a in number, b out number) returns  DEFINER VOLATILE " +
			"body=[BEGIN\n  b := a;\nEND;]",
	})

	// Every routine COMPILED. This is the assertion that separates "the server
	// accepted the statement" from "the routine exists": measured on
	// 23.26.2.0.0, a CREATE handed over without the semicolon that closes its
	// PL/SQL block returns no driver error, leaves USER_OBJECTS.STATUS at
	// INVALID, and is not listed by USER_PROCEDURES at all.
	c.Assert(oracleInvalidObjects(ctx, c, conn), qt.HasLen, 0)

	// The loop closes: a second comparison against the same declaration plans
	// nothing. This is the assertion a missing reader, a missing fold or a
	// mis-taken body all fail.
	settled, err := schemadiff.CompareWithDatabase(ctx, conn, declared, after, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.ProceduresRemoved, qt.HasLen, 0)

	// And the removal direction, whose verb has to match the object: measured,
	// DROP FUNCTION on a procedure answers ORA-04043.
	teardown, err := schemadiff.CompareWithDatabase(ctx, conn, &schemamodel.Database{}, after, nil)
	c.Assert(err, qt.IsNil)
	teardownStatements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(teardown, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	drops := oracleStatementsNamingRoutines(teardownStatements)
	c.Assert(drops, qt.HasLen, 3)
	c.Assert(oracleStatementsNaming(drops, "DROP PROCEDURE"), qt.HasLen, 1)
	for _, statement := range drops {
		execOracle(ctx, c, conn, statement)
	}

	empty, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleRoutineSummary(empty), qt.HasLen, 0)
}

// oracleRoutineDeclaration declares the three routines the round trip uses.
//
// They differ on every axis the model carries: kind, volatility, security, the
// argument modes, and whether the body has a declaration section. A fixture
// varying one of them would leave the others unmeasured.
func oracleRoutineDeclaration() *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{
		{
			StructName: "R", Name: "fn_double",
			Parameters: "p IN NUMBER", Returns: "NUMBER", Language: "plsql",
			Security: "INVOKER", Volatility: "IMMUTABLE",
			Body: "BEGIN\n  RETURN p * 2;\nEND;",
		},
		{
			StructName: "R", Name: "fn_dec",
			Parameters: "p IN NUMBER", Returns: "NUMBER", Language: "plsql",
			Security: "DEFINER", Volatility: "VOLATILE",
			Body: "x NUMBER := 0;\nBEGIN\n  x := p * 3;\n  RETURN x;\nEND;",
		},
		{
			StructName: "R", Name: "pr_touch", Kind: schemamodel.FunctionKindProcedure,
			Parameters: "a IN NUMBER, b OUT NUMBER", Language: "plsql",
			Security: "DEFINER", Volatility: "VOLATILE",
			Body: "BEGIN\n  b := a;\nEND;",
		},
	}}
}

// dropOracleRoutines removes what this test creates, tolerating what is absent.
//
// The guard is in Go rather than in the SQL because DROP FUNCTION IF EXISTS is
// accepted on 23 and is ORA-00933 on 21, and this runs on both.
func dropOracleRoutines(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{
		"DROP FUNCTION fn_double", "DROP FUNCTION fn_dec", "DROP PROCEDURE pr_touch",
	} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}

// oracleRoutineSummary renders each read routine in one line, sorted by name.
func oracleRoutineSummary(read *catalog.Database) []string {
	summary := make([]string, 0, len(read.Functions))
	for _, function := range read.Functions {
		if !oracleDeclaredRoutineNames[strings.ToLower(function.Name)] {
			continue
		}
		kind := function.Kind
		if kind == "" {
			kind = schemamodel.FunctionKindFunction
		}
		summary = append(summary, strings.Join([]string{
			function.Name + " " + kind + "(" + function.Parameters + ")",
			"returns " + function.Returns,
			function.Security,
			function.Volatility,
			"body=[" + function.Body + "]",
		}, " "))
	}
	sort.Strings(summary)
	return summary
}

// oracleDeclaredRoutineNames is the set this test owns on a shared server.
var oracleDeclaredRoutineNames = map[string]bool{
	"fn_double": true, "fn_dec": true, "pr_touch": true,
}

// oracleStatementsNamingRoutines keeps the planned statements about the
// routines this test declares, and drops the ones about anybody else's.
func oracleStatementsNamingRoutines(statements []string) []string {
	var found []string
	for _, statement := range statements {
		for name := range oracleDeclaredRoutineNames {
			if strings.Contains(strings.ToLower(statement), name) {
				found = append(found, statement)
				break
			}
		}
	}
	sort.Strings(found)
	return found
}

// TestOracleTriggerCompilesE2E is the routine test's other half, for the kind
// that reached the same hazard first.
//
// A trigger's body is PL/SQL too, and its block ends with the same semicolon.
// Nothing downstream notices when it is missing: the driver reports no error,
// and USER_TRIGGERS reports the trigger ENABLED whether it compiled or not --
// measured on 23.26.2.0.0, ZZ_TR_NOSEMI was ENABLED and INVALID at once. Only
// USER_OBJECTS.STATUS says so, which is why this test asks it.
func TestOracleTriggerCompilesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleTriggerFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleTriggerFixture(context.WithoutCancel(ctx), conn)

	execOracle(ctx, c, conn, "CREATE TABLE ora_tr_items (id NUMBER PRIMARY KEY, n NUMBER)")

	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "ora_tr_items"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "NUMBER", Primary: true},
			{StructName: "T", Name: "n", Type: "NUMBER"},
		},
		Triggers: []schemamodel.Trigger{{
			StructName: "T", Name: "ora_tr_set_n", Table: "ora_tr_items",
			Timing: "BEFORE", Event: "INSERT", ForEach: "ROW",
			Body: "BEGIN\n  :NEW.n := 1;\nEND;",
		}},
	}

	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)

	triggers := oracleStatementsNaming(statements, "CREATE TRIGGER")
	c.Assert(triggers, qt.HasLen, 1)
	c.Assert(strings.HasSuffix(strings.TrimSpace(triggers[0]), "END;"), qt.IsTrue)
	execOracle(ctx, c, conn, triggers[0])

	c.Assert(oracleInvalidObjects(ctx, c, conn), qt.HasLen, 0)
}

// dropOracleTriggerFixture removes what the trigger test creates. Dropping the
// table takes the trigger with it.
func dropOracleTriggerFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	_ = conn.SchemaWriter().ExecuteSQL(ctx, "DROP TABLE ora_tr_items PURGE")
}

// oracleInvalidObjects names every object in this schema the server could not
// compile.
//
// USER_OBJECTS.STATUS is the only place an uncompilable PL/SQL object shows up:
// the driver reports success, USER_TRIGGERS reports ENABLED, and
// USER_PROCEDURES simply omits the routine.
func oracleInvalidObjects(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx,
		"SELECT object_name, object_type FROM user_objects WHERE status <> 'VALID' ORDER BY object_name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var invalid []string
	for rows.Next() {
		var name, objectType string
		c.Assert(rows.Scan(&name, &objectType), qt.IsNil)
		invalid = append(invalid, objectType+" "+name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return invalid
}
