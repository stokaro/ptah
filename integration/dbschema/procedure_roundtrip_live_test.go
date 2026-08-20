//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPostgresLiveProcedureConverges is what stokaro/ptah#1722 asked for on the
// PostgreSQL side.
//
// A procedure was filtered out of every read -- `AND p.prokind = 'f'`, with no
// diagnostic -- so a database holding one produced a clean diff and the
// operator was told a whole object kind matched when it had never been looked
// at. Only a live round trip shows the filter is gone: the read has to return
// the procedure, the comparison has to leave it alone, and a removal has to
// emit the verb the server takes.
func TestPostgresLiveProcedureConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_proc_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()
	_, err = conn.ExecContext(ctx, `SET search_path TO "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)

	declared := &goschema.Database{Functions: []goschema.Function{{
		Name:       schemaName + ".bump",
		Kind:       goschema.FunctionKindProcedure,
		Parameters: "n integer",
		Language:   "sql",
		Security:   "INVOKER",
		Body:       "SELECT n",
	}}}

	// 1. The creation is planned and the server takes it. A CREATE PROCEDURE
	//    carrying RETURNS would not parse, which is the grammar difference the
	//    kind exists for.
	created := compareLiveRoutines(c, ctx, conn, declared, schemaName)
	c.Assert(created.FunctionsAdded, qt.HasLen, 1)
	applyLiveRoutines(c, ctx, conn, created, declared, platform.Postgres)

	// 2. The read returns it, so the comparison is empty. Before this the read
	//    dropped it and the next comparison asked for the same CREATE forever.
	converged := compareLiveRoutines(c, ctx, conn, declared, schemaName)
	c.Assert(converged.FunctionsAdded, qt.HasLen, 0)
	c.Assert(converged.FunctionsModified, qt.HasLen, 0)
	c.Assert(converged.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(converged.ProceduresRemoved, qt.HasLen, 0)

	// 3. It is a working procedure, not merely a catalog row that reads back.
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CALL %q."bump"(1)`, schemaName))
	c.Assert(err, qt.IsNil)

	// 4. A removal is reported as a procedure and dropped with the verb the
	//    server takes. DROP FUNCTION aimed at one is refused by name.
	removed := compareLiveRoutines(c, ctx, conn, &goschema.Database{}, schemaName)
	c.Assert(removed.ProceduresRemoved, qt.HasLen, 1)
	c.Assert(removed.FunctionsRemoved, qt.HasLen, 0)
	applyLiveRoutines(c, ctx, conn, removed, &goschema.Database{}, platform.Postgres)

	c.Assert(compareLiveRoutines(c, ctx, conn, &goschema.Database{}, schemaName).ProceduresRemoved, qt.HasLen, 0)
}

// TestMySQLLiveProcedureConverges is the same round trip on the other family,
// whose reader filtered `ROUTINE_TYPE = 'FUNCTION'` for the same reason and
// with the same silence.
func TestMySQLLiveProcedureConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MySQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_proc_%d", time.Now().Unix())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "DROP PROCEDURE IF EXISTS `"+name+"`")
	}()

	declared := &goschema.Database{Functions: []goschema.Function{{
		Name:       name,
		Kind:       goschema.FunctionKindProcedure,
		Parameters: "n int",
		Language:   "sql",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "SELECT n",
	}}}

	created := compareLiveRoutines(c, ctx, conn, declared, "")
	c.Assert(created.FunctionsAdded, qt.HasLen, 1)
	applyLiveRoutines(c, ctx, conn, created, declared, platform.MySQL)

	converged := compareLiveRoutines(c, ctx, conn, declared, "")
	c.Assert(converged.FunctionsAdded, qt.HasLen, 0)
	c.Assert(converged.FunctionsModified, qt.HasLen, 0)

	_, err = conn.ExecContext(ctx, "CALL `"+name+"`(1)")
	c.Assert(err, qt.IsNil)
}

// compareLiveRoutines reads the schema and compares it through the connection.
func compareLiveRoutines(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
	schemaName string,
) *difftypes.SchemaDiff {
	c.Helper()
	schemas := []string{schemaName}
	if schemaName == "" {
		schemas = nil
	}
	current, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, current, nil)
	c.Assert(err, qt.IsNil)
	return diff
}

// applyLiveRoutines plans the diff and runs every statement it produces.
func applyLiveRoutines(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	diff *difftypes.SchemaDiff,
	declared *goschema.Database,
	dialect string,
) {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, declared, dialect, conn.Info().Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}
