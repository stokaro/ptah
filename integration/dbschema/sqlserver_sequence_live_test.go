//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveSequenceRoundTrip is the test the Sequences capability could
// not have been flipped without.
//
// The key promises three things at once -- Ptah renders the object, reads it
// back, and plans it again -- and the failure mode when one is missing is not a
// compile error. It is an apply loop that emits the same CREATE forever,
// because the reader never sees what the renderer made. No offline test can
// catch that: the fixture on both sides is written by the same hand.
//
// So this applies the rendered statement to a real server, reads the catalog
// back through the shipping reader, and asks the shipping comparator whether
// anything is left to do. The answer has to be nothing (stokaro/ptah#1626).
func TestSQLServerLiveSequenceRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_seq_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		for _, name := range []string{"order_number_seq", "plain_seq", "capped_seq"} {
			_, _ = conn.ExecContext(ctx, "DROP SEQUENCE IF EXISTS "+quoted+"."+quoteSQLServerIdentifier(name))
		}
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	description := sqlServerSequenceSchema(schemaName)

	// 1. The renderer's statements are the ones the server is given. Nothing is
	// hand-written here, so a statement this engine refuses fails the test
	// rather than being quietly corrected.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "CREATE SEQUENCE")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it actually holds.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	byName := make(map[string]dbschematypes.DBSequence)
	for _, sequence := range live.Sequences {
		byName[sequence.Name] = sequence
	}
	c.Assert(byName, qt.HasLen, 3)

	ordered := byName["order_number_seq"]
	c.Assert(ordered.DataType, qt.Equals, "bigint")
	c.Assert(*ordered.Start, qt.Equals, int64(1000))
	c.Assert(*ordered.Increment, qt.Equals, int64(5))
	c.Assert(ordered.Cycle, qt.IsTrue)
	c.Assert(*ordered.Cache, qt.Equals, int64(20))

	// A declaration that named nothing still comes back fully populated: the
	// engine resolved the type's bounds at creation time and sys.sequences has
	// no column saying which of them the statement wrote. That is exactly why
	// step 3 is the assertion that matters.
	plain := byName["plain_seq"]
	c.Assert(*plain.MinValue, qt.Equals, int64(-9223372036854775808))
	c.Assert(*plain.MaxValue, qt.Equals, int64(9223372036854775807))

	// 3. The convergence assertion. Comparing the same description against what
	// the server now holds must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(settled.SequencesAdded, qt.HasLen, 0)
	c.Assert(settled.SequencesRemoved, qt.HasLen, 0)
	c.Assert(settled.SequencesModified, qt.HasLen, 0)

	// 4. And a real change plans, renders and applies. INCREMENT BY is chosen
	// because ALTER SEQUENCE accepts it, unlike AS and START WITH.
	changed := sqlServerSequenceSchema(schemaName)
	newIncrement := int64(7)
	changed.Sequences[0].Increment = &newIncrement
	modification := schemadiff.CompareWithDialect(changed, live, platform.SQLServer)
	c.Assert(modification.SequencesModified, qt.HasLen, 1)
	nodes, err := planner.GenerateSchemaDiffAST(modification, changed, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	alters := renderedStatementsNaming(c, nodes, "ALTER SEQUENCE")
	c.Assert(alters, qt.HasLen, 1)
	_, err = conn.ExecContext(ctx, alters[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", alters[0]))

	after, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(*sequenceNamed(after.Sequences, "order_number_seq").Increment, qt.Equals, int64(7))
}

// renderedStatementsNaming renders the planned nodes and keeps the ones naming
// a keyword.
func renderedStatementsNaming(c *qt.C, nodes []ast.Node, keyword string) []string {
	c.Helper()
	kept := make([]string, 0, len(nodes))
	for _, node := range nodes {
		sql, err := renderer.RenderSQL(platform.SQLServer, node)
		c.Assert(err, qt.IsNil)
		if strings.Contains(sql, keyword) {
			kept = append(kept, sql)
		}
	}
	return kept
}

// sequenceNamed returns the sequence a catalog read reports under a name.
func sequenceNamed(sequences []dbschematypes.DBSequence, name string) dbschematypes.DBSequence {
	for _, sequence := range sequences {
		if sequence.Name == name {
			return sequence
		}
	}
	return dbschematypes.DBSequence{}
}

// TestSQLServerLiveSequenceRefusesWhatTheRendererDeclines pins that the two
// clauses the renderer reports instead of emitting are ones the engine really
// refuses, rather than rules this repository invented.
func TestSQLServerLiveSequenceRefusesWhatTheRendererDeclines(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("[dbo].[ptah_refused_%d]", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, "CREATE SEQUENCE "+name+" AS bigint START WITH 1 INCREMENT BY 1")
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = conn.ExecContext(ctx, "DROP SEQUENCE IF EXISTS "+name) }()

	tests := []struct {
		name      string
		statement string
		wantError string
	}{
		{
			name:      "alter cannot change the type",
			statement: "ALTER SEQUENCE " + name + " AS int",
			wantError: "'AS' cannot be used in an ALTER SEQUENCE statement",
		},
		{
			name:      "alter spells it RESTART WITH",
			statement: "ALTER SEQUENCE " + name + " START WITH 5",
			wantError: "'START WITH' cannot be used in an ALTER SEQUENCE statement",
		},
		{
			name:      "a cache of zero is not a cache size",
			statement: "ALTER SEQUENCE " + name + " CACHE 0",
			wantError: "must be greater than 0",
		},
		{
			name:      "there is no CASCADE",
			statement: "DROP SEQUENCE " + name + " CASCADE",
			wantError: "Incorrect syntax near the keyword 'CASCADE'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, execErr := conn.ExecContext(ctx, test.statement)
			c.Assert(execErr, qt.IsNotNil)
			c.Assert(execErr.Error(), qt.Contains, test.wantError)
		})
	}

	// The control: the spelling the renderer DOES emit is accepted, so the four
	// refusals above are about those clauses rather than about ALTER SEQUENCE.
	_, err = conn.ExecContext(ctx, "ALTER SEQUENCE "+name+" RESTART WITH 5 INCREMENT BY 2")
	c.Assert(err, qt.IsNil)
}

// sqlServerSequenceSchema declares three sequences: one naming every option,
// one naming none, and one bounded.
func sqlServerSequenceSchema(schemaName string) *goschema.Database {
	start, increment, cache := int64(1000), int64(5), int64(20)
	minValue, maxValue := int64(1), int64(9999)
	return &goschema.Database{
		Sequences: []goschema.Sequence{
			{
				StructName: "Order", Name: "order_number_seq", Schema: schemaName,
				AsType: "bigint", Start: &start, Increment: &increment, Cache: &cache, Cycle: true,
			},
			{StructName: "Plain", Name: "plain_seq", Schema: schemaName},
			{
				StructName: "Capped", Name: "capped_seq", Schema: schemaName,
				AsType: "int", MinValue: &minValue, MaxValue: &maxValue,
			},
		},
	}
}
