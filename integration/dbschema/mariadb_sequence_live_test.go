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

// TestMariaDBLiveSequenceConverges is the test the Sequences capability could
// not be flipped without.
//
// The key was false with a recorded reason that names exactly what was missing:
// MariaDB the engine has had SEQUENCE objects since 10.3, and Ptah had no
// introspection and no MySQL-family planning for them, so a rendered
// CREATE SEQUENCE would have been a statement `schema apply` never plans and a
// reader never sees again. The key describes the generator, so it may only be
// claimed where all three halves exist -- and only a live round trip shows they
// do (stokaro/ptah#1759).
//
// Two failures a fixture could not catch are pinned here:
//
//   - The grammar is MariaDB's own. `NO CYCLE` is ERROR 1064 near 'CYCLE' on
//     12.3, so a plan carrying the PostgreSQL spelling renders at exit 0 and
//     fails on apply.
//   - The cache size is not in information_schema.SEQUENCES. Reading the
//     sequence's own row is the only way to see it, and a declaration that
//     names CACHE would otherwise ask for the same change forever.
func TestMariaDBLiveSequenceConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.MariaDB)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_seq_%d", time.Now().UnixNano())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), fmt.Sprintf("DROP SEQUENCE IF EXISTS `%s`", name))
	}()

	declared := func(cache int64) *goschema.Database {
		return declaredSequence(name, cache, false)
	}

	// 1. The creation is planned and the server takes it. Every option is
	//    named, so every option is one the comparison will hold Ptah to.
	created := compareLiveSequences(c, ctx, conn, declared(42))
	c.Assert(created.SequencesAdded, qt.DeepEquals, []string{name})
	applyLiveStatements(c, ctx, conn, created, declared(42), platform.MariaDB)

	// 2. The read describes what was created, cache included -- the option
	//    information_schema.SEQUENCES does not carry.
	converged := compareLiveSequences(c, ctx, conn, declared(42))
	c.Assert(converged.SequencesAdded, qt.HasLen, 0)
	c.Assert(converged.SequencesModified, qt.HasLen, 0)
	c.Assert(converged.SequencesRemoved, qt.HasLen, 0)

	// 3. A changed option is one change, and the ALTER the server takes.
	changed := compareLiveSequences(c, ctx, conn, declared(11))
	c.Assert(changed.SequencesModified, qt.HasLen, 1)
	applyLiveStatements(c, ctx, conn, changed, declared(11), platform.MariaDB)

	// 4. And the comparison is empty again, which is the property the key rests
	//    on: a plan that converges rather than one that asks forever.
	settled := compareLiveSequences(c, ctx, conn, declared(11))
	c.Assert(settled.SequencesModified, qt.HasLen, 0)

	// 5. The sequence is a working one, not merely a row that reads back.
	var next int64
	err = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT NEXTVAL(`%s`)", name)).Scan(&next)
	c.Assert(err, qt.IsNil)
	c.Assert(next, qt.Equals, int64(7))

	// 6. CYCLE on, then off again. The second half is the one that pins the
	//    spelling: an ALTER turning cycling off has to say NOCYCLE, and
	//    `NO CYCLE` is ERROR 1064 near 'CYCLE' on this engine. Nothing before
	//    this line emits the negative form at all, so without this step the
	//    PostgreSQL spelling would render, plan and pass every offline check.
	cycling := declaredSequence(name, 11, true)
	turnedOn := compareLiveSequences(c, ctx, conn, cycling)
	c.Assert(turnedOn.SequencesModified, qt.HasLen, 1)
	applyLiveStatements(c, ctx, conn, turnedOn, cycling, platform.MariaDB)

	notCycling := declaredSequence(name, 11, false)
	turnedOff := compareLiveSequences(c, ctx, conn, notCycling)
	c.Assert(turnedOff.SequencesModified, qt.HasLen, 1)
	applyLiveStatements(c, ctx, conn, turnedOff, notCycling, platform.MariaDB)

	c.Assert(compareLiveSequences(c, ctx, conn, notCycling).SequencesModified, qt.HasLen, 0)
}

// declaredSequence is the declaration under test, with the two options each
// step varies.
func declaredSequence(name string, cache int64, cycle bool) *goschema.Database {
	start, increment, minValue, maxValue := int64(7), int64(3), int64(1), int64(900)
	return &goschema.Database{Sequences: []goschema.Sequence{{
		Name:      name,
		AsType:    "bigint",
		Start:     &start,
		Increment: &increment,
		MinValue:  &minValue,
		MaxValue:  &maxValue,
		Cache:     &cache,
		Cycle:     cycle,
	}}}
}

// compareLiveSequences reads the connected database and compares it with the
// declaration.
func compareLiveSequences(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
) *difftypes.SchemaDiff {
	c.Helper()
	current, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, current, nil)
	c.Assert(err, qt.IsNil)
	return diff
}

// applyLiveStatements plans the diff and runs every statement it produces.
func applyLiveStatements(
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
