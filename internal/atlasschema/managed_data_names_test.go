package atlasschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

// TestPreparePlanFile_FindsTheDeclaredTableUnderTheEnginesNameRules looks the
// declared table up the way the engine compares names.
//
// Oracle folds the bare name the renderer wrote, so a declared `regions` is
// REGIONS in its catalog. SQLite compares table names without regard to ASCII
// case too, and that is what lets this run offline: the table exists as
// REGIONS and already holds the declared row. Compared exactly, the lookup
// never finds the table, never reads the row, and plans the INSERT again into
// a table that holds it.
func TestPreparePlanFile_FindsTheDeclaredTableUnderTheEnginesNameRules(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := managedDataConnection(c, "folded.db")
	_, err := conn.ExecContext(ctx,
		`CREATE TABLE REGIONS (code TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, rank INTEGER, note TEXT)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO REGIONS (code, name, rank) VALUES ('CZ', 'Czechia', 2)`)
	c.Assert(err, qt.IsNil)

	plan, err := atlasschema.PreparePlanFile(ctx, conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("CZ", "Czechia", 2)),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(planSQL(plan), qt.Not(qt.Contains), "INSERT INTO")
}
