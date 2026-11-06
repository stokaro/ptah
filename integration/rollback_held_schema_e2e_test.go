//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestCompatMigrateDiffE2E_ARollbackCreatesNoSchemaTheDatabaseHolds writes a table's drop
// and its rollback on PostgreSQL, runs both and reads the table back. The forward plan
// keeps the schema, so the rollback that puts the table back holds no CREATE SCHEMA: a
// rollback reads as the forward file undone, and the forward file created none.
func TestCompatMigrateDiffE2E_ARollbackCreatesNoSchemaTheDatabaseHolds(t *testing.T) {
	c := qt.New(t)
	const (
		first  = "CREATE SCHEMA held;\nCREATE TABLE held.kept (id int PRIMARY KEY);\nCREATE TABLE held.gone (id int PRIMARY KEY);\n"
		second = "CREATE SCHEMA held;\nCREATE TABLE held.kept (id int PRIMARY KEY);\n"
		read   = "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_name = 'gone'"
	)
	dev := postgresScratchDevURL(c, "ptah_held_schema_dev")
	target := postgresScratchDevURL(c, "ptah_held_schema")

	pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{first, second}))

	c.Assert(pairs, qt.HasLen, 2)
	c.Assert(pairs[1].Down, qt.Not(qt.Contains), "SCHEMA")
	applyEngineFile(c, target, platform.Postgres, pairs[0].Up)
	applyEngineFile(c, target, platform.Postgres, pairs[1].Up)
	applyEngineFile(c, target, platform.Postgres, pairs[1].Down)
	c.Assert(heldColumns(c, target, read), qt.Equals, "held.gone")
}
