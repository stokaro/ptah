package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// TestGenerateMigrationSpendsConnectTimeoutOnTheConnect pins that the budget
// reaches the connect.
//
// It is the control for the fix that stopped the same budget reaching
// everything else. `migrations generate --db-url` used to run under a context
// derived from --connect-timeout, so its 10s default bounded planning,
// rendering and file publication as well; on a slow runner that expired during
// publication and surfaced as `error creating migration files: context
// deadline exceeded` (stokaro/ptah#1749).
//
// Moving the budget into an option could have removed it instead of scoping
// it, and nothing about a successful run would say which. A nanosecond is
// expired before it is read, so a connect that still honours it cannot
// succeed, and the assertion does not depend on how fast the machine is.
func TestGenerateMigrationSpendsConnectTimeoutOnTheConnect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	entities := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)

	_, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir:  entities,
		DatabaseURL:    "sqlite:///" + filepath.Join(dir, "ptah.db"),
		MigrationName:  "init",
		OutputDir:      filepath.Join(dir, "migrations"),
		ConnectTimeout: time.Nanosecond,
	})

	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`,
		qt.Commentf("an expired connect budget must stop the connect"))
}
