//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// A derived rollback runs statements the migrator never sees, and moves the
// revision boundary all the same. The log is what is left of the versions it
// deleted, so a run recorded nothing would leave a database that reads exactly
// like one that was never at those versions (stokaro/ptah#3406).
func TestMigrationsDownPlanRecordsItsRollbackInTheLogE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()
	testID := time.Now().UnixNano()
	targetDBName := fmt.Sprintf("ptah_down_plan_target_e2e_%d", testID)
	shadowDBName := fmt.Sprintf("ptah_down_plan_shadow_e2e_%d", testID)
	createE2EDatabase(c, ctx, adminDB, targetDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, targetDBName)
	createE2EDatabase(c, ctx, adminDB, shadowDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, shadowDBName)
	targetDBURL := replaceDatabaseName(c, dbURL, targetDBName)
	shadowDBURL := replaceDatabaseName(c, dbURL, shadowDBName)

	migrationsDir := writeDownPlanMigrations(c, t.TempDir())
	upOutput, err := runPtah(ctx, repoRoot, binaryPath,
		"migrations", "up", "--db-url", targetDBURL, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", upOutput))

	downOutput, err := runPtah(ctx, repoRoot, binaryPath,
		"migrations", "down", "--plan",
		"--db-url", targetDBURL,
		"--shadow-db", shadowDBURL,
		"--migrations-dir", migrationsDir,
		"--target", "1",
		"--confirm")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", downOutput))

	logOutput, err := runPtah(ctx, repoRoot, binaryPath,
		"migrations", "log", "--db-url", targetDBURL, "--json")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", logOutput))
	attempts := decodeMigrationLogJSON(c, logOutput)
	c.Assert(attempts[0].Operation, qt.Equals, "down")
	c.Assert(attempts[0].Version, qt.Equals, int64(2))
	c.Assert(attempts[0].Outcome, qt.Equals, "rolled_back")
	// The control: the apply that put version 2 there is still recorded, so the
	// assertion above measures a rollback entry rather than any entry at all.
	c.Assert(attempts[1].Operation, qt.Equals, "up")
	c.Assert(attempts[1].Outcome, qt.Equals, "applied")
}

// migrationLogJSONAttempt is the shape `ptah migrations log --json` publishes,
// read here rather than imported so the test measures the output an operator
// parses.
type migrationLogJSONAttempt struct {
	Operation string `json:"operation"`
	Version   int64  `json:"version"`
	Outcome   string `json:"outcome"`
}

func decodeMigrationLogJSON(c *qt.C, output string) []migrationLogJSONAttempt {
	c.Helper()
	var attempts []migrationLogJSONAttempt
	c.Assert(json.Unmarshal([]byte(output), &attempts), qt.IsNil, qt.Commentf("%s", output))
	c.Assert(len(attempts) >= 2, qt.IsTrue, qt.Commentf("%s", output))
	return attempts
}

// writeDownPlanMigrations writes two migrations whose down bodies exist, so a
// rollback that ran them would succeed too: what distinguishes the derived path
// is that it never opens them.
func writeDownPlanMigrations(c *qt.C, dir string) string {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0755), qt.IsNil)
	files := map[string]string{
		"0000000000001_create_users.up.sql":   "CREATE TABLE users (id SERIAL PRIMARY KEY);\n",
		"0000000000001_create_users.down.sql": "DROP TABLE users;\n",
		"0000000000002_create_posts.up.sql":   "CREATE TABLE posts (id SERIAL PRIMARY KEY);\n",
		"0000000000002_create_posts.down.sql": "DROP TABLE posts;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0600), qt.IsNil)
	}
	return migrations
}
