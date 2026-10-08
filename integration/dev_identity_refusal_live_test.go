//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// devIdentityEngines are the servers the dev-database identity check is run
// against. Each is asked, over its own connection, which database the session
// selected, so the answer is the server's and differs by engine.
//
// dropSuffix completes the DROP DATABASE a test's cleanup runs: PostgreSQL
// refuses to drop a database with a session left on it, and the planning code
// opens sessions of its own.
var devIdentityEngines = []struct {
	name       string
	engine     dbtarget.Engine
	dropSuffix string
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL, dropSuffix: " WITH (FORCE)"},
	{name: "MySQL", engine: dbtarget.MySQLAdmin, dropSuffix: ""},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, dropSuffix: ""},
}

// devIdentityDDL is the desired state the tests plan toward: the target's own
// table and one it does not have yet.
const devIdentityDDL = "CREATE TABLE kept (id int NOT NULL, PRIMARY KEY (id));\nCREATE TABLE added (id int NOT NULL, PRIMARY KEY (id));\n"

// devIdentityTarget is a scratch database holding one row in `kept`, with the
// URL that names it and a connection to it.
type devIdentityTarget struct {
	url  string
	conn *dbschema.DatabaseConnection
}

func newDevIdentityTarget(c *qt.C, engine dbtarget.Engine, dropSuffix string) devIdentityTarget {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_dev_identity_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+dropSuffix)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	targetURL, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	c.Assert(atlasschema.ApplySQL(c.Context(), conn, migrator.MigrationTxModeNone,
		"CREATE TABLE kept (id int NOT NULL, PRIMARY KEY (id));\nINSERT INTO kept (id) VALUES (1);\n"), qt.IsNil)
	return devIdentityTarget{url: targetURL, conn: conn}
}

// keptRows counts the rows of `kept`, read back from the target.
func (t devIdentityTarget) keptRows(c *qt.C) int {
	c.Helper()
	var rows int
	c.Assert(t.conn.QueryRowContext(c.Context(), "SELECT count(*) FROM kept").Scan(&rows), qt.IsNil)
	return rows
}

// writeDevIdentitySources writes the desired state as a schema file and as a
// hashed migration directory, and returns both.
func writeDevIdentitySources(c *qt.C) (schemaFile, dir string) {
	c.Helper()
	root := c.TempDir()
	schemaFile = filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte(devIdentityDDL), 0o600), qt.IsNil)
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(devIdentityDDL), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return schemaFile, dir
}

// The tests below name the target as the dev database and give the planning
// code no target URL to compare it with. That is the alias a URL comparison
// cannot see -- a connection pooler serving the target under another database
// name -- so only the live comparison of the two sessions stands between the
// dev reset and the target. Without the live comparison ahead of the reset,
// through a PgBouncer alias and through a MySQL-family scheme pair, `schema
// apply` refuses with the sentence below after the target has lost `kept`
// (stokaro/ptah#3769).

// TestSimulateOnDevLeavesTheTargetsRowsWhenItRefusesTheDevDatabaseLive
// rehearses a plan with the target as the dev database.
func TestSimulateOnDevLeavesTheTargetsRowsWhenItRefusesTheDevDatabaseLive(t *testing.T) {
	for _, engine := range devIdentityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
			schemaFile, _ := writeDevIdentitySources(c)
			plan, err := atlasschema.PrepareApply(c.Context(), target.conn, atlasschema.ApplyRuntimeOptions{
				ToURLs:  []string{"file://" + schemaFile},
				TxMode:  migrator.MigrationTxModeNone,
				Runtime: must.Must(builtin.New())})
			c.Assert(err, qt.IsNil)

			err = plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{DevURL: target.url})

			c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
			c.Assert(target.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestRehearsePlanStatementsLeavesTheTargetsRowsWhenItRefusesTheDevDatabaseLive
// is the plan-file path, which arms the same cleanup of the dev database.
func TestRehearsePlanStatementsLeavesTheTargetsRowsWhenItRefusesTheDevDatabaseLive(t *testing.T) {
	for _, engine := range devIdentityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)

			err := atlasschema.RehearsePlanStatements(c.Context(), target.conn,
				[]string{"CREATE TABLE added (id int NOT NULL, PRIMARY KEY (id))"},
				&schemamodel.Database{},
				atlasschema.PlanRehearsalOptions{DevURL: target.url, Runtime: must.Must(builtin.New())})

			c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: the dev database is reset destructively before the plan is rehearsed on it`)
			c.Assert(target.keptRows(c), qt.Equals, 1)
		})
	}
}

// TestPrepareApplyLeavesTheTargetsRowsWhenItRefusesToReplayOnTheTarget plans
// toward a migration directory, which is replayed on the dev database while
// the plan is computed and reset first.
func TestPrepareApplyLeavesTheTargetsRowsWhenItRefusesToReplayOnTheTarget(t *testing.T) {
	for _, engine := range devIdentityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
			_, dir := writeDevIdentitySources(c)

			_, err := atlasschema.PrepareApply(c.Context(), target.conn, atlasschema.ApplyRuntimeOptions{
				ToURLs:  []string{"file://" + dir},
				DevURL:  target.url,
				TxMode:  migrator.MigrationTxModeNone,
				Runtime: must.Must(builtin.New())})

			c.Assert(err, qt.ErrorMatches, `load --to schema: --dev-url must not point at the target database: the dev database is reset destructively before the migration directory is replayed on it`)
			c.Assert(target.keptRows(c), qt.Equals, 1)
		})
	}
}
