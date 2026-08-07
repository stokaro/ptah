package atlasschema_test

// Live MySQL coverage for stokaro/ptah#1240: the dev-database simulation must
// run entirely inside the dev database.
//
// SQLite cannot express the defect. On MySQL a schema IS a database, so an
// apply plan rendered with the target's schema name — which is what the planner
// emits from an Atlas HCL desired state — lands in the target no matter which
// connection issues it. Measured 2026-08-07 on live MySQL 9.7 before the fix:
// one apply against a freshly created pair exited 1 reporting "the plan was not
// applied to the target database", and the target held the table the simulation
// had created while the dev database held nothing.
//
// Gated on MYSQL_TEST_DSN / MYSQL_TEST_URL like the migrator's MySQL tests.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestSimulateOnDev_MySQLFailedRehearsalLeavesTargetByteIdenticalLive(t *testing.T) {
	c := qt.New(t)
	baseURL := liveMySQLURLForSimulation(t)
	targetURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_fail_target")
	devURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_fail_dev")

	targetConn := openLiveMySQL(c, targetURL)
	c.Assert(atlasschema.ApplySQL(c.Context(), targetConn, migrator.MigrationTxModeNone,
		"CREATE TABLE `sim_keep` (`id` int NOT NULL, `label` varchar(32) NOT NULL, PRIMARY KEY (`id`));"), qt.IsNil)

	desiredPath := writeMySQLDesiredHCL(c, liveMySQLDatabaseName(c, targetURL), `
table "sim_keep" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  column "label" {
    type = varchar(32)
  }
  primary_key {
    columns = [column.id]
  }
}

table "sim_added" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`)

	plan, err := atlasschema.PrepareApply(c.Context(), targetConn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + desiredPath},
		TxMode: migrator.MigrationTxModeNone,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasChanges(), qt.IsTrue)
	// The premise of the defect: the plan carries the TARGET's schema name.
	// Without this the rest of the test would pass for the wrong reason.
	c.Assert(strings.Join(plan.Statements(), "\n"), qt.Contains,
		"`"+liveMySQLDatabaseName(c, targetURL)+"`.`sim_added`")

	before := mySQLSchemaSnapshot(c, targetConn, liveMySQLDatabaseName(c, targetURL))

	// The plan's own statements, plus one that collides with the baseline the
	// rehearsal recreates on the dev database. The first statement is the one
	// that used to land in the target; the last one guarantees the rehearsal
	// fails after it ran.
	statements := append(plan.Statements(),
		"CREATE TABLE `"+liveMySQLDatabaseName(c, targetURL)+"`.`sim_keep` (`id` int NOT NULL)")

	err = plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
		DevURL:     devURL,
		TargetURL:  targetURL,
		Statements: statements,
	})

	c.Assert(atlasschema.IsSimulationFailure(err), qt.IsTrue, qt.Commentf("error: %v", err))
	// The property worth pinning: a failed apply leaves the target exactly as
	// it was, down to every column and index information_schema reports.
	c.Assert(mySQLSchemaSnapshot(c, targetConn, liveMySQLDatabaseName(c, targetURL)), qt.DeepEquals, before)
	// And the dev database is handed back empty, as the pinned community
	// binary v1.3.0 does when its own dev-database work fails.
	devConn := openLiveMySQL(c, devURL)
	c.Assert(mySQLTableNames(c, devConn, liveMySQLDatabaseName(c, devURL)), qt.HasLen, 0)
}

func TestSimulateOnDev_MySQLSuccessfulRehearsalStillAppliesLive(t *testing.T) {
	c := qt.New(t)
	baseURL := liveMySQLURLForSimulation(t)
	targetURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_ok_target")
	devURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_ok_dev")

	targetConn := openLiveMySQL(c, targetURL)
	targetName := liveMySQLDatabaseName(c, targetURL)
	desiredPath := writeMySQLDesiredHCL(c, targetName, `
table "sim_added" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  column "email" {
    type = varchar(255)
  }
  primary_key {
    columns = [column.id]
  }
}
`)

	plan, err := atlasschema.PrepareApply(c.Context(), targetConn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + desiredPath},
		TxMode: migrator.MigrationTxModeNone,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(plan.Statements(), "\n"), qt.Contains, "`"+targetName+"`.`sim_added`")

	c.Assert(plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
		DevURL:    devURL,
		TargetURL: targetURL,
	}), qt.IsNil)

	// Isolation must not be bought by rehearsing nothing: the simulation left
	// the target untouched, and the apply that follows it still works.
	c.Assert(mySQLTableNames(c, targetConn, targetName), qt.HasLen, 0)
	devConn := openLiveMySQL(c, devURL)
	c.Assert(mySQLTableNames(c, devConn, liveMySQLDatabaseName(c, devURL)), qt.HasLen, 0)

	c.Assert(plan.Execute(c.Context()), qt.IsNil)
	c.Assert(mySQLTableNames(c, targetConn, targetName), qt.DeepEquals, []string{"sim_added"})
	c.Assert(mySQLSchemaSnapshot(c, targetConn, targetName), qt.Contains,
		"column|sim_added|email|2|varchar(255)|NO||<null>|")
}

func TestSimulateOnDev_MySQLRefusesAThirdDatabaseLive(t *testing.T) {
	c := qt.New(t)
	baseURL := liveMySQLURLForSimulation(t)
	targetURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_third_target")
	devURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_third_dev")
	bystanderURL := createLiveMySQLDatabase(c, baseURL, "ptah1240_third_bystander")

	bystanderConn := openLiveMySQL(c, bystanderURL)
	bystanderName := liveMySQLDatabaseName(c, bystanderURL)
	c.Assert(atlasschema.ApplySQL(c.Context(), bystanderConn, migrator.MigrationTxModeNone,
		"CREATE TABLE `bystander_keep` (`id` int NOT NULL, PRIMARY KEY (`id`));"), qt.IsNil)
	before := mySQLSchemaSnapshot(c, bystanderConn, bystanderName)

	targetConn := openLiveMySQL(c, targetURL)
	targetName := liveMySQLDatabaseName(c, targetURL)
	desiredPath := writeMySQLDesiredHCL(c, targetName, `
table "sim_added" {
  schema = schema.%[1]s
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`)
	plan, err := atlasschema.PrepareApply(c.Context(), targetConn, atlasschema.ApplyRuntimeOptions{
		ToURLs: []string{"file://" + desiredPath},
		TxMode: migrator.MigrationTxModeNone,
	})
	c.Assert(err, qt.IsNil)

	// A statement naming a database that is neither the target nor the dev one
	// cannot be re-scoped onto the dev database, so it is refused rather than
	// executed somewhere nobody asked for.
	err = plan.SimulateOnDev(c.Context(), atlasschema.SimulateOptions{
		DevURL:     devURL,
		TargetURL:  targetURL,
		Statements: []string{"DROP TABLE `" + bystanderName + "`.`bystander_keep`"},
	})

	c.Assert(atlasschema.IsDevScopeEscape(err), qt.IsTrue, qt.Commentf("error: %v", err))
	c.Assert(mySQLSchemaSnapshot(c, bystanderConn, bystanderName), qt.DeepEquals, before)
}

// liveMySQLURLForSimulation gates the live simulation tests on the same
// environment variables as the migrator's MySQL integration tests.
func liveMySQLURLForSimulation(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("MYSQL_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("MYSQL_TEST_URL")
	}
	if dbURL == "" {
		t.Skip("MYSQL_TEST_DSN or MYSQL_TEST_URL not set")
	}
	if !strings.HasPrefix(dbURL, "mysql://") {
		t.Skip("MySQL URL required for schema apply simulation live tests")
	}
	return dbURL
}

// createLiveMySQLDatabase creates a database of its own for one probe and drops
// it again when the test ends, so probes never share scratch space.
func createLiveMySQLDatabase(c *qt.C, baseURL, name string) string {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(context.Background(), baseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	_, err = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+name+"`")
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE `"+name+"`")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		cleanup, err := dbschema.ConnectToDatabase(context.Background(), baseURL)
		if err != nil {
			return
		}
		defer dbschema.CloseAndWarn(cleanup)
		_, _ = cleanup.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+name+"`")
	})
	return mySQLURLForDatabase(c, baseURL, name)
}

func mySQLURLForDatabase(c *qt.C, baseURL, name string) string {
	c.Helper()
	parsed, err := url.Parse(baseURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

func liveMySQLDatabaseName(c *qt.C, dbURL string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	return strings.TrimPrefix(parsed.Path, "/")
}

func openLiveMySQL(c *qt.C, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// writeMySQLDesiredHCL writes an Atlas HCL desired state whose tables name the
// database explicitly. That explicit `schema = schema.<database>` is what makes
// the planner qualify every statement with the target's name, which is the
// input the defect needs.
func writeMySQLDesiredHCL(c *qt.C, database, tables string) string {
	c.Helper()
	path := filepath.Join(c.TB.TempDir(), "desired.hcl")
	document := fmt.Sprintf("schema %q {\n}\n", database) + fmt.Sprintf(tables, database)
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// mySQLSchemaSnapshot renders every column and index information_schema reports
// for one database as sorted text, so "the target is unchanged" is asserted
// against the catalog rather than against an exit status.
func mySQLSchemaSnapshot(c *qt.C, conn *dbschema.DatabaseConnection, database string) []string {
	c.Helper()
	snapshot := mySQLQueryLines(c, conn, `
SELECT CONCAT_WS('|', 'column', TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE,
                 IS_NULLABLE, COLUMN_KEY, IFNULL(COLUMN_DEFAULT, '<null>'), EXTRA)
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ?
ORDER BY TABLE_NAME, ORDINAL_POSITION`, database)
	return append(snapshot, mySQLQueryLines(c, conn, `
SELECT CONCAT_WS('|', 'index', TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE)
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = ?
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`, database)...)
}

func mySQLTableNames(c *qt.C, conn *dbschema.DatabaseConnection, database string) []string {
	c.Helper()
	return mySQLQueryLines(c, conn, `
SELECT TABLE_NAME
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ?
ORDER BY TABLE_NAME`, database)
}

func mySQLQueryLines(c *qt.C, conn *dbschema.DatabaseConnection, query, database string) []string {
	c.Helper()
	rows, err := conn.QueryContext(context.Background(), query, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()

	var lines []string
	for rows.Next() {
		var line string
		c.Assert(rows.Scan(&line), qt.IsNil)
		lines = append(lines, line)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return lines
}
