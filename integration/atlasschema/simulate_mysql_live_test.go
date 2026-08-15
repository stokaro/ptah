//go:build integration

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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestSimulateOnDev_MySQLFailedRehearsalLeavesTargetByteIdenticalLive(t *testing.T) {
	c := qt.New(t)
	restrictedURL, adminURL := liveMySQLURLsForSimulation(t)
	targetURL := createRestrictedLiveMySQLDatabase(c, adminURL, restrictedURL, "ptah1240_fail_target")
	devURL := createLiveMySQLDatabase(c, adminURL, adminURL, "ptah1240_fail_dev")

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
	restrictedURL, adminURL := liveMySQLURLsForSimulation(t)
	targetURL := createRestrictedLiveMySQLDatabase(c, adminURL, restrictedURL, "ptah1240_ok_target")
	devURL := createLiveMySQLDatabase(c, adminURL, adminURL, "ptah1240_ok_dev")

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
	restrictedURL, adminURL := liveMySQLURLsForSimulation(t)
	targetURL := createRestrictedLiveMySQLDatabase(c, adminURL, restrictedURL, "ptah1240_third_target")
	devURL := createLiveMySQLDatabase(c, adminURL, adminURL, "ptah1240_third_dev")
	bystanderURL := createRestrictedLiveMySQLDatabase(c, adminURL, restrictedURL, "ptah1240_third_bystander")

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

// liveMySQLURLsForSimulation keeps the target connection restricted while
// requiring a separate administrative connection for scratch provisioning.
func liveMySQLURLsForSimulation(t *testing.T) (restrictedURL, adminURL string) {
	t.Helper()
	restrictedURL = dbtarget.URL(t, dbtarget.MySQL)
	adminURL = dbtarget.URL(t, dbtarget.MySQLAdmin)
	return normalizeMySQLTestURL(restrictedURL), normalizeMySQLTestURL(adminURL)
}

func normalizeMySQLTestURL(rawURL string) string {
	if strings.HasPrefix(rawURL, "mysql://") {
		return rawURL
	}
	return "mysql://" + rawURL
}

// createLiveMySQLDatabase provisions through the administrative connection,
// but returns a URL with the caller-selected credentials. This distinction is
// what lets the tests exercise a restricted target and an administrative dev
// database without granting CREATE DATABASE to the target user.
func createLiveMySQLDatabase(c *qt.C, adminURL, connectionURL, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })

	quotedName := sqlident.Quote(platform.MySQL, name)
	_, err = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quotedName)
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(context.Background(), "CREATE DATABASE "+quotedName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, cleanupErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+quotedName)
		c.Check(cleanupErr, qt.IsNil)
	})
	return mySQLURLForDatabase(c, connectionURL, name)
}

func createRestrictedLiveMySQLDatabase(c *qt.C, adminURL, restrictedURL, prefix string) string {
	c.Helper()
	dbURL := createLiveMySQLDatabase(c, adminURL, restrictedURL, prefix)
	username, host := currentMySQLAccount(c, restrictedURL)
	admin, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	_, err = admin.ExecContext(context.Background(), fmt.Sprintf(
		"GRANT ALL PRIVILEGES ON %s.* TO %s@%s",
		sqlident.Quote(platform.MySQL, liveMySQLDatabaseName(c, dbURL)),
		quoteMySQLString(username),
		quoteMySQLString(host),
	))
	c.Assert(err, qt.IsNil)
	return dbURL
}

func currentMySQLAccount(c *qt.C, dbURL string) (username, host string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var account string
	err = conn.QueryRowContext(context.Background(), "SELECT CURRENT_USER()").Scan(&account)
	c.Assert(err, qt.IsNil)
	username, host, found := strings.Cut(account, "@")
	c.Assert(found, qt.IsTrue)
	c.Assert(username, qt.Not(qt.Equals), "")
	c.Assert(host, qt.Not(qt.Equals), "")
	return username, host
}

func mySQLURLForDatabase(c *qt.C, baseURL, name string) string {
	c.Helper()
	scheme, dsn, found := strings.Cut(baseURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = name
	return scheme + "://" + config.FormatDSN()
}

func liveMySQLDatabaseName(c *qt.C, dbURL string) string {
	c.Helper()
	_, dsn, found := strings.Cut(dbURL, "://")
	c.Assert(found, qt.IsTrue)
	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	c.Assert(config.DBName, qt.Not(qt.Equals), "")
	return config.DBName
}

func quoteMySQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
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
