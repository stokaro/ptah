//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/sqlserverexec"
)

// SQL Server has no DDL for a rename or for dropping a default whose name the
// server chose, so Ptah writes an EXEC for each: sp_rename, and a batch that
// sp_executesql runs. The dev replay guard refuses an EXEC, which can run
// anything, and accepts these in the shape Ptah writes them. A history that
// holds them replays on a shadow or dev database without trust in the SQL, and
// an EXEC that reaches past that database is still refused.

// sqlServerRenameChangelog creates two tables and an index with a default on
// one column, renames a column whose name holds a dot and a table whose new
// name holds a bracket, and renames the index. Liquibase's renameColumn and
// renameTable are rendered by Ptah's SQL Server renderer. The renderer writes
// no index rename on SQL Server, so changeset 3 carries the call in the shape
// the guard accepts.
const sqlServerRenameChangelog = `<databaseChangeLog>` +
	`<changeSet id="1" author="a"><sql>` +
	`CREATE TABLE dbo.accounts (id int NOT NULL PRIMARY KEY, [e.mail] nvarchar(100) NULL, status nvarchar(20) NULL DEFAULT 'active');` +
	`CREATE INDEX ix_status ON dbo.accounts (status);` +
	`CREATE TABLE dbo.[o'ld] (id int NOT NULL PRIMARY KEY);` +
	`</sql></changeSet>` +
	`<changeSet id="2" author="a">` +
	`<renameColumn schemaName="dbo" tableName="accounts" oldColumnName="e.mail" newColumnName="mail's"/>` +
	`<renameTable schemaName="dbo" oldTableName="o'ld" newTableName="ne]w"/>` +
	`</changeSet>` +
	`<changeSet id="3" author="a"><sql>EXEC sp_rename N'[dbo].[accounts].[ix_status]', N'ix_state', N'INDEX';</sql></changeSet>` +
	`</databaseChangeLog>`

// sqlServerRenameDesired is the schema after the history, without the default.
const sqlServerRenameDesired = `CREATE TABLE dbo.accounts (id int NOT NULL PRIMARY KEY, [mail's] nvarchar(100) NULL, status nvarchar(20) NULL);
CREATE INDEX ix_state ON dbo.accounts (status);
CREATE TABLE dbo.[ne]]w] (id int NOT NULL PRIMARY KEY);
`

// runSQLServerRename runs the native binary in work with no PTAH_ variable set,
// so nothing declares trust in the replayed SQL.
func runSQLServerRename(c *qt.C, work string, args ...string) clirun.Result {
	c.Helper()
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()}, args...)
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
	return got
}

// sqlServerStrings reads the one string column query returns, in its order.
func sqlServerStrings(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	read := make([]string, 0)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		read = append(read, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return read
}

// sqlServerDatabaseName is the database a SQL Server URL names.
func sqlServerDatabaseName(c *qt.C, rawURL string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	return parsed.Query().Get("database")
}

const (
	sqlServerRenameTables  = `SELECT name FROM sys.tables WHERE name IN (N'accounts', N'o''ld', N'ne]w') ORDER BY name`
	sqlServerRenameColumns = `SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.accounts') ORDER BY column_id`
	sqlServerRenameIndexes = `SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.accounts') AND is_primary_key = 0 AND name IS NOT NULL`
	sqlServerRenameDefault = `SELECT c.name FROM sys.default_constraints AS dc ` +
		`JOIN sys.columns AS c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id ` +
		`WHERE dc.parent_object_id = OBJECT_ID(N'dbo.accounts')`
)

// TestSQLServerReplaysTheRenamesAndTheDefaultDropPtahWritesE2E imports the
// renames, applies them, generates the migration that drops the default, and
// replays the history through `migrations generate --shadow-db`, `migrations
// validate` and `migrations drift`, none of them told to trust the SQL. The
// shadow check of `migrations generate` also runs the new migration's down,
// which replaces the default it dropped. The target reads back renamed and
// without the default, and the drift check finds it matches the history.
func TestSQLServerReplaysTheRenamesAndTheDefaultDropPtahWritesE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	target, db := declaredSQLServerDatabase(c)
	generateShadow, _ := declaredSQLServerDatabase(c)
	validateDev, _ := declaredSQLServerDatabase(c)
	driftShadow, _ := declaredSQLServerDatabase(c)
	writeLiquibaseSource(c, work, "changelog.xml", sqlServerRenameChangelog)
	c.Assert(os.WriteFile(filepath.Join(work, "desired.sql"), []byte(sqlServerRenameDesired), 0o600), qt.IsNil)

	runSQLServerRename(c, work, "migrations", "import", "--from", "liquibase", "--dialect", "sqlserver",
		"--source-dir", "legacy", "--migrations-dir", "migrations")
	runSQLServerRename(c, work, "migrations", "up", "--db-url", target, "--migrations-dir", "migrations")
	c.Assert(sqlServerStrings(c, db, sqlServerRenameDefault), qt.DeepEquals, []string{"status"})
	runSQLServerRename(c, work, "migrations", "generate", "--db-url", target, "--migrations-dir", "migrations",
		"--schema-file", "desired.sql", "--shadow-db", generateShadow, "--name", "drop_default")
	runSQLServerRename(c, work, "migrations", "hash", "--dir", "migrations")
	runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", validateDev)
	runSQLServerRename(c, work, "migrations", "up", "--db-url", target, "--migrations-dir", "migrations")
	runSQLServerRename(c, work, "migrations", "drift", "--db-url", target, "--migrations-dir", "migrations",
		"--shadow-db", driftShadow)

	history := readSQLServerMigrations(c, filepath.Join(work, "migrations"), "*.up.sql")
	c.Assert(history, qt.Contains, sqlserverexec.Rename{
		Name: []string{"dbo", "accounts", "e.mail"}, NewName: "mail's", Type: sqlserverexec.RenameColumn,
	}.Statement())
	c.Assert(history, qt.Contains, sqlserverexec.Rename{
		Name: []string{"dbo", "o'ld"}, NewName: "ne]w", Type: sqlserverexec.RenameObject,
	}.Statement())
	c.Assert(history, qt.Contains, sqlserverexec.DefaultConstraintBatch([]string{"dbo", "accounts"}, "status", ""))
	c.Assert(readSQLServerMigrations(c, filepath.Join(work, "migrations"), "*.down.sql"), qt.Contains,
		sqlserverexec.DefaultConstraintBatch([]string{"dbo", "accounts"}, "status",
			sqlserverexec.AddDefault([]string{"dbo", "accounts"}, "status", "'active'")))
	c.Assert(sqlServerStrings(c, db, sqlServerRenameTables), qt.DeepEquals, []string{"accounts", "ne]w"})
	c.Assert(sqlServerStrings(c, db, sqlServerRenameColumns), qt.DeepEquals, []string{"id", "mail's", "status"})
	c.Assert(sqlServerStrings(c, db, sqlServerRenameIndexes), qt.DeepEquals, []string{"ix_state"})
	c.Assert(sqlServerStrings(c, db, sqlServerRenameDefault), qt.HasLen, 0)
}

// readSQLServerMigrations is every migration file in dir that pattern
// matches, joined.
func readSQLServerMigrations(c *qt.C, dir, pattern string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.Not(qt.HasLen), 0)
	var history string
	for _, file := range files {
		body, err := os.ReadFile(file) // #nosec G304 -- the path is a glob over c.TempDir()
		c.Assert(err, qt.IsNil)
		history += string(body)
	}
	return history
}

// sqlServerEscapes are EXEC statements that reach past the dev database, each
// with the reason the guard gives. {other} stands for another database on the
// server, which holds the table kept. A rename of a database runs only outside
// a transaction, so its migration says so.
var sqlServerEscapes = []struct {
	name, header, statement, operation string
}{
	{
		name: "a database rename", header: "-- atlas:txmode none\n",
		statement: "EXEC sp_rename N'{other}', N'{other}_moved', N'DATABASE';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "a database rename in the rendered shape",
		header:    "-- atlas:txmode none\n",
		statement: "EXEC sp_rename N'[{other}]', N'{other}_moved', N'DATABASE';",
		operation: "sp_rename @objtype DATABASE",
	},
	{
		name:      "a three-part name in another database",
		statement: "EXEC sp_rename N'[{other}].[dbo].[kept]', N'moved', N'OBJECT';",
		operation: `cross-database target "{other}.dbo.kept"`,
	},
	{
		name:      "sp_rename of another database",
		statement: "EXEC {other}..sp_rename N'[dbo].[kept]', N'moved', N'OBJECT';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "sp_executesql with other text",
		statement: "EXEC sp_executesql N'CREATE TABLE {other}.dbo.escaped (id int)';",
		operation: "EXEC/EXECUTE sublanguage",
	},
}

// writeSQLServerEscape writes a history whose one migration creates a table
// and runs statement, hashed for `migrations validate`.
func writeSQLServerEscape(c *qt.C, work, header, statement string) {
	c.Helper()
	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	up := header + "CREATE TABLE dbo.items (id int PRIMARY KEY);\n" + statement + "\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.down.sql"), []byte("DROP TABLE dbo.items;\n"), 0o600), qt.IsNil)
	runSQLServerRename(c, work, "migrations", "hash", "--dir", "migrations")
}

// TestSQLServerReplayRefusesAnEXECThatReachesPastTheDevDatabaseE2E replays a
// hand-written EXEC through `migrations validate` without trust. Each is
// refused before the migration runs, and the other database reads back with
// its name and its one table.
func TestSQLServerReplayRefusesAnEXECThatReachesPastTheDevDatabaseE2E(t *testing.T) {
	for _, escape := range sqlServerEscapes {
		t.Run(escape.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dev, devDB := declaredSQLServerDatabase(c)
			otherURL, other := declaredSQLServerDatabase(c)
			name := sqlServerDatabaseName(c, otherURL)
			declaredSQLServerExec(c, other, "CREATE TABLE dbo.kept (id int PRIMARY KEY)")
			// No session of the test stays in the other database, so a rename
			// of it that got past the guard would take effect.
			other.SetMaxIdleConns(0)
			writeSQLServerEscape(c, work, escape.header, strings.ReplaceAll(escape.statement, "{other}", name))

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()},
				"migrations", "validate", "--dir", "migrations", "--dev-url", dev)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Matches, `(?s).*sqlserver migration replay rejects `+
				regexp.QuoteMeta(strings.ReplaceAll(escape.operation, "{other}", name))+
				` because its effects cannot be confined to the disposable database realm.*`)
			c.Assert(sqlServerStrings(c, devDB, "SELECT name FROM sys.databases WHERE name LIKE N'"+name+"%' ORDER BY name"),
				qt.DeepEquals, []string{name})
			c.Assert(sqlServerStrings(c, devDB, "SELECT name FROM ["+name+"].sys.tables ORDER BY name"), qt.DeepEquals, []string{"kept"})
		})
	}
}

// TestSQLServerReplayRunsATrustedEXECThatReachesPastTheDevDatabaseE2E is the
// control for the refusal: with trust in the replayed SQL, the batch writes
// into the other database, so the refusal is what kept it out.
func TestSQLServerReplayRunsATrustedEXECThatReachesPastTheDevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	dev, _ := declaredSQLServerDatabase(c)
	otherURL, other := declaredSQLServerDatabase(c)
	name := sqlServerDatabaseName(c, otherURL)
	declaredSQLServerExec(c, other, "CREATE TABLE dbo.kept (id int PRIMARY KEY)")
	writeSQLServerEscape(c, work, "", fmt.Sprintf("EXEC sp_executesql N'CREATE TABLE %s.dbo.escaped (id int)';", name))

	runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev, "--trust-replay-sql")

	c.Assert(sqlServerStrings(c, other, "SELECT name FROM sys.tables ORDER BY name"), qt.DeepEquals, []string{"escaped", "kept"})
}
