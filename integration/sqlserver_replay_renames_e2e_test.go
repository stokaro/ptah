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

// sqlServerStrings reads the one string column query returns, in its order,
// and nil when it returns no row.
func sqlServerStrings(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var read []string
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

// sqlServerEscapes are statements that reach past the dev database, each
// with the reason the guard gives. {other} stands for another database on the
// server, which holds the table kept with one row. A migration that drops or
// renames a database runs only outside a transaction, so it says so.
//
// Each row ran on SQL Server 2022 and 2025 with trust, or without the guard,
// and reached the other database, read a file on the server or changed the
// session; TestSQLServerReplayRunsATrustedWriteIntoAnotherDatabaseE2E runs the
// writes. SHUTDOWN is pinned in the guard's unit tests and never run here.
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
		operation: `cross-database reference "{other}..sp_rename"`,
	},
	{
		name:      "sp_executesql with other text",
		statement: "EXEC sp_executesql N'CREATE TABLE {other}.dbo.escaped (id int)';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "sp_executesql named with no EXEC",
		statement: "sp_executesql N'CREATE TABLE {other}.dbo.escaped (id int)';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		// The server answers with xp_cmdshell disabled, as it is by default,
		// so the row asserts the refusal and does not enable it.
		name:      "xp_cmdshell named with no EXEC",
		statement: "xp_cmdshell N'whoami';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "DROP DATABASE after an ALTER TABLE with no separator",
		header:    "-- atlas:txmode none\n",
		statement: "ALTER TABLE dbo.items ADD DEFAULT 0 FOR id DROP DATABASE [{other}];",
		operation: "DROP DATABASE",
	},
	{
		name:      "sp_executesql after a CREATE TABLE with no separator",
		statement: "CREATE TABLE dbo.more (id int) EXEC sp_executesql N'CREATE TABLE {other}.dbo.escaped (id int)';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "DROP DATABASE after OUTPUT deleted.*",
		header:    "-- atlas:txmode none\n",
		statement: "DELETE FROM dbo.items OUTPUT deleted.* DROP DATABASE [{other}];",
		operation: "DROP DATABASE",
	},
	{
		name:      "DROP DATABASE after OUTPUT inserted.*",
		header:    "-- atlas:txmode none\n",
		statement: "UPDATE dbo.items SET id = 2 OUTPUT inserted.* DROP DATABASE [{other}];",
		operation: "DROP DATABASE",
	},
	{
		name:      "DROP DATABASE after a bare @",
		header:    "-- atlas:txmode none\n",
		statement: "DECLARE @ int = 1 SELECT @ DROP DATABASE [{other}];",
		operation: "DROP DATABASE",
	},
	{
		name:      "sp_executesql after a bare @",
		statement: "DECLARE @ int = 1 SELECT @ EXEC sp_executesql N'INSERT {other}.dbo.kept VALUES (2)';",
		operation: "EXEC/EXECUTE sublanguage",
	},
	{
		name:      "USE after a bare @ assigned to itself",
		statement: "DECLARE @ int = 1 SET @ = @ USE master;",
		operation: "USE",
	},
	{
		name:      "a constant EXEC creating a table with the schema left out",
		statement: "EXEC(N'CREATE TABLE {other}..escaped (id int)');",
		operation: `cross-database reference "{other}..escaped"`,
	},
	{
		// The migration file splitter drops the semicolon a MERGE ends with,
		// so with trust the server refuses this one (Msg 10713) and writes
		// nothing. The constant EXEC of the same MERGE among sqlServerWrites
		// writes through the same target.
		name: "the target of a MERGE",
		statement: "MERGE {other}.dbo.kept AS t USING (SELECT 2 AS id) AS s ON t.id = s.id " +
			"WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);",
		operation: `cross-database reference "{other}.dbo.kept"`,
	},
	{
		name:      "OPENROWSET reading a file on the server",
		statement: "INSERT INTO dbo.items SELECT LEN(BulkColumn) FROM OPENROWSET(BULK '/etc/hostname', SINGLE_CLOB) AS x;",
		operation: "OPENROWSET",
	},
}

// sqlServerWrites are writes into another database in every form T-SQL gives
// the target, each with what the other database holds after it runs: its
// tables, the columns of kept and the rows of kept, which start as kept, id
// and 1. Without trust the guard refuses each one, naming target, the first
// name of another database the statement holds.
var sqlServerWrites = []struct {
	name, statement, target string
	tables, columns, rows   []string
}{
	{
		name: "INSERT into a name with the schema left out", statement: "INSERT INTO {other}..kept VALUES (2);",
		target: "{other}..kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"1", "2"},
	},
	{
		name: "CREATE TABLE with the schema left out", statement: "CREATE TABLE {other}..escaped (id int);",
		target: "{other}..escaped", tables: []string{"escaped", "kept"}, columns: []string{"id"}, rows: []string{"1"},
	},
	{
		name: "DROP TABLE with the schema left out", statement: "DROP TABLE {other}..kept;",
		target: "{other}..kept", tables: nil, columns: nil, rows: nil,
	},
	{
		name: "UPDATE with the schema left out", statement: "UPDATE {other}..kept SET id = 9;",
		target: "{other}..kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"9"},
	},
	{
		name: "DELETE with the schema left out", statement: "DELETE FROM {other}..kept;",
		target: "{other}..kept", tables: []string{"kept"}, columns: []string{"id"}, rows: nil,
	},
	{
		name: "TRUNCATE with the schema left out", statement: "TRUNCATE TABLE {other}..kept;",
		target: "{other}..kept", tables: []string{"kept"}, columns: []string{"id"}, rows: nil,
	},
	{
		name: "ALTER TABLE with the schema left out", statement: "ALTER TABLE {other}..kept ADD c int;",
		target: "{other}..kept", tables: []string{"kept"}, columns: []string{"id", "c"}, rows: []string{"1"},
	},
	{
		// The guard does not know which words are reserved, so it reads INTO
		// as a part of the name, which it refuses.
		name: "a name that leaves the server out", statement: "INSERT INTO .{other}.dbo.kept VALUES (2);",
		target: "INTO.{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"1", "2"},
	},
	{
		name: "DELETE with no FROM", statement: "DELETE {other}.dbo.kept;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: nil,
	},
	{
		name: "INSERT with no INTO", statement: "INSERT {other}.dbo.kept VALUES (2);",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"1", "2"},
	},
	{
		name: "UPDATE TOP", statement: "UPDATE TOP (1) {other}.dbo.kept SET id = 9;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"9"},
	},
	{
		name: "UPDATE through an alias", statement: "UPDATE x SET id = 9 FROM {other}.dbo.kept AS x;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"9"},
	},
	{
		name: "UPDATE through the table's own name", statement: "UPDATE kept SET id = 9 FROM {other}.dbo.kept;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"9"},
	},
	{
		name: "DELETE through an alias", statement: "DELETE FROM x FROM {other}.dbo.kept AS x;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: nil,
	},
	{
		name: "OUTPUT INTO", statement: "INSERT INTO dbo.items OUTPUT inserted.id + 10 INTO {other}.dbo.kept VALUES (3);",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"1", "13"},
	},
	{
		name: "a constant EXEC of a MERGE",
		statement: "EXEC(N'MERGE {other}.dbo.kept AS t USING (SELECT 2 AS id) AS s ON t.id = s.id " +
			"WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);');",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"1", "2"},
	},
	{
		name: "UPDATE through a common table expression", statement: "WITH c AS (SELECT id FROM {other}.dbo.kept) UPDATE c SET id = 9;",
		target: "{other}.dbo.kept", tables: []string{"kept"}, columns: []string{"id"}, rows: []string{"9"},
	},
}

// sqlServerEscapeDesired is the schema `migrations generate` plans toward.
const sqlServerEscapeDesired = "CREATE TABLE dbo.items (id int PRIMARY KEY);\nCREATE TABLE dbo.more (id int);\n"

// writeSQLServerEscape writes a history whose one migration creates a table
// and runs statement, hashed for `migrations validate`, and the schema
// `migrations generate` plans toward.
func writeSQLServerEscape(c *qt.C, work, header, statement string) {
	c.Helper()
	dir := filepath.Join(work, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	up := header + "CREATE TABLE dbo.items (id int PRIMARY KEY);\n" + statement + "\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.down.sql"), []byte("DROP TABLE dbo.items;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(work, "desired.sql"), []byte(sqlServerEscapeDesired), 0o600), qt.IsNil)
	runSQLServerRename(c, work, "migrations", "hash", "--dir", "migrations")
}

// sqlServerOtherDatabase is what the tests read back from the other
// database: its name as the server lists it, its tables, and the columns and
// the rows of kept.
type sqlServerOtherDatabase struct {
	Names, Tables, Columns, Rows []string
}

// readSQLServerOtherDatabase reads the database name from db, a session in
// another database of the same server, so that no session of the test stays
// in name.
func readSQLServerOtherDatabase(c *qt.C, db *sql.DB, name string) sqlServerOtherDatabase {
	c.Helper()
	kept := "N'[" + name + "].dbo.kept'"
	return sqlServerOtherDatabase{
		Names:  sqlServerStrings(c, db, "SELECT name FROM sys.databases WHERE name LIKE N'"+name+"%' ORDER BY name"),
		Tables: sqlServerStrings(c, db, "SELECT name FROM ["+name+"].sys.tables ORDER BY name"),
		Columns: sqlServerStrings(c, db, "SELECT name FROM ["+name+"].sys.columns WHERE object_id = OBJECT_ID("+kept+
			") ORDER BY column_id"),
		Rows: sqlServerStrings(c, db, "IF OBJECT_ID("+kept+") IS NOT NULL EXEC(N'SELECT CAST(id AS nvarchar(20)) FROM ["+
			name+"].dbo.kept ORDER BY id')"),
	}
}

// sqlServerEscapeCommands are the commands that replay a history without
// trust: `migrations validate` on a dev database, and `migrations generate`,
// which replays it on a shadow database before it plans.
func sqlServerEscapeCommands(dev, target, shadow string) [][]string {
	return [][]string{
		{"migrations", "validate", "--dir", "migrations", "--dev-url", dev},
		{"migrations", "generate", "--db-url", target, "--migrations-dir", "migrations", "--schema-file", "desired.sql",
			"--shadow-db", shadow, "--name", "more"},
	}
}

// TestSQLServerReplayRefusesAStatementThatReachesPastTheDevDatabaseE2E
// replays each escape and each write through `migrations validate` and
// `migrations generate --shadow-db`, neither told to trust the SQL. Each is
// refused before the migration runs: the dev and the shadow databases hold no
// table after it, and the other database reads back with its name, its one
// table, and the column and the row of kept.
func TestSQLServerReplayRefusesAStatementThatReachesPastTheDevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	dev, devDB := declaredSQLServerDatabase(c)
	target, _ := declaredSQLServerDatabase(c)
	shadow, shadowDB := declaredSQLServerDatabase(c)
	otherURL, other := declaredSQLServerDatabase(c)
	name := sqlServerDatabaseName(c, otherURL)
	declaredSQLServerExec(c, other, "CREATE TABLE dbo.kept (id int PRIMARY KEY)", "INSERT dbo.kept VALUES (1)")
	// No session of the test stays in the other database, so a rename or a
	// drop of it that got past the guard would take effect.
	other.SetMaxIdleConns(0)
	refused := make([]struct{ name, header, statement, operation string }, 0, len(sqlServerEscapes)+len(sqlServerWrites))
	refused = append(refused, sqlServerEscapes...)
	for _, write := range sqlServerWrites {
		refused = append(refused, struct{ name, header, statement, operation string }{
			name: write.name, statement: write.statement, operation: fmt.Sprintf("cross-database reference %q", write.target),
		})
	}
	for _, escape := range refused {
		t.Run(escape.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeSQLServerEscape(c, work, escape.header, strings.ReplaceAll(escape.statement, "{other}", name))
			refusal := `(?s).*sqlserver migration replay rejects ` + regexp.QuoteMeta(strings.ReplaceAll(escape.operation, "{other}", name)) +
				` because .*`

			for _, args := range sqlServerEscapeCommands(dev, target, shadow) {
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()}, args...)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Matches, refusal, qt.Commentf("%v", args))
			}
			c.Assert(readSQLServerOtherDatabase(c, devDB, name), qt.DeepEquals, sqlServerOtherDatabase{
				Names: []string{name}, Tables: []string{"kept"}, Columns: []string{"id"}, Rows: []string{"1"},
			})
			c.Assert(sqlServerStrings(c, devDB, "SELECT name FROM sys.tables"), qt.HasLen, 0)
			c.Assert(sqlServerStrings(c, shadowDB, "SELECT name FROM sys.tables"), qt.HasLen, 0)
		})
	}
}

// TestSQLServerReplayRunsATrustedEXECThatReachesPastTheDevDatabaseE2E is the
// control for the EXEC refusals: with trust in the replayed SQL, each batch
// writes into the other database, so the refusal is what kept it out. The
// server runs the EXEC that follows a statement with no separator, and the
// procedure a statement names with no EXEC.
func TestSQLServerReplayRunsATrustedEXECThatReachesPastTheDevDatabaseE2E(t *testing.T) {
	tests := []struct {
		name, statement string
	}{
		{name: "sp_executesql", statement: "EXEC sp_executesql N'CREATE TABLE %s.dbo.escaped (id int)';"},
		{name: "sp_executesql named with no EXEC", statement: "sp_executesql N'CREATE TABLE %s.dbo.escaped (id int)';"},
		{
			name:      "sp_executesql after a CREATE TABLE with no separator",
			statement: "CREATE TABLE dbo.more (id int) EXEC sp_executesql N'CREATE TABLE %s.dbo.escaped (id int)';",
		},
		{name: "a constant EXEC creating a table with the schema left out", statement: "EXEC(N'CREATE TABLE %s..escaped (id int)');"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dev, _ := declaredSQLServerDatabase(c)
			otherURL, other := declaredSQLServerDatabase(c)
			name := sqlServerDatabaseName(c, otherURL)
			declaredSQLServerExec(c, other, "CREATE TABLE dbo.kept (id int PRIMARY KEY)")
			writeSQLServerEscape(c, work, "", fmt.Sprintf(test.statement, name))

			runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev, "--trust-replay-sql")

			c.Assert(sqlServerStrings(c, other, "SELECT name FROM sys.tables ORDER BY name"), qt.DeepEquals, []string{"escaped", "kept"})
		})
	}
}

// TestSQLServerReplayRunsATrustedWriteIntoAnotherDatabaseE2E is the control
// for the write refusals: with trust in the replayed SQL, each write changes
// the other database as its row says, so the refusal is what kept it out.
func TestSQLServerReplayRunsATrustedWriteIntoAnotherDatabaseE2E(t *testing.T) {
	for _, write := range sqlServerWrites {
		t.Run(write.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dev, devDB := declaredSQLServerDatabase(c)
			otherURL, other := declaredSQLServerDatabase(c)
			name := sqlServerDatabaseName(c, otherURL)
			declaredSQLServerExec(c, other, "CREATE TABLE dbo.kept (id int PRIMARY KEY)", "INSERT dbo.kept VALUES (1)")
			writeSQLServerEscape(c, work, "", strings.ReplaceAll(write.statement, "{other}", name))

			runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev, "--trust-replay-sql")

			c.Assert(readSQLServerOtherDatabase(c, devDB, name), qt.DeepEquals, sqlServerOtherDatabase{
				Names: []string{name}, Tables: write.tables, Columns: write.columns, Rows: write.rows,
			})
		})
	}
}

// sqlServerTemporalHistory creates a temporal table, adds the period columns
// to a second table and turns system versioning on there, and then takes both
// back to plain tables, so that the schema it leaves is dbo.items and dbo.ta.
// The END of ROW END closes no block, so the guard reads each statement and
// accepts it.
const sqlServerTemporalHistory = "CREATE TABLE dbo.tt (id int PRIMARY KEY, " +
	"vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, " +
	"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt)) " +
	"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tt_history));\n" +
	"CREATE TABLE dbo.ta (id int PRIMARY KEY);\n" +
	"ALTER TABLE dbo.ta ADD " +
	"vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL CONSTRAINT df_ta_vf DEFAULT SYSUTCDATETIME(), " +
	"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL " +
	"CONSTRAINT df_ta_vt DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'), PERIOD FOR SYSTEM_TIME (vf, vt);\n" +
	"ALTER TABLE dbo.ta SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.ta_history));\n" +
	"ALTER TABLE dbo.tt SET (SYSTEM_VERSIONING = OFF);\n" +
	"DROP TABLE dbo.tt;\n" +
	"DROP TABLE dbo.tt_history;\n" +
	"ALTER TABLE dbo.ta SET (SYSTEM_VERSIONING = OFF);\n" +
	"ALTER TABLE dbo.ta DROP PERIOD FOR SYSTEM_TIME;\n" +
	"ALTER TABLE dbo.ta DROP CONSTRAINT df_ta_vf, df_ta_vt;\n" +
	"ALTER TABLE dbo.ta DROP COLUMN vf, vt;\n" +
	"DROP TABLE dbo.ta_history;\n"

// sqlServerTemporalDesired is the schema the temporal history leaves, and
// one table more.
const sqlServerTemporalDesired = "CREATE TABLE dbo.items (id int PRIMARY KEY);\n" +
	"CREATE TABLE dbo.ta (id int PRIMARY KEY);\n" +
	"CREATE TABLE dbo.more (id int);\n"

// TestSQLServerReplaysATemporalTableE2E replays the temporal history through
// `migrations validate` and `migrations generate --shadow-db`, neither told
// to trust the SQL, and both succeed. The target is at the history's last
// version, so the migration `migrations generate` writes creates dbo.more,
// and the shadow check replays the history before it.
func TestSQLServerReplaysATemporalTableE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	dev, _ := declaredSQLServerDatabase(c)
	target, targetDB := declaredSQLServerDatabase(c)
	shadow, _ := declaredSQLServerDatabase(c)
	writeSQLServerEscape(c, work, "", sqlServerTemporalHistory)
	c.Assert(os.WriteFile(filepath.Join(work, "desired.sql"), []byte(sqlServerTemporalDesired), 0o600), qt.IsNil)
	// The history drops the history tables the server created, which the
	// safety check reads as dropping data.
	runSQLServerRename(c, work, "migrations", "up", "--db-url", target, "--migrations-dir", "migrations", "--allow-destructive")

	for _, args := range sqlServerEscapeCommands(dev, target, shadow) {
		runSQLServerRename(c, work, args...)
	}

	c.Assert(readSQLServerMigrations(c, filepath.Join(work, "migrations"), "*_more.up.sql"), qt.Contains, "[dbo].[more]")
	c.Assert(sqlServerStrings(c, targetDB, "SELECT name FROM sys.tables WHERE name IN (N'items', N'ta', N'ta_history', N'tt', "+
		"N'tt_history', N'more') ORDER BY name"), qt.DeepEquals, []string{"items", "ta"})
}
