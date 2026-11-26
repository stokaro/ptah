//go:build integration

package integration_test

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// Without trust in the replayed SQL, the SQL Server replay guard refuses every
// name of another database, a read as well as a write, every DBCC, and the
// functions that read a file on the server. These statements reach another
// database through a place no write target names: a text pointer, a DBCC
// string, a statistics target, a sequence, a view, and a plain read.

// sqlServerReferenceFixture is what the other database holds before each
// statement runs: a table with one row, a text column, an identity, and a
// sequence nothing has read.
var sqlServerReferenceFixture = []string{
	"CREATE TABLE dbo.kept (id int PRIMARY KEY)",
	"INSERT dbo.kept VALUES (1)",
	"CREATE TABLE dbo.tx (id int PRIMARY KEY, c text)",
	"INSERT dbo.tx VALUES (1, 'kept')",
	"CREATE TABLE dbo.ident (id int IDENTITY(1, 1), v int)",
	"INSERT dbo.ident (v) VALUES (1)",
	"CREATE SEQUENCE dbo.seq START WITH 1",
}

// sqlServerReferencedDatabase is what the tests read back from the other
// database: the rows of kept, the text of tx, the identity of ident, the
// statistics created on kept, and the last value read from seq.
type sqlServerReferencedDatabase struct {
	Rows, Text, Identity, Statistics, Sequence []string
}

// sqlServerReferenceUntouched is the other database as the fixture leaves it.
var sqlServerReferenceUntouched = sqlServerReferencedDatabase{
	Rows: []string{"1"}, Text: []string{"kept"}, Identity: []string{"1"}, Sequence: []string{"unused"},
}

// readSQLServerReferencedDatabase reads the database name from db, a session
// in another database of the same server.
func readSQLServerReferencedDatabase(c *qt.C, db *sql.DB, name string) sqlServerReferencedDatabase {
	c.Helper()
	database := "[" + name + "]"
	return sqlServerReferencedDatabase{
		Rows:     sqlServerStrings(c, db, "SELECT CAST(id AS nvarchar(20)) FROM "+database+".dbo.kept ORDER BY id"),
		Text:     sqlServerStrings(c, db, "SELECT CAST(c AS nvarchar(100)) FROM "+database+".dbo.tx"),
		Identity: sqlServerStrings(c, db, "SELECT CAST(IDENT_CURRENT(N'"+database+".dbo.ident') AS nvarchar(20))"),
		Statistics: sqlServerStrings(c, db, "SELECT name FROM "+database+".sys.stats WHERE user_created = 1 AND object_id = "+
			"OBJECT_ID(N'"+database+".dbo.kept')"),
		Sequence: sqlServerStrings(c, db, "SELECT ISNULL(CAST(last_used_value AS nvarchar(20)), N'unused') FROM "+
			database+".sys.sequences WHERE name = N'seq'"),
	}
}

// sqlServerReferences are the statements, each with the operation its
// refusal names. Each row changed the other database with the replayed SQL
// trusted on SQL Server 2025, as the review of stokaro/ptah#6737 measured.
var sqlServerReferences = []struct {
	name, statement, operation string
}{
	{
		name:      "WRITETEXT through a text pointer",
		statement: "DECLARE @p binary(16) SELECT @p = TEXTPTR(c) FROM {other}.dbo.tx WRITETEXT {other}.dbo.tx.c @p 'pwned';",
		operation: `cross-database reference "{other}.dbo.tx"`,
	},
	{name: "DBCC CHECKIDENT", statement: "DBCC CHECKIDENT ('{other}.dbo.ident', RESEED, 5000);", operation: "DBCC CHECKIDENT"},
	{
		name:      "CREATE STATISTICS",
		statement: "CREATE STATISTICS s ON {other}.dbo.kept (id);",
		operation: `cross-database reference "{other}.dbo.kept"`,
	},
	{
		name:      "NEXT VALUE FOR",
		statement: "INSERT INTO dbo.items VALUES (NEXT VALUE FOR {other}.dbo.seq);",
		operation: `cross-database reference "{other}.dbo.seq"`,
	},
	{
		name:      "a write through a view",
		statement: "CREATE VIEW dbo.v AS SELECT id FROM {other}.dbo.kept;\nINSERT INTO dbo.v VALUES (5);",
		operation: `cross-database reference "{other}.dbo.kept"`,
	},
	{
		name:      "a read",
		statement: "INSERT INTO dbo.items SELECT id + 100 FROM {other}.dbo.kept;",
		operation: `cross-database reference "{other}.dbo.kept"`,
	},
}

// TestSQLServerReplayRefusesAReferenceToAnotherDatabaseE2E replays each
// statement through `migrations validate` and `migrations generate
// --shadow-db`, neither told to trust the SQL. Each is refused before the
// migration runs, and the other database reads back as the fixture left it.
func TestSQLServerReplayRefusesAReferenceToAnotherDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	dev, devDB := declaredSQLServerDatabase(c)
	target, _ := declaredSQLServerDatabase(c)
	shadow, shadowDB := declaredSQLServerDatabase(c)
	otherURL, other := declaredSQLServerDatabase(c)
	name := sqlServerDatabaseName(c, otherURL)
	declaredSQLServerExec(c, other, sqlServerReferenceFixture...)
	for _, reference := range sqlServerReferences {
		t.Run(reference.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeSQLServerEscape(c, work, "", strings.ReplaceAll(reference.statement, "{other}", name))
			refusal := `(?s).*sqlserver migration replay rejects ` +
				regexp.QuoteMeta(strings.ReplaceAll(reference.operation, "{other}", name)) + ` because .*`

			for _, args := range sqlServerEscapeCommands(dev, target, shadow) {
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()}, args...)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Matches, refusal, qt.Commentf("%v", args))
			}
			c.Assert(readSQLServerReferencedDatabase(c, devDB, name), qt.DeepEquals, sqlServerReferenceUntouched)
			c.Assert(sqlServerStrings(c, devDB, "SELECT name FROM sys.tables"), qt.HasLen, 0)
			c.Assert(sqlServerStrings(c, shadowDB, "SELECT name FROM sys.tables"), qt.HasLen, 0)
		})
	}
}

// The refusal of a read says that only trust lets the run read another
// database, and names the variable that grants it.
func TestSQLServerReplayRefusesAReadOfAnotherDatabaseUntilTrustedE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	dev, _ := declaredSQLServerDatabase(c)
	otherURL, other := declaredSQLServerDatabase(c)
	name := sqlServerDatabaseName(c, otherURL)
	declaredSQLServerExec(c, other, sqlServerReferenceFixture...)
	writeSQLServerEscape(c, work, "", "INSERT INTO dbo.items SELECT id + 100 FROM "+name+".dbo.kept;")

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()},
		"migrations", "validate", "--dir", "migrations", "--dev-url", dev)

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Matches, `(?s).*sqlserver migration replay rejects cross-database reference "`+regexp.QuoteMeta(name)+
		`\.dbo\.kept" because a statement that reads or writes another database runs only when the replayed SQL is trusted; `+
		`.*PTAH_TRUST_REPLAY_SQL.*`)
}

// TestSQLServerReplayRunsATrustedReferenceToAnotherDatabaseE2E is the control
// for the refusals: with trust in the replayed SQL, the read runs, and the
// write through a view changes the other database, so the refusal is what kept
// it out.
func TestSQLServerReplayRunsATrustedReferenceToAnotherDatabaseE2E(t *testing.T) {
	tests := []struct {
		name, statement string
		want            sqlServerReferencedDatabase
	}{
		{
			name:      "a write through a view",
			statement: "CREATE VIEW dbo.v AS SELECT id FROM {other}.dbo.kept;\nINSERT INTO dbo.v VALUES (5);",
			want: sqlServerReferencedDatabase{
				Rows: []string{"1", "5"}, Text: []string{"kept"}, Identity: []string{"1"}, Sequence: []string{"unused"},
			},
		},
		{name: "a read", statement: "INSERT INTO dbo.items SELECT id + 100 FROM {other}.dbo.kept;", want: sqlServerReferenceUntouched},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dev, devDB := declaredSQLServerDatabase(c)
			otherURL, other := declaredSQLServerDatabase(c)
			name := sqlServerDatabaseName(c, otherURL)
			declaredSQLServerExec(c, other, sqlServerReferenceFixture...)
			writeSQLServerEscape(c, work, "", strings.ReplaceAll(test.statement, "{other}", name))

			runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev, "--trust-replay-sql")

			c.Assert(readSQLServerReferencedDatabase(c, devDB, name), qt.DeepEquals, test.want)
		})
	}
}
