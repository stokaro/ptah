//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// Without trust in the replayed SQL, the SQL Server replay guard refuses every
// name of another database the dev session reaches, a read as well as a
// write, a DBCC on a table of another database, and the functions that read a
// file on the server. These statements reach another database through a place
// no write target names: a text pointer, a DBCC string, a statistics target, a
// sequence, a view, and a plain read.

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
	{
		name:      "DBCC CHECKIDENT",
		statement: "DBCC CHECKIDENT ('{other}.dbo.ident', RESEED, 5000);",
		operation: `DBCC CHECKIDENT of "{other}.dbo.ident"`,
	},
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
	{
		// The session reaches master, so master.sys.databases is a read of
		// another database and not a column of a table aliased master.
		name:      "a read of master",
		statement: "INSERT INTO dbo.items SELECT TOP (1) database_id FROM master.sys.databases;",
		operation: `cross-database reference "master.sys.databases"`,
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

// sqlServerPlacesDesired is a view that calls a method on a geography, a
// hierarchyid and an xml column through the table's alias. Each call reads
// like a name of another database, p, but the session reaches no database of
// that name, so the server reads it as a column.
const sqlServerPlacesDesired = `CREATE TABLE dbo.places (id int NOT NULL PRIMARY KEY, geo geography NULL, node hierarchyid NULL, doc xml NULL);
CREATE VIEW dbo.v_places AS SELECT p.id, p.geo.STAsText() AS wkt, p.node.GetLevel() AS lvl, p.doc.value('(/a)[1]', 'int') AS a FROM dbo.places AS p;
`

// TestSQLServerGeneratesAViewThatCallsAMethodOnAColumnE2E generates the view
// through `migrations generate --shadow-db`, not told to trust the SQL, which
// replays the new migration on the shadow database, applies it, and reads a
// row back through the view.
func TestSQLServerGeneratesAViewThatCallsAMethodOnAColumnE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	target, db := declaredSQLServerDatabase(c)
	shadow, _ := declaredSQLServerDatabase(c)
	c.Assert(os.WriteFile(filepath.Join(work, "desired.sql"), []byte(sqlServerPlacesDesired), 0o600), qt.IsNil)

	runSQLServerRename(c, work, "migrations", "generate", "--db-url", target, "--migrations-dir", "migrations",
		"--schema-file", "desired.sql", "--shadow-db", shadow, "--name", "places")
	runSQLServerRename(c, work, "migrations", "up", "--db-url", target, "--migrations-dir", "migrations")
	declaredSQLServerExec(c, db,
		"INSERT dbo.places VALUES (1, geography::Point(1, 2, 4326), hierarchyid::Parse('/1/2/'), '<a>7</a>')")

	c.Assert(sqlServerStrings(c, db, "SELECT CONCAT(id, N' ', wkt, N' ', lvl, N' ', a) FROM dbo.v_places"), qt.DeepEquals,
		[]string{"1 POINT (2 1) 2 7"})
}

// sqlServerDatabaseNamedWithAnAt creates a database named like the scratch
// database rawURL names followed by @, which holds kept with one row, and
// returns its name. db is a session of the same server in another database.
// T-SQL lets @ follow the first character of a name, so the server reads the
// unquoted name as one word.
func sqlServerDatabaseNamedWithAnAt(c *qt.C, db *sql.DB, rawURL string) string {
	c.Helper()
	name := sqlServerDatabaseName(c, rawURL) + "@"
	declaredSQLServerExec(c, db, "CREATE DATABASE ["+name+"]")
	c.Cleanup(func() {
		_, err := db.ExecContext(context.Background(),
			"ALTER DATABASE ["+name+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+name+"]")
		c.Check(err, qt.IsNil)
	})
	declaredSQLServerExec(c, db, "CREATE TABLE ["+name+"].dbo.kept (id int PRIMARY KEY)", "INSERT ["+name+"].dbo.kept VALUES (1)")
	return name
}

// sqlServerWritesThroughANameWithAnAt write into the database {at}, whose
// name is the dev database's followed by @, each with the name its refusal
// gives. Read as the lexer reads them, each names the local table .dbo.kept
// after a variable.
var sqlServerWritesThroughANameWithAnAt = []struct {
	name, statement, target string
}{
	{name: "a database name ending in @", statement: "INSERT INTO {at}.dbo.kept VALUES (2);", target: "{at}.dbo.kept"},
	{name: "the schema left out", statement: "INSERT INTO {at}..kept VALUES (2);", target: "{at}..kept"},
	{name: "a space before the dot", statement: "INSERT INTO {at} .dbo.kept VALUES (2);", target: "{at}.dbo.kept"},
}

// TestSQLServerReplayRefusesAWriteThroughADatabaseNamedWithAnAtE2E replays
// each write through `migrations validate` and `migrations generate
// --shadow-db`, neither told to trust the SQL. Each is refused, and the
// database reads back with the one row of kept.
func TestSQLServerReplayRefusesAWriteThroughADatabaseNamedWithAnAtE2E(t *testing.T) {
	c := qt.New(t)
	dev, devDB := declaredSQLServerDatabase(c)
	target, _ := declaredSQLServerDatabase(c)
	shadow, shadowDB := declaredSQLServerDatabase(c)
	at := sqlServerDatabaseNamedWithAnAt(c, devDB, dev)
	for _, write := range sqlServerWritesThroughANameWithAnAt {
		t.Run(write.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeSQLServerEscape(c, work, "", strings.ReplaceAll(write.statement, "{at}", at))
			refusal := `(?s).*sqlserver migration replay rejects ` +
				regexp.QuoteMeta(fmt.Sprintf("cross-database reference %q", strings.ReplaceAll(write.target, "{at}", at))) + ` because .*`

			for _, args := range sqlServerEscapeCommands(dev, target, shadow) {
				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()}, args...)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Matches, refusal, qt.Commentf("%v", args))
			}
			c.Assert(sqlServerStrings(c, devDB, "SELECT CAST(id AS nvarchar(20)) FROM ["+at+"].dbo.kept ORDER BY id"), qt.DeepEquals,
				[]string{"1"})
			c.Assert(sqlServerStrings(c, shadowDB, "SELECT name FROM sys.tables"), qt.HasLen, 0)
		})
	}
}

// TestSQLServerReplayRunsATrustedWriteThroughADatabaseNamedWithAnAtE2E is the
// control for the refusals: with trust in the replayed SQL, each write lands
// in the database named with an @, so the refusal is what kept it out.
func TestSQLServerReplayRunsATrustedWriteThroughADatabaseNamedWithAnAtE2E(t *testing.T) {
	for _, write := range sqlServerWritesThroughANameWithAnAt {
		t.Run(write.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			dev, devDB := declaredSQLServerDatabase(c)
			at := sqlServerDatabaseNamedWithAnAt(c, devDB, dev)
			writeSQLServerEscape(c, work, "", strings.ReplaceAll(write.statement, "{at}", at))

			runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev, "--trust-replay-sql")

			c.Assert(sqlServerStrings(c, devDB, "SELECT CAST(id AS nvarchar(20)) FROM ["+at+"].dbo.kept ORDER BY id"), qt.DeepEquals,
				[]string{"1", "2"})
		})
	}
}

// sqlServerHiddenDatabasePassword is the password of the login that cannot
// list the databases.
//
// #nosec G101 -- the password of a login the test creates and drops on a test server.
const sqlServerHiddenDatabasePassword = "Hd_login_pw_6688!"

// sqlServerLoginThatCannotListDatabases creates a login that owns the scratch
// database dev names, is a db_owner of the database other, and is denied VIEW
// ANY DATABASE, and returns the URL of dev as that login. admin is a session in
// master, where the server grants a server permission.
func sqlServerLoginThatCannotListDatabases(c *qt.C, admin, devDB, otherDB *sql.DB, dev string) string {
	c.Helper()
	login := fmt.Sprintf("hd6688_%d", time.Now().UnixNano()%1_000_000_000)
	declaredSQLServerExec(c, admin, "CREATE LOGIN ["+login+"] WITH PASSWORD = '"+sqlServerHiddenDatabasePassword+"', CHECK_POLICY = OFF")
	// The login is server-wide, so it is dropped on its own after the two
	// databases, which it owns or is a user of.
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP LOGIN ["+login+"]")
		c.Check(err, qt.IsNil)
	})
	declaredSQLServerExec(c, admin, "DENY VIEW ANY DATABASE TO ["+login+"]")
	database := sqlServerDatabaseName(c, dev)
	owner := sqlServerStrings(c, admin, "SELECT SUSER_SNAME(owner_sid) FROM sys.databases WHERE name = N'"+database+"'")
	c.Assert(owner, qt.HasLen, 1)
	declaredSQLServerExec(c, devDB, "ALTER AUTHORIZATION ON DATABASE::["+database+"] TO ["+login+"]")
	// A login that owns a database cannot be dropped, so the owner is given
	// back first.
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "ALTER AUTHORIZATION ON DATABASE::["+database+"] TO ["+owner[0]+"]")
		c.Check(err, qt.IsNil)
	})
	declaredSQLServerExec(c, otherDB, "CREATE USER hd FOR LOGIN ["+login+"]", "ALTER ROLE db_owner ADD MEMBER hd")
	parsed, err := url.Parse(dev)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(login, sqlServerHiddenDatabasePassword)
	return parsed.String()
}

// sqlServerMasterSession is a session in master on the server dbtarget names.
func sqlServerMasterSession(c *qt.C) *sql.DB {
	c.Helper()
	admin, err := sql.Open("sqlserver", replaceSQLServerDatabase(c, dbtarget.URL(c, dbtarget.SQLServer), "master"))
	c.Assert(err, qt.IsNil)
	return admin
}

// A login denied VIEW ANY DATABASE sees no row for the other database in
// sys.databases and no DB_ID for it, yet still writes into it, since its user
// there is a db_owner. HAS_DBACCESS answers 1 for it, so the guard refuses the
// write without trust in the replayed SQL; trusted, the same write lands.
func TestSQLServerReplayRefusesAWriteIntoADatabaseTheLoginCannotListE2E(t *testing.T) {
	c := qt.New(t)
	admin := sqlServerMasterSession(c)
	dev, devDB := declaredSQLServerDatabase(c)
	otherURL, otherDB := declaredSQLServerDatabase(c)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	name := sqlServerDatabaseName(c, otherURL)
	declaredSQLServerExec(c, otherDB, "CREATE TABLE dbo.kept (id int PRIMARY KEY)", "INSERT dbo.kept VALUES (1)")
	asLogin := sqlServerLoginThatCannotListDatabases(c, admin, devDB, otherDB, dev)
	loginDB, err := sql.Open("sqlserver", asLogin)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(loginDB.Close(), qt.IsNil) })
	c.Assert(sqlServerStrings(c, loginDB, "SELECT CONCAT(ISNULL(DB_ID(N'"+name+"'), 0), N' ', HAS_DBACCESS(N'"+name+"'), N' ', "+
		"(SELECT COUNT(*) FROM sys.databases WHERE name = N'"+name+"'))"), qt.DeepEquals, []string{"0 1 0"})
	loginDB.SetMaxIdleConns(0)
	work := c.TempDir()
	writeSQLServerEscape(c, work, "", "INSERT INTO "+name+".dbo.kept VALUES (2);")

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work, Env: environmentWithoutPtahVariables()},
		"migrations", "validate", "--dir", "migrations", "--dev-url", asLogin)

	c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Matches, `(?s).*sqlserver migration replay rejects cross-database reference "`+regexp.QuoteMeta(name)+
		`\.dbo\.kept" because .*`)
	c.Assert(sqlServerStrings(c, otherDB, "SELECT CAST(id AS nvarchar(20)) FROM dbo.kept ORDER BY id"), qt.DeepEquals, []string{"1"})

	runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", asLogin, "--trust-replay-sql")

	c.Assert(sqlServerStrings(c, otherDB, "SELECT CAST(id AS nvarchar(20)) FROM dbo.kept ORDER BY id"), qt.DeepEquals, []string{"1", "2"})
}

// sqlServerLocalDBCC reseeds and checks a table of the dev database, naming it
// in several spellings the server reads. DBCC CHECKTABLE reads a snapshot of
// the database, which holds no table the migration's open transaction
// created, so it is pinned in the guard's unit tests instead.
const sqlServerLocalDBCC = "CREATE TABLE dbo.seeded (id int IDENTITY(1, 1), v int);\n" +
	"INSERT dbo.seeded (v) VALUES (1);\n" +
	"DBCC CHECKIDENT ('dbo.seeded', RESEED, 100);\n" +
	"DBCC CHECKIDENT (seeded);\n" +
	"DBCC CHECKIDENT (N'[dbo].[seeded]', NORESEED);\n" +
	"DBCC CHECKCONSTRAINTS ('dbo.seeded');\n" +
	"INSERT dbo.seeded (v) VALUES (2);\n" +
	"INSERT dbo.items SELECT id FROM dbo.seeded WHERE v = 2;"

// TestSQLServerReplayRunsADBCCOnATableOfTheDevDatabaseE2E replays the reseed
// through `migrations validate`, applies it, and replays it again through
// `migrations generate --shadow-db`, neither told to trust the SQL. The row
// inserted after the reseed takes identity 101.
func TestSQLServerReplayRunsADBCCOnATableOfTheDevDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	work := c.TempDir()
	dev, _ := declaredSQLServerDatabase(c)
	target, db := declaredSQLServerDatabase(c)
	shadow, _ := declaredSQLServerDatabase(c)
	writeSQLServerEscape(c, work, "", sqlServerLocalDBCC)

	runSQLServerRename(c, work, "migrations", "validate", "--dir", "migrations", "--dev-url", dev)
	runSQLServerRename(c, work, "migrations", "up", "--db-url", target, "--migrations-dir", "migrations")
	runSQLServerRename(c, work, "migrations", "generate", "--db-url", target, "--migrations-dir", "migrations",
		"--schema-file", "desired.sql", "--shadow-db", shadow, "--name", "more")

	c.Assert(sqlServerStrings(c, db, "SELECT CAST(id AS nvarchar(20)) FROM dbo.items"), qt.DeepEquals, []string{"101"})
}
