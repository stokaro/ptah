package sqlserverreplay_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/sqlserverreplay"
)

// databaseSession is a dev session in database. HAS_DBACCESS answers access
// for a name access lists, an int64 or nil for NULL, and 1 for every other
// name, so a database part the test does not list is one the session
// reaches. A failure to read the database is err, and a failure to ask about
// a name is accessErr. The session records the name of each question in asked,
// "" for the read of the database.
type databaseSession struct {
	database  string
	access    map[string]any
	err       error
	accessErr error
	asked     *[]string
}

func (s databaseSession) QueryRowContext(_ context.Context, query string, args ...any) dataext.Row {
	name := fmt.Sprint(args...)
	*s.asked = append(*s.asked, name)
	answer, listed := s.access[name]
	rows := map[bool]dataext.Row{
		false: databaseRow{value: s.database, err: s.err},
		true:  accessRow{value: map[bool]any{true: answer, false: int64(1)}[listed], err: s.accessErr},
	}
	return rows[strings.Contains(query, "HAS_DBACCESS")]
}

// databaseRow is the row of DB_NAME().
type databaseRow struct {
	value string
	err   error
}

func (r databaseRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.value
	return r.err
}

func (r databaseRow) Err() error { return r.err }

// accessRow is the row of HAS_DBACCESS.
type accessRow struct {
	value any
	err   error
}

func (r accessRow) Scan(dest ...any) error {
	return errors.Join(dest[0].(sql.Scanner).Scan(r.value), r.err)
}

func (r accessRow) Err() error { return r.err }

// sessionGuard is the SQL Server guard as a guard that reads the session.
func sessionGuard(c *qt.C) replay.SessionGuard {
	c.Helper()
	reader, ok := sqlserverreplay.Services()[0].Guard.(replay.SessionGuard)
	c.Assert(ok, qt.IsTrue)
	return reader
}

// validateIn validates statement with the guard of a dev session in
// database, which reaches every database a statement names.
func validateIn(c *qt.C, database, statement string, scope replay.Scope) error {
	c.Helper()
	return guardFor(c, databaseSession{database: database, asked: new([]string)}).Validate(statement, scope)
}

// guardFor is the guard of session.
func guardFor(c *qt.C, session databaseSession) replay.Guard {
	c.Helper()
	guard, err := sessionGuard(c).ValidateSession(c.Context(), session, replay.Scope{})
	c.Assert(err, qt.IsNil)
	return guard
}

// The guard reads the database of the session once, and the guard it returns
// takes a name of that database for a local one.
func TestGuard_ValidateSession_HappyPath(t *testing.T) {
	c := qt.New(t)
	asked := new([]string)

	guard, err := sessionGuard(c).ValidateSession(c.Context(), databaseSession{database: "ptah dev", asked: asked}, replay.Scope{})

	c.Assert(err, qt.IsNil)
	c.Assert(*asked, qt.DeepEquals, []string{""})
	c.Assert(guard.Validate("SELECT id FROM [ptah dev].dbo.t", replay.Scope{}), qt.IsNil)
	c.Assert(*asked, qt.DeepEquals, []string{""})
}

// A session the guard cannot read refuses the run, and is not a refusal that
// trust lifts.
func TestGuard_ValidateSession_FailurePath(t *testing.T) {
	c := qt.New(t)
	lost := errors.New("connection lost")

	guard, err := sessionGuard(c).ValidateSession(c.Context(), databaseSession{err: lost, asked: new([]string)}, replay.Scope{})

	c.Assert(err, qt.ErrorIs, lost)
	c.Assert(err, qt.ErrorMatches, "read the database of the dev session: connection lost")
	c.Assert(err, qt.Not(qt.ErrorIs), replay.ErrRefused)
	c.Assert(guard, qt.IsNil)
}

// referencesToAnotherDatabase holds a name of a database the dev session
// reaches, mostly other, in each place a statement writes one as identifiers:
// the target of a write in every form T-SQL gives it, a table a read names,
// the body of a view, a function, a procedure and a trigger, NEXT VALUE FOR,
// CREATE and UPDATE STATISTICS, WRITETEXT, UPDATETEXT and READTEXT, OUTPUT
// INTO, REFERENCES, and the procedure an EXEC runs; and in every spelling of
// the database part. The dev session is in thisdb and reaches every database
// a row names. Each row names the first name the refusal reports. Every
// write, with m6688_other for other, wrote into that database on SQL Server
// 2025.
var referencesToAnotherDatabase = []struct {
	name      string
	statement string
	reference string
}{
	{name: "INSERT into a name with the schema left out", statement: "INSERT INTO other..kept VALUES (1)", reference: "other..kept"},
	{name: "CREATE TABLE with the schema left out", statement: "CREATE TABLE other..kept (id int)", reference: "other..kept"},
	{name: "DROP TABLE with the schema left out", statement: "DROP TABLE other..kept", reference: "other..kept"},
	{name: "UPDATE with the schema left out", statement: "UPDATE other..kept SET id = 1", reference: "other..kept"},
	{name: "DELETE with the schema left out", statement: "DELETE FROM other..kept", reference: "other..kept"},
	{name: "TRUNCATE with the schema left out", statement: "TRUNCATE TABLE other..kept", reference: "other..kept"},
	{name: "ALTER TABLE with the schema left out", statement: "ALTER TABLE other..kept ADD c int", reference: "other..kept"},
	{
		// The guard reads a word before a leading dot as a part of the name,
		// since it does not know which words are reserved, and refuses it.
		name:      "a name that leaves the server out",
		statement: "INSERT INTO .other.dbo.kept VALUES (2)",
		reference: "INTO.other.dbo.kept",
	},
	{name: "a name that leaves the server out after a comma", statement: "SELECT 1 FROM dbo.t AS a, .other.dbo.kept AS b", reference: ".other.dbo.kept"},
	{name: "DELETE with no FROM", statement: "DELETE other.dbo.kept", reference: "other.dbo.kept"},
	{name: "INSERT with no INTO", statement: "INSERT other.dbo.kept VALUES (2)", reference: "other.dbo.kept"},
	{name: "UPDATE TOP", statement: "UPDATE TOP (1) other.dbo.kept SET id = 9", reference: "other.dbo.kept"},
	{name: "UPDATE through an alias", statement: "UPDATE x SET id = 9 FROM other.dbo.kept AS x", reference: "other.dbo.kept"},
	{name: "UPDATE through an alias with no AS", statement: "UPDATE x SET id = 9 FROM other.dbo.kept x", reference: "other.dbo.kept"},
	{name: "UPDATE through the table's own name", statement: "UPDATE kept SET id = 9 FROM other.dbo.kept", reference: "other.dbo.kept"},
	{
		name:      "UPDATE through an alias in a join",
		statement: "UPDATE x SET id = 9 FROM dbo.t AS a JOIN other.dbo.kept AS x ON x.id = a.a",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias after a subquery with a FROM",
		statement: "UPDATE x SET id = (SELECT MAX(id) + 8 FROM dbo.items) FROM other.dbo.kept AS x",
		reference: "other.dbo.kept",
	},
	{name: "UPDATE through the alias of a function", statement: "UPDATE x SET id = 9 FROM other.dbo.f() AS x", reference: "other.dbo.f"},
	{name: "DELETE through an alias", statement: "DELETE FROM x FROM other.dbo.kept AS x", reference: "other.dbo.kept"},
	{name: "DELETE through the table's own name", statement: "DELETE kept FROM other.dbo.kept", reference: "other.dbo.kept"},
	{name: "DELETE through a derived table", statement: "DELETE FROM x FROM (SELECT id FROM other.dbo.kept) AS x", reference: "other.dbo.kept"},
	{
		name:      "OUTPUT INTO",
		statement: "INSERT INTO dbo.items OUTPUT inserted.id + 10 INTO other.dbo.kept VALUES (3)",
		reference: "other.dbo.kept",
	},
	{
		name:      "the target of a MERGE",
		statement: "MERGE other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);",
		reference: "other.dbo.kept",
	},
	{
		name:      "the target of a MERGE TOP",
		statement: "MERGE TOP (1) other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE;",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) UPDATE c SET id = 9",
		reference: "other.dbo.kept",
	},
	{
		name:      "DELETE through a second common table expression",
		statement: "WITH a AS (SELECT id FROM other.dbo.kept), b AS (SELECT id FROM a) DELETE FROM b",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through a common table expression after one with a column list",
		statement: "WITH a (id) AS (SELECT 1), c AS (SELECT id FROM other.dbo.kept) UPDATE c SET id = 9",
		reference: "other.dbo.kept",
	},
	{
		name:      "INSERT through a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) INSERT INTO c VALUES (2)",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias of a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) UPDATE x SET id = 9 FROM c AS x",
		reference: "other.dbo.kept",
	},
	{name: "a constant EXEC creating a table with the schema left out", statement: "EXEC(N'CREATE TABLE other..escaped (id int)')", reference: "other..escaped"},
	{
		name:      "a constant EXEC of a MERGE",
		statement: "EXEC(N'MERGE other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);')",
		reference: "other.dbo.kept",
	},
	{name: "INSERT of rows another database holds", statement: "INSERT INTO dbo.t SELECT id FROM other.dbo.kept", reference: "other.dbo.kept"},
	{
		name:      "DELETE filtered by another database",
		statement: "DELETE FROM dbo.t WHERE id IN (SELECT id FROM other.dbo.kept)",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias joined to another database",
		statement: "UPDATE x SET a = k.id FROM dbo.t AS x JOIN other.dbo.kept AS k ON k.id = x.a",
		reference: "other.dbo.kept",
	},
	{
		name:      "INSERT of rows a common table expression reads from another database",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) INSERT INTO dbo.t SELECT id FROM c",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias, with the same alias in a subquery that reads another database",
		statement: "UPDATE x SET a = 1 FROM dbo.t AS x WHERE EXISTS (SELECT 1 FROM other.dbo.kept AS x WHERE x.id = 1)",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias joined to a derived table that reads another database",
		statement: "UPDATE x SET a = k.id FROM dbo.t AS x JOIN (SELECT id FROM other.dbo.kept) AS k ON k.id = x.a",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE of a table set from the table of that name in another database",
		statement: "UPDATE t SET a = (SELECT MAX(id) FROM other.dbo.t)",
		reference: "other.dbo.t",
	},
	{
		name:      "UPDATE of a CROSS APPLY alias over another database",
		statement: "UPDATE x SET id = 21 FROM dbo.items AS y CROSS APPLY (SELECT id FROM other.dbo.kept) AS x",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE of an alias after a comma",
		statement: "UPDATE x SET id = 22 FROM dbo.items AS y, other.dbo.kept x",
		reference: "other.dbo.kept",
	},
	{
		name:      "UPDATE of an alias in nested parentheses",
		statement: "UPDATE x SET id = 23 FROM ((other.dbo.kept AS x JOIN dbo.items AS y ON 1 = 1))",
		reference: "other.dbo.kept",
	},
	{name: "a SELECT", statement: "SELECT id FROM other.dbo.kept", reference: "other.dbo.kept"},
	{name: "a MERGE source", statement: "MERGE dbo.t AS t USING other.dbo.kept AS s ON t.id = s.id WHEN MATCHED THEN DELETE;", reference: "other.dbo.kept"},
	{name: "an IF condition", statement: "IF EXISTS (SELECT 1 FROM other.dbo.kept) DELETE FROM dbo.t", reference: "other.dbo.kept"},
	{name: "a column named through its database", statement: "SELECT other.dbo.kept.id FROM dbo.kept", reference: "other.dbo.kept.id"},
	{name: "a view's body", statement: "CREATE VIEW dbo.v AS SELECT id FROM other.dbo.kept", reference: "other.dbo.kept"},
	{
		name:      "a function's body",
		statement: "CREATE FUNCTION dbo.f() RETURNS int AS BEGIN RETURN (SELECT MAX(id) FROM other.dbo.kept) END",
		reference: "other.dbo.kept",
	},
	{name: "a procedure's body", statement: "CREATE PROCEDURE dbo.p AS SELECT id FROM other.dbo.kept", reference: "other.dbo.kept"},
	{
		name:      "a trigger's body",
		statement: "CREATE TRIGGER dbo.tr ON dbo.t AFTER INSERT AS INSERT INTO other.dbo.kept VALUES (1)",
		reference: "other.dbo.kept",
	},
	{name: "NEXT VALUE FOR", statement: "SELECT NEXT VALUE FOR other.dbo.seq", reference: "other.dbo.seq"},
	{
		name:      "NEXT VALUE FOR in a default",
		statement: "ALTER TABLE dbo.t ADD CONSTRAINT df_t_a DEFAULT (NEXT VALUE FOR other.dbo.seq) FOR a",
		reference: "other.dbo.seq",
	},
	{name: "CREATE STATISTICS", statement: "CREATE STATISTICS s ON other.dbo.kept (id)", reference: "other.dbo.kept"},
	{name: "UPDATE STATISTICS", statement: "UPDATE STATISTICS other.dbo.kept", reference: "other.dbo.kept"},
	{name: "WRITETEXT", statement: "WRITETEXT other.dbo.tx.c @p 'pwned'", reference: "other.dbo.tx.c"},
	{name: "UPDATETEXT", statement: "UPDATETEXT other.dbo.tx.c @p 0 NULL 'pwned'", reference: "other.dbo.tx.c"},
	{name: "READTEXT", statement: "READTEXT other.dbo.tx.c @p 0 5", reference: "other.dbo.tx.c"},
	{
		name:      "the text pointer a WRITETEXT writes through",
		statement: "DECLARE @p binary(16) SELECT @p = TEXTPTR(c) FROM other.dbo.tx WRITETEXT other.dbo.tx.c @p 'pwned'",
		reference: "other.dbo.tx",
	},
	{
		name:      "REFERENCES",
		statement: "CREATE TABLE dbo.c (id int REFERENCES other.dbo.kept (id))",
		reference: "other.dbo.kept",
	},
	{name: "the procedure of an EXEC", statement: "EXEC other.dbo.p", reference: "other.dbo.p"},
	{name: "a constant EXEC of a SELECT", statement: "EXEC(N'SELECT id FROM other.dbo.kept')", reference: "other.dbo.kept"},
	{name: "a bracketed database", statement: "SELECT id FROM [other].[dbo].[kept]", reference: "other.dbo.kept"},
	{name: "a quoted database", statement: `SELECT id FROM "other"."dbo"."kept"`, reference: "other.dbo.kept"},
	{name: "a bracketed database with the schema left out", statement: "SELECT id FROM [other]..kept", reference: "other..kept"},
	{name: "a database after spaces and a comment", statement: "SELECT id FROM other /* x */ . dbo . kept", reference: "other.dbo.kept"},
	{name: "a linked server", statement: "SELECT id FROM srv.thisdb.dbo.kept", reference: "srv.thisdb.dbo.kept"},
	{
		// The first part of a four-part name can name a server, so a column
		// named through the dev database reads as a table on a linked server.
		name:      "a column named through the dev database",
		statement: "SELECT thisdb.dbo.t.a FROM dbo.t",
		reference: "thisdb.dbo.t.a",
	},
	{
		// The server compares database names under its collation, which can
		// take THISDB for thisdb; the guard compares them exactly.
		name:      "the dev database in another case",
		statement: "SELECT id FROM THISDB.dbo.t",
		reference: "THISDB.dbo.t",
	},
	{
		// The word before a leading dot reads as a part, as above.
		name:      "the dev database with the server left out after a word",
		statement: "SELECT id FROM .thisdb.dbo.t",
		reference: "FROM.thisdb.dbo.t",
	},
	{
		// A three-part column name is a name of another database where its
		// first part is a database the session reaches, as every name is
		// here; see TestGuard_AcceptsAColumnOrAMethodNamedThroughItsTable.
		name:      "a column named through its schema",
		statement: "SELECT dbo.t.a FROM dbo.t",
		reference: "dbo.t.a",
	},
	{
		// T-SQL lets @, # and $ follow the first character of a name, so the
		// server reads x@ as one name: each of these wrote into the
		// database x@ (or x#y) on SQL Server 2025, where the lexer alone
		// reads x, a variable, and .dbo.kept.
		name:      "a database name ending in @",
		statement: "INSERT INTO x@.dbo.kept VALUES (2)",
		reference: "x@.dbo.kept",
	},
	{name: "a database name with @ inside", statement: "INSERT INTO x@d_at2v.dbo.kept VALUES (2)", reference: "x@d_at2v.dbo.kept"},
	{name: "a database name ending in @ with the schema left out", statement: "INSERT INTO x@..kept VALUES (2)", reference: "x@..kept"},
	{name: "a database name ending in @@", statement: "INSERT INTO x@@.dbo.kept VALUES (2)", reference: "x@@.dbo.kept"},
	{name: "a database name ending in @ before a space", statement: "INSERT INTO x@ .dbo.kept VALUES (2)", reference: "x@.dbo.kept"},
	{name: "a read of a database name ending in @", statement: "SELECT * FROM x@.dbo.kept", reference: "x@.dbo.kept"},
	{name: "a schema name ending in @", statement: "UPDATE o.x@.kept SET id = 2", reference: "o.x@.kept"},
	{name: "a database name ending in @ on a linked server", statement: "SELECT * FROM lnk.x@.dbo.kept", reference: "lnk.x@.dbo.kept"},
	{name: "a database name with # inside", statement: "INSERT INTO x#y.dbo.kept VALUES (2)", reference: "x#y.dbo.kept"},
	{name: "a database name ending in #", statement: "INSERT INTO x#.dbo.kept VALUES (2)", reference: "x#.dbo.kept"},
	{name: "a database name that starts with a keyword and @", statement: "INSERT INTO SELECT@x.dbo.kept VALUES (2)", reference: "SELECT@x.dbo.kept"},
}

// TestGuard_RefusesAReferenceToAnotherDatabase replays each statement without
// trust, on a dev session in thisdb.
func TestGuard_RefusesAReferenceToAnotherDatabase(t *testing.T) {
	for _, test := range referencesToAnotherDatabase {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(
				validateIn(c, "thisdb", test.statement, replay.Scope{}),
				qt.ErrorMatches,
				refusal(fmt.Sprintf("cross-database reference %q", test.reference)),
			)
		})
	}
}

// TestGuard_TrustedSQLReferencesAnotherDatabase is the control for the
// refusals above: a run that trusts its SQL replays the same statements, so
// each refusal comes from the name and from nothing else in the statement.
func TestGuard_TrustedSQLReferencesAnotherDatabase(t *testing.T) {
	for _, test := range referencesToAnotherDatabase {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// A refusal of a name of another database says that trust lifts it, reads
// included.
func TestGuard_RefusesAReferenceToAnotherDatabaseOnlyWithoutTrust(t *testing.T) {
	c := qt.New(t)

	err := validateIn(c, "thisdb", "INSERT INTO dbo.t SELECT id FROM other.dbo.kept", replay.Scope{})

	c.Assert(err, qt.ErrorIs, replay.ErrRefused)
	c.Assert(err, qt.ErrorMatches, `sqlserver migration replay rejects cross-database reference "other.dbo.kept" `+
		`because a statement that reads or writes another database runs only when the replayed SQL is trusted`)
}

// TestGuard_AcceptsANameOfTheDevDatabase replays, without trust, statements
// that name only the dev database, thisdb, in every spelling of its name, and
// writes into it that read only it.
func TestGuard_AcceptsANameOfTheDevDatabase(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a bracketed database", statement: "SELECT id FROM [thisdb].dbo.t"},
		{name: "a quoted database", statement: `SELECT id FROM "thisdb"."dbo"."t"`},
		{name: "an INSERT", statement: "INSERT INTO thisdb.dbo.t VALUES (1)"},
		{name: "the schema left out", statement: "UPDATE thisdb..t SET a = 1"},
		{name: "the server left out", statement: "SELECT 1 FROM dbo.a AS a, .thisdb.dbo.t AS b"},
		{name: "the server and the database left out", statement: "SELECT 1 FROM dbo.a AS a, ..dbo.t AS b"},
		{name: "every qualifier left out", statement: "SELECT 1 FROM dbo.a AS a, ...t AS b"},
		{name: "a view's body", statement: "CREATE VIEW dbo.v AS SELECT id FROM thisdb.dbo.t"},
		{name: "NEXT VALUE FOR", statement: "SELECT NEXT VALUE FOR thisdb.dbo.seq"},
		{name: "CREATE STATISTICS", statement: "CREATE STATISTICS s ON [thisdb].dbo.t (id)"},
		{name: "WRITETEXT on a table named alone", statement: "WRITETEXT tx.c @p 'local'"},
		{name: "a local join", statement: "UPDATE x SET a = k.id FROM dbo.t AS x JOIN dbo.k AS k ON k.id = x.a"},
		{name: "OUTPUT with no INTO", statement: "DELETE FROM dbo.t OUTPUT deleted.id WHERE id = 1"},
		{name: "a local table named after a rowset function", statement: "INSERT INTO [OPENQUERY] (id) VALUES (1)"},
		{name: "a column named after a rowset function", statement: "SELECT openquery FROM dbo.t"},
		{name: "a column named after a file reader", statement: "SELECT fn_trace_gettable FROM dbo.t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{}), qt.IsNil)
		})
	}
}

// A guard that read no session takes no database part for the dev database,
// and refuses a name of it without trust.
func TestGuard_RefusesANameOfTheDevDatabaseWithoutASession(t *testing.T) {
	c := qt.New(t)

	c.Assert(validate("SELECT id FROM [thisdb].dbo.t", replay.Scope{}), qt.ErrorMatches, refusal(`cross-database reference "thisdb.dbo.t"`))
	c.Assert(validate("SELECT id FROM [thisdb].dbo.t", replay.Scope{TrustedSQL: true}), qt.IsNil)
}

// dbccOnTheDevDatabase holds DBCC CHECKIDENT, CHECKTABLE and CHECKCONSTRAINTS
// on a table of the dev database, thisdb, in every spelling of the table the
// server reads; reseeding an identity after seed data is a common step of a
// migration. The session reaches no database named p.
var dbccOnTheDevDatabase = []struct {
	name      string
	statement string
}{
	{name: "CHECKIDENT with RESEED", statement: "DBCC CHECKIDENT ('dbo.items', RESEED, 1)"},
	{name: "CHECKIDENT on a table named alone", statement: "DBCC CHECKIDENT ('items')"},
	{name: "CHECKIDENT in lower case", statement: "dbcc checkident ('dbo.items', reseed, 1)"},
	{name: "CHECKIDENT with NORESEED", statement: "DBCC CHECKIDENT ('dbo.items', NORESEED)"},
	{name: "CHECKIDENT with WITH NO_INFOMSGS", statement: "DBCC CHECKIDENT ('dbo.items', RESEED, 0) WITH NO_INFOMSGS"},
	{name: "CHECKIDENT on a unicode string", statement: "DBCC CHECKIDENT (N'dbo.items', RESEED, 1)"},
	{name: "CHECKIDENT on bracketed parts", statement: "DBCC CHECKIDENT ('[dbo].[items]', RESEED, 1)"},
	{name: "CHECKIDENT on quoted parts", statement: `DBCC CHECKIDENT ('"dbo"."items"', RESEED, 1)`},
	{name: "CHECKIDENT on an unquoted name", statement: "DBCC CHECKIDENT (items, RESEED, 1)"},
	{name: "CHECKIDENT on a bracketed name", statement: "DBCC CHECKIDENT ([items], RESEED, 1)"},
	{name: "CHECKIDENT on a quoted name", statement: `DBCC CHECKIDENT ("dbo.items", RESEED, 1)`},
	{name: "CHECKIDENT through the dev database", statement: "DBCC CHECKIDENT ('thisdb.dbo.items', RESEED, 1)"},
	{name: "CHECKIDENT through the dev database with the schema left out", statement: "DBCC CHECKIDENT ('thisdb..items', RESEED, 1)"},
	{name: "CHECKIDENT through a database the session does not reach", statement: "DBCC CHECKIDENT ('p.dbo.items', RESEED, 1)"},
	{name: "CHECKIDENT reseeded to a variable", statement: "DBCC CHECKIDENT ('dbo.items', RESEED, @seed)"},
	{name: "CHECKTABLE", statement: "DBCC CHECKTABLE ('dbo.items')"},
	{name: "CHECKTABLE with an option", statement: "DBCC CHECKTABLE (items, NOINDEX) WITH NO_INFOMSGS"},
	{name: "CHECKCONSTRAINTS on a table", statement: "DBCC CHECKCONSTRAINTS ('dbo.items')"},
	{name: "CHECKCONSTRAINTS on the database", statement: "DBCC CHECKCONSTRAINTS"},
	{name: "CHECKCONSTRAINTS on the database with an option", statement: "DBCC CHECKCONSTRAINTS WITH ALL_CONSTRAINTS"},
	{name: "a constant EXEC of CHECKIDENT", statement: "EXEC(N'DBCC CHECKIDENT (''dbo.items'', RESEED, 1)')"},
}

// TestGuard_RunsADBCCOnTheDevDatabase replays each DBCC without trust.
func TestGuard_RunsADBCCOnTheDevDatabase(t *testing.T) {
	for _, test := range dbccOnTheDevDatabase {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := databaseSession{database: "thisdb", access: map[string]any{"p": nil}, asked: new([]string)}

			c.Assert(guardFor(c, session).Validate(test.statement, replay.Scope{}), qt.IsNil)
		})
	}
}

// dbccOnAnotherTable holds DBCC CHECKIDENT, CHECKTABLE and CHECKCONSTRAINTS
// on a table of another database, other, which the session reaches, and on a
// table the guard cannot read. DBCC CHECKIDENT ('other.dbo.ident', RESEED,
// 5000) reseeded m6688_other.dbo.ident on SQL Server 2025, and so did the
// bracketed and the double-quoted name.
var dbccOnAnotherTable = []struct {
	name      string
	statement string
	operation string
}{
	{name: "CHECKIDENT on another database", statement: "DBCC CHECKIDENT ('other.dbo.ident', RESEED, 5000)", operation: `DBCC CHECKIDENT of "other.dbo.ident"`},
	{name: "CHECKIDENT with the schema left out", statement: "DBCC CHECKIDENT ('other..ident', RESEED, 5000)", operation: `DBCC CHECKIDENT of "other..ident"`},
	{name: "CHECKIDENT on bracketed parts", statement: "DBCC CHECKIDENT (N'[other].[dbo].[ident]', RESEED, 5000)", operation: `DBCC CHECKIDENT of "other.dbo.ident"`},
	{name: "CHECKIDENT on a bracketed name", statement: "DBCC CHECKIDENT ([other.dbo.ident], RESEED, 5000)", operation: `DBCC CHECKIDENT of "other.dbo.ident"`},
	{name: "CHECKIDENT on a quoted name", statement: `DBCC CHECKIDENT ("other.dbo.ident", RESEED, 5000)`, operation: `DBCC CHECKIDENT of "other.dbo.ident"`},
	{name: "CHECKIDENT on a linked server", statement: "DBCC CHECKIDENT ('srv.thisdb.dbo.ident')", operation: `DBCC CHECKIDENT of "srv.thisdb.dbo.ident"`},
	{name: "CHECKIDENT on the dev database in another case", statement: "DBCC CHECKIDENT ('THISDB.dbo.ident')", operation: `DBCC CHECKIDENT of "THISDB.dbo.ident"`},
	{name: "CHECKTABLE on another database", statement: "DBCC CHECKTABLE ('other.dbo.kept')", operation: `DBCC CHECKTABLE of "other.dbo.kept"`},
	{name: "CHECKCONSTRAINTS on another database", statement: "DBCC CHECKCONSTRAINTS ('other.dbo.kept')", operation: `DBCC CHECKCONSTRAINTS of "other.dbo.kept"`},
	{name: "a constant EXEC of CHECKIDENT", statement: "EXEC(N'DBCC CHECKIDENT (''other.dbo.ident'', RESEED, 5000)')", operation: `DBCC CHECKIDENT of "other.dbo.ident"`},
	{name: "CHECKIDENT on a variable", statement: "DBCC CHECKIDENT (@table, RESEED, 5000)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT on a concatenation", statement: "DBCC CHECKIDENT ('dbo.' + 'items', RESEED, 1)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT on a parenthesized string", statement: "DBCC CHECKIDENT (('dbo.items'), RESEED, 1)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT on a name with a space", statement: "DBCC CHECKIDENT ('dbo . items', RESEED, 1)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT on an empty table name", statement: "DBCC CHECKIDENT ('dbo.', RESEED, 1)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT with no table", statement: "DBCC CHECKIDENT", operation: "DBCC CHECKIDENT"},
	{name: "CHECKTABLE on a number", statement: "DBCC CHECKTABLE (581577110)", operation: "DBCC CHECKTABLE"},
	{name: "CHECKCONSTRAINTS on a variable", statement: "DBCC CHECKCONSTRAINTS (@id)", operation: "DBCC CHECKCONSTRAINTS"},
}

// TestGuard_RefusesADBCCOnAnotherTable replays each DBCC without trust.
func TestGuard_RefusesADBCCOnAnotherTable(t *testing.T) {
	for _, test := range dbccOnAnotherTable {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLRunsADBCCOnAnotherTable is the control for the
// refusals above.
func TestGuard_TrustedSQLRunsADBCCOnAnotherTable(t *testing.T) {
	for _, test := range dbccOnAnotherTable {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// A refusal of a DBCC says why and that trust lifts it; a DBCC that acts on
// the server is refused under trust too.
func TestGuard_RefusesADBCC(t *testing.T) {
	c := qt.New(t)

	c.Assert(validateIn(c, "thisdb", "DBCC CHECKIDENT ('other.dbo.ident', RESEED, 5000)", replay.Scope{}), qt.ErrorMatches,
		`sqlserver migration replay rejects DBCC CHECKIDENT of "other.dbo.ident" because the table it names is in another `+
			`database, which a statement reaches only when the replayed SQL is trusted`)
	c.Assert(validateIn(c, "thisdb", "DBCC CHECKIDENT (@table, RESEED, 5000)", replay.Scope{}), qt.ErrorMatches,
		`sqlserver migration replay rejects DBCC CHECKIDENT because its table is not a constant name, so the guard cannot `+
			`tell which database it names; it runs only when the replayed SQL is trusted`)
	c.Assert(validateIn(c, "thisdb", "DBCC FREEPROCCACHE", replay.Scope{}), qt.ErrorMatches, refusal("DBCC server operation"))
	c.Assert(validateIn(c, "thisdb", "DBCC FREEPROCCACHE", replay.Scope{TrustedSQL: true}), qt.ErrorMatches, refusal("DBCC server operation"))
}

// A guard that read no session takes the database part of a DBCC's table for
// another database.
func TestGuard_RefusesADBCCThroughADatabaseWithoutASession(t *testing.T) {
	c := qt.New(t)

	c.Assert(validate("DBCC CHECKIDENT ('p.dbo.items', RESEED, 1)", replay.Scope{}), qt.ErrorMatches, refusal(`DBCC CHECKIDENT of "p.dbo.items"`))
	c.Assert(validate("DBCC CHECKIDENT ('dbo.items', RESEED, 1)", replay.Scope{}), qt.IsNil)
}

// columnsAndMethods name a column, or a method on one, through a table, an
// alias or a schema: three parts that read like a database, a schema and an
// object. The reviewer's view, generated by Ptah, holds the first three.
var columnsAndMethods = []struct {
	name      string
	statement string
}{
	{
		name: "a view with a geography, a hierarchyid and an xml method",
		statement: "CREATE VIEW dbo.v_places AS SELECT p.id, p.geo.STAsText() AS wkt, p.node.GetLevel() AS lvl, " +
			"p.doc.value('(/a)[1]', 'int') AS a FROM dbo.places AS p",
	},
	{name: "an xml nodes method", statement: "SELECT n.c.value('.', 'int') AS v FROM dbo.docs AS t CROSS APPLY t.doc.nodes('/a') AS n(c)"},
	{name: "a computed column", statement: "CREATE TABLE dbo.g3 (geo geography, area AS g3.geo.STArea())"},
	{name: "a check constraint", statement: "ALTER TABLE dbo.g5 ADD CONSTRAINT ck_geo CHECK (g5.geo.STIsValid() = 1)"},
	{name: "a column named through its schema", statement: "SELECT dbo.t.c FROM dbo.t"},
}

// columnAccess is HAS_DBACCESS for the first parts of columnsAndMethods: the
// session reaches no database of any of these names. p is not a database, and
// t a database the login is not mapped into.
var columnAccess = map[string]any{"p": nil, "n": nil, "t": int64(0), "g3": nil, "g5": nil, "dbo": nil}

// TestGuard_AcceptsAColumnOrAMethodNamedThroughItsTable replays each statement
// without trust, on a session that reaches no database named like the first
// part of a three-part name: the server reads such a name as a column.
func TestGuard_AcceptsAColumnOrAMethodNamedThroughItsTable(t *testing.T) {
	for _, test := range columnsAndMethods {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := databaseSession{database: "thisdb", access: columnAccess, asked: new([]string)}

			c.Assert(guardFor(c, session).Validate(test.statement, replay.Scope{}), qt.IsNil)
		})
	}
}

// The same statements on a session that reaches a database of each first
// part are names of those databases, and refused.
func TestGuard_RefusesAColumnNamedThroughADatabaseTheSessionReaches(t *testing.T) {
	for _, test := range columnsAndMethods {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{}), qt.ErrorMatches,
				`sqlserver migration replay rejects cross-database reference .+`)
		})
	}
}

// The guard asks the session about a database part once per run, and asks
// nothing about an empty part, the dev database, a name with a server part,
// or a name of fewer than three parts.
func TestGuard_AsksTheSessionOncePerDatabasePart(t *testing.T) {
	c := qt.New(t)
	asked := new([]string)
	guard := guardFor(c, databaseSession{database: "thisdb", access: map[string]any{"p": nil}, asked: asked})

	c.Assert(guard.Validate("SELECT p.geo.STAsText() FROM dbo.places AS p", replay.Scope{}), qt.IsNil)
	c.Assert(guard.Validate("SELECT p.geo.STArea(), x.id FROM dbo.places AS p, thisdb.dbo.t AS x, ..dbo.t AS y, dbo.t AS z", replay.Scope{}), qt.IsNil)
	c.Assert(guard.Validate("SELECT * FROM lnk.p.dbo.t", replay.Scope{}), qt.ErrorMatches, refusal(`cross-database reference "lnk.p.dbo.t"`))
	c.Assert(*asked, qt.DeepEquals, []string{"", "p"})
}

// databaseAccess holds the three answers of HAS_DBACCESS for the database
// part p of p.geo.STAsText(): 1 is a database the session reaches, so the
// name is refused; 0, a database the login is not mapped into, and NULL, no
// database of that name, leave a column.
var databaseAccess = []struct {
	name   string
	answer any
	want   string
}{
	{name: "1", answer: int64(1), want: "refused"},
	{name: "0", answer: int64(0), want: "accepted"},
	{name: "NULL", answer: nil, want: "accepted"},
}

// TestGuard_ReadsEachAnswerOfTheSession replays one method call on a session
// that answers each value of HAS_DBACCESS for its first part.
func TestGuard_ReadsEachAnswerOfTheSession(t *testing.T) {
	for _, test := range databaseAccess {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := databaseSession{database: "thisdb", access: map[string]any{"p": test.answer}, asked: new([]string)}

			err := guardFor(c, session).Validate("SELECT p.geo.STAsText() FROM dbo.places AS p", replay.Scope{})

			c.Assert(map[bool]string{true: "accepted", false: "refused"}[err == nil], qt.Equals, test.want)
		})
	}
}

// A run that trusts its SQL asks the session nothing about a name.
func TestGuard_TrustedSQLAsksNothing(t *testing.T) {
	c := qt.New(t)
	asked := new([]string)
	guard := guardFor(c, databaseSession{database: "thisdb", asked: asked})

	c.Assert(guard.Validate("INSERT INTO other.dbo.kept SELECT p.geo.STAsText() FROM p.dbo.places", replay.Scope{TrustedSQL: true}), qt.IsNil)
	c.Assert(guard.Validate("DBCC CHECKIDENT ('other.dbo.ident', RESEED, 5000)", replay.Scope{TrustedSQL: true}), qt.IsNil)
	c.Assert(*asked, qt.DeepEquals, []string{""})
}

// A question the session cannot answer fails the statement with the
// session's error, which is not a refusal that trust lifts, and leaves the
// name unanswered for the next statement.
func TestGuard_FailsWhereTheSessionCannotAnswer(t *testing.T) {
	c := qt.New(t)
	lost := errors.New("connection lost")
	asked := new([]string)
	guard := guardFor(c, databaseSession{database: "thisdb", accessErr: lost, asked: asked})

	err := guard.Validate("SELECT p.geo.STAsText() FROM dbo.places AS p", replay.Scope{})

	c.Assert(err, qt.ErrorIs, lost)
	c.Assert(err, qt.ErrorMatches, `ask the dev session whether it reaches the database "p": connection lost`)
	c.Assert(err, qt.Not(qt.ErrorIs), replay.ErrRefused)
	c.Assert(guard.Validate("SELECT p.geo.STArea() FROM dbo.places AS p", replay.Scope{}), qt.ErrorIs, lost)
	c.Assert(*asked, qt.DeepEquals, []string{"", "p", "p"})
}

// readsOutsideTheServer holds a function that reads a file on the server or a
// remote source, wherever it stands and however its name is spelled, and a
// signature, which gives a module the permissions of a certificate's user or
// login. SQL Server 2025 read /etc/hostname for the first row, listed /etc
// for the file system row and returned the rows of a trace file for the trace
// rows.
var readsOutsideTheServer = []struct {
	name      string
	statement string
	operation string
}{
	{
		name:      "OPENROWSET reading a file on the server",
		statement: "SELECT BulkColumn FROM OPENROWSET(BULK '/etc/hostname', SINGLE_CLOB) AS x",
		operation: "OPENROWSET",
	},
	{
		name:      "OPENDATASOURCE",
		statement: "SELECT id FROM OPENDATASOURCE('MSOLEDBSQL', 'Server=remote;').otherdb.dbo.t",
		operation: "OPENDATASOURCE",
	},
	{name: "OPENQUERY", statement: "SELECT id FROM OPENQUERY(remote_link, 'SELECT id FROM t')", operation: "OPENQUERY"},
	{
		name:      "OPENROWSET in a common table expression",
		statement: "WITH r AS (SELECT BulkColumn FROM OPENROWSET(BULK '/etc/hostname', SINGLE_CLOB) AS x) INSERT INTO dbo.t SELECT 1 FROM r",
		operation: "OPENROWSET",
	},
	{
		name:      "the file system listed",
		statement: "SELECT file_or_directory_name FROM sys.dm_os_enumerate_filesystem(N'/etc', N'*')",
		operation: "dm_os_enumerate_filesystem",
	},
	{name: "a path tested", statement: "SELECT file_exists FROM sys.dm_os_file_exists(N'/etc/hostname')", operation: "dm_os_file_exists"},
	{
		name:      "an audit file",
		statement: "SELECT * FROM sys.fn_get_audit_file(N'/var/opt/mssql/log/*.sqlaudit', DEFAULT, DEFAULT)",
		operation: "fn_get_audit_file",
	},
	{
		name:      "an audit file through the second reader",
		statement: "SELECT * FROM sys.fn_get_audit_file_v2(N'/var/opt/mssql/log/*.sqlaudit', DEFAULT, DEFAULT, DEFAULT, DEFAULT)",
		operation: "fn_get_audit_file_v2",
	},
	{
		name:      "an extended event file",
		statement: "SELECT * FROM sys.fn_xe_file_target_read_file(N'/var/opt/mssql/log/*.xel', NULL, NULL, NULL)",
		operation: "fn_xe_file_target_read_file",
	},
	{name: "a trace file", statement: "SELECT * FROM sys.fn_trace_gettable(N'/var/opt/mssql/log/log.trc', DEFAULT)", operation: "fn_trace_gettable"},
	{name: "a trace file named alone", statement: "SELECT * FROM fn_trace_gettable(N'/x.trc', DEFAULT)", operation: "fn_trace_gettable"},
	{name: "a trace file named with two colons", statement: "SELECT * FROM ::fn_trace_gettable(N'/x.trc', DEFAULT)", operation: "fn_trace_gettable"},
	{name: "a trace file named in brackets", statement: "SELECT * FROM [sys].[fn_trace_gettable](N'/x.trc', DEFAULT)", operation: "fn_trace_gettable"},
	{name: "a trace file named in upper case", statement: "SELECT * FROM SYS.FN_TRACE_GETTABLE(N'/x.trc', DEFAULT)", operation: "fn_trace_gettable"},
	{
		name:      "a trace file in a view's body",
		statement: "CREATE VIEW dbo.v AS SELECT * FROM sys.fn_trace_gettable(N'/x.trc', DEFAULT)",
		operation: "fn_trace_gettable",
	},
	{name: "a backup file", statement: "SELECT * FROM sys.fn_dump_dblog(NULL, NULL, N'DISK', 1, N'/etc/hostname')", operation: "fn_dump_dblog"},
	{name: "ADD SIGNATURE", statement: "ADD SIGNATURE TO dbo.p BY CERTIFICATE c", operation: "ADD SIGNATURE"},
	{name: "ADD COUNTER SIGNATURE", statement: "ADD COUNTER SIGNATURE TO dbo.p BY CERTIFICATE c", operation: "ADD SIGNATURE"},
}

// TestGuard_RefusesWhatReadsOutsideTheServer replays each statement without
// trust.
func TestGuard_RefusesWhatReadsOutsideTheServer(t *testing.T) {
	for _, test := range readsOutsideTheServer {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLReadsOutsideTheServer is the control for the refusals
// above: a run that trusts its SQL replays the same statements.
func TestGuard_TrustedSQLReadsOutsideTheServer(t *testing.T) {
	for _, test := range readsOutsideTheServer {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// A refusal of a file reader says what the function reads.
func TestGuard_RefusesAFileReaderWithItsReason(t *testing.T) {
	c := qt.New(t)

	c.Assert(validate("SELECT * FROM sys.fn_trace_gettable(N'/x.trc', DEFAULT)", replay.Scope{}), qt.ErrorMatches,
		`sqlserver migration replay rejects fn_trace_gettable because it reads trace files on the server`)
}
