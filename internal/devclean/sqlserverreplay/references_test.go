package sqlserverreplay_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/sqlserverreplay"
)

// databaseSession is a dev session in database: it answers every query with
// one row that holds the name, and counts the queries it is asked.
type databaseSession struct {
	database string
	err      error
	asked    *int
}

func (s databaseSession) QueryRowContext(context.Context, string, ...any) dataext.Row {
	*s.asked++
	return databaseRow(s)
}

type databaseRow databaseSession

func (r databaseRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.database
	return r.err
}

func (r databaseRow) Err() error { return r.err }

// sessionGuard is the SQL Server guard as a guard that reads the session.
func sessionGuard(c *qt.C) replay.SessionGuard {
	c.Helper()
	reader, ok := sqlserverreplay.Services()[0].Guard.(replay.SessionGuard)
	c.Assert(ok, qt.IsTrue)
	return reader
}

// validateIn validates statement with the guard of a dev session in
// database.
func validateIn(c *qt.C, database, statement string, scope replay.Scope) error {
	c.Helper()
	guard, err := sessionGuard(c).ValidateSession(c.Context(), databaseSession{database: database, asked: new(int)}, replay.Scope{})
	c.Assert(err, qt.IsNil)
	return guard.Validate(statement, scope)
}

// The guard reads the database of the session once, and the guard it returns
// takes a name of that database for a local one.
func TestGuard_ValidateSession_HappyPath(t *testing.T) {
	c := qt.New(t)
	asked := new(int)

	guard, err := sessionGuard(c).ValidateSession(c.Context(), databaseSession{database: "ptah dev", asked: asked}, replay.Scope{})

	c.Assert(err, qt.IsNil)
	c.Assert(*asked, qt.Equals, 1)
	c.Assert(guard.Validate("SELECT id FROM [ptah dev].dbo.t", replay.Scope{}), qt.IsNil)
}

// A session the guard cannot read refuses the run, and is not a refusal that
// trust lifts.
func TestGuard_ValidateSession_FailurePath(t *testing.T) {
	c := qt.New(t)
	lost := errors.New("connection lost")

	guard, err := sessionGuard(c).ValidateSession(c.Context(), databaseSession{err: lost, asked: new(int)}, replay.Scope{})

	c.Assert(err, qt.ErrorIs, lost)
	c.Assert(err, qt.ErrorMatches, "read the database of the dev session: connection lost")
	c.Assert(err, qt.Not(qt.ErrorIs), replay.ErrRefused)
	c.Assert(guard, qt.IsNil)
}

// referencesToAnotherDatabase holds a name of the database other, or one the
// guard cannot tell from it, in every place a statement can hold one: the
// target of a write in every form T-SQL gives it, a table a read names, the
// body of a view, a function, a procedure and a trigger, NEXT VALUE FOR,
// CREATE and UPDATE STATISTICS, WRITETEXT, UPDATETEXT and READTEXT, OUTPUT
// INTO, REFERENCES, and the procedure an EXEC runs; and in every spelling of
// the database part. The dev session is in thisdb. Each row names the first
// name the refusal reports. Every write, with m6688_other for other, wrote
// into that database on SQL Server 2025.
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
		// A guard that does not resolve names cannot tell schema.table.column
		// from database.schema.table.
		name:      "a column named through its schema",
		statement: "SELECT dbo.t.a FROM dbo.t",
		reference: "dbo.t.a",
	},
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

// localDBCC holds the DBCC commands that check or reseed a table the dev
// database holds, or another database's, since the string naming the table
// can name one: DBCC CHECKIDENT ('other.dbo.ident', RESEED, 5000) reseeded
// m6688_other.dbo.ident on SQL Server 2025.
var localDBCC = []struct {
	name      string
	statement string
	operation string
}{
	{name: "CHECKIDENT on another database", statement: "DBCC CHECKIDENT ('other.dbo.ident', RESEED, 5000)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT on the dev database", statement: "DBCC CHECKIDENT ('dbo.ident', RESEED, 1)", operation: "DBCC CHECKIDENT"},
	{name: "CHECKIDENT in lower case", statement: "dbcc checkident ('dbo.ident')", operation: "DBCC CHECKIDENT"},
	{name: "CHECKTABLE", statement: "DBCC CHECKTABLE ('dbo.t')", operation: "DBCC CHECKTABLE"},
	{name: "CHECKCONSTRAINTS", statement: "DBCC CHECKCONSTRAINTS ('dbo.t')", operation: "DBCC CHECKCONSTRAINTS"},
	{name: "a constant EXEC of CHECKIDENT", statement: "EXEC(N'DBCC CHECKIDENT (''other.dbo.ident'', RESEED, 5000)')", operation: "DBCC CHECKIDENT"},
}

// TestGuard_RefusesEveryDBCCWithoutTrust replays each DBCC without trust.
func TestGuard_RefusesEveryDBCCWithoutTrust(t *testing.T) {
	for _, test := range localDBCC {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateIn(c, "thisdb", test.statement, replay.Scope{}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLRunsALocalDBCC is the control for the refusals above.
func TestGuard_TrustedSQLRunsALocalDBCC(t *testing.T) {
	for _, test := range localDBCC {
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
		`sqlserver migration replay rejects DBCC CHECKIDENT because DBCC names the table it checks or reseeds in a string, `+
			`which can name another database, so it runs only when the replayed SQL is trusted`)
	c.Assert(validateIn(c, "thisdb", "DBCC FREEPROCCACHE", replay.Scope{}), qt.ErrorMatches, refusal("DBCC server operation"))
	c.Assert(validateIn(c, "thisdb", "DBCC FREEPROCCACHE", replay.Scope{TrustedSQL: true}), qt.ErrorMatches, refusal("DBCC server operation"))
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
