package sqlserverreplay_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// writesIntoAnotherDatabase holds a write into the database other in every
// form T-SQL gives the target, with the name the refusal reports. Each one,
// with m6688_other for other, wrote into that database on SQL Server 2025.
var writesIntoAnotherDatabase = []struct {
	name      string
	statement string
	target    string
}{
	{name: "INSERT into a name with the schema left out", statement: "INSERT INTO other..kept VALUES (1)", target: "other..kept"},
	{name: "CREATE TABLE with the schema left out", statement: "CREATE TABLE other..kept (id int)", target: "other..kept"},
	{name: "DROP TABLE with the schema left out", statement: "DROP TABLE other..kept", target: "other..kept"},
	{name: "UPDATE with the schema left out", statement: "UPDATE other..kept SET id = 1", target: "other..kept"},
	{name: "DELETE with the schema left out", statement: "DELETE FROM other..kept", target: "other..kept"},
	{name: "TRUNCATE with the schema left out", statement: "TRUNCATE TABLE other..kept", target: "other..kept"},
	{name: "ALTER TABLE with the schema left out", statement: "ALTER TABLE other..kept ADD c int", target: "other..kept"},
	{name: "a name that leaves the server out", statement: "INSERT INTO .other.dbo.kept VALUES (2)", target: ".other.dbo.kept"},
	{name: "DELETE with no FROM", statement: "DELETE other.dbo.kept", target: "other.dbo.kept"},
	{name: "INSERT with no INTO", statement: "INSERT other.dbo.kept VALUES (2)", target: "other.dbo.kept"},
	{name: "UPDATE TOP", statement: "UPDATE TOP (1) other.dbo.kept SET id = 9", target: "other.dbo.kept"},
	{name: "UPDATE through an alias", statement: "UPDATE x SET id = 9 FROM other.dbo.kept AS x", target: "other.dbo.kept"},
	{name: "UPDATE through an alias with no AS", statement: "UPDATE x SET id = 9 FROM other.dbo.kept x", target: "other.dbo.kept"},
	{name: "UPDATE through the table's own name", statement: "UPDATE kept SET id = 9 FROM other.dbo.kept", target: "other.dbo.kept"},
	{
		name:      "UPDATE through an alias in a join",
		statement: "UPDATE x SET id = 9 FROM dbo.t AS a JOIN other.dbo.kept AS x ON x.id = a.a",
		target:    "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias after a subquery with a FROM",
		statement: "UPDATE x SET id = (SELECT MAX(id) + 8 FROM dbo.items) FROM other.dbo.kept AS x",
		target:    "other.dbo.kept",
	},
	{name: "UPDATE through the alias of a function", statement: "UPDATE x SET id = 9 FROM other.dbo.f() AS x", target: "other.dbo.f"},
	{name: "DELETE through an alias", statement: "DELETE FROM x FROM other.dbo.kept AS x", target: "other.dbo.kept"},
	{name: "DELETE through the table's own name", statement: "DELETE kept FROM other.dbo.kept", target: "other.dbo.kept"},
	{name: "DELETE through a derived table", statement: "DELETE FROM x FROM (SELECT id FROM other.dbo.kept) AS x", target: "other.dbo.kept"},
	{
		name:      "OUTPUT INTO",
		statement: "INSERT INTO dbo.items OUTPUT inserted.id + 10 INTO other.dbo.kept VALUES (3)",
		target:    "other.dbo.kept",
	},
	{
		name:      "the target of a MERGE",
		statement: "MERGE other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);",
		target:    "other.dbo.kept",
	},
	{
		name:      "the target of a MERGE TOP",
		statement: "MERGE TOP (1) other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN MATCHED THEN DELETE;",
		target:    "other.dbo.kept",
	},
	{
		name:      "UPDATE through a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) UPDATE c SET id = 9",
		target:    "other.dbo.kept",
	},
	{
		name:      "DELETE through a second common table expression",
		statement: "WITH a AS (SELECT id FROM other.dbo.kept), b AS (SELECT id FROM a) DELETE FROM b",
		target:    "other.dbo.kept",
	},
	{
		name:      "UPDATE through a common table expression after one with a column list",
		statement: "WITH a (id) AS (SELECT 1), c AS (SELECT id FROM other.dbo.kept) UPDATE c SET id = 9",
		target:    "other.dbo.kept",
	},
	{
		name:      "INSERT through a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) INSERT INTO c VALUES (2)",
		target:    "other.dbo.kept",
	},
	{
		name:      "UPDATE through an alias of a common table expression",
		statement: "WITH c AS (SELECT id FROM other.dbo.kept) UPDATE x SET id = 9 FROM c AS x",
		target:    "other.dbo.kept",
	},
	{name: "a constant EXEC creating a table with the schema left out", statement: "EXEC(N'CREATE TABLE other..escaped (id int)')", target: "other..escaped"},
	{
		name:      "a constant EXEC of a MERGE",
		statement: "EXEC(N'MERGE other.dbo.kept AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);')",
		target:    "other.dbo.kept",
	},
}

// TestGuard_RefusesAWriteIntoAnotherDatabaseInEveryForm replays each write
// without trust. The guard reads the real target of each DML form: after TOP,
// with INTO and FROM left out, through an alias or a table name to the FROM
// clause, into OUTPUT INTO, and through a common table expression to the
// tables it reads.
func TestGuard_RefusesAWriteIntoAnotherDatabaseInEveryForm(t *testing.T) {
	for _, test := range writesIntoAnotherDatabase {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(
				validate(test.statement, replay.Scope{}),
				qt.ErrorMatches,
				refusal(fmt.Sprintf("cross-database target %q", test.target)),
			)
		})
	}
}

// TestGuard_TrustedSQLWritesIntoAnotherDatabaseInEveryForm is the control for
// the refusals above: a run that trusts its SQL replays the same writes, so
// each refusal comes from the target and from nothing else in the statement.
func TestGuard_TrustedSQLWritesIntoAnotherDatabaseInEveryForm(t *testing.T) {
	for _, test := range writesIntoAnotherDatabase {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// TestGuard_AcceptsAWriteThatReadsAnotherDatabase replays, without trust,
// writes into the dev database that only read another one. The target reader
// follows an alias and a common table expression to what the statement
// writes, not to every table it names.
func TestGuard_AcceptsAWriteThatReadsAnotherDatabase(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "INSERT of rows another database holds", statement: "INSERT INTO dbo.t SELECT id FROM other.dbo.kept"},
		{name: "DELETE filtered by another database", statement: "DELETE FROM dbo.t WHERE id IN (SELECT id FROM other.dbo.kept)"},
		{
			name:      "UPDATE through an alias joined to another database",
			statement: "UPDATE x SET a = k.id FROM dbo.t AS x JOIN other.dbo.kept AS k ON k.id = x.a",
		},
		{
			name:      "INSERT of rows a common table expression reads from another database",
			statement: "WITH c AS (SELECT id FROM other.dbo.kept) INSERT INTO dbo.t SELECT id FROM c",
		},
		{
			name:      "UPDATE through an alias, with the same alias in a subquery that reads another database",
			statement: "UPDATE x SET a = 1 FROM dbo.t AS x WHERE EXISTS (SELECT 1 FROM other.dbo.kept AS x WHERE x.id = 1)",
		},
		{
			name:      "UPDATE through an alias joined to a derived table that reads another database",
			statement: "UPDATE x SET a = k.id FROM dbo.t AS x JOIN (SELECT id FROM other.dbo.kept) AS k ON k.id = x.a",
		},
		{
			// With no FROM of the UPDATE's own, the target is the local t.
			name:      "UPDATE of a table set from the table of that name in another database",
			statement: "UPDATE t SET a = (SELECT MAX(id) FROM other.dbo.t)",
		},
		{name: "OUTPUT with no INTO", statement: "DELETE FROM dbo.t OUTPUT deleted.id WHERE id = 1"},
		{name: "a local table named after a rowset function", statement: "INSERT INTO [OPENQUERY] (id) VALUES (1)"},
		{name: "a column named after a rowset function", statement: "SELECT openquery FROM dbo.t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{}), qt.IsNil)
		})
	}
}

// readsOutsideTheServer holds a rowset function, which reads a file on the
// server or a remote source, and a signature, which gives a module the
// permissions of a certificate's user or login. SQL Server 2025 read
// /etc/hostname for the first row.
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
