package sqlserverreplay_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// TestGuard_RefusesAStatementWhereverTheBatchHoldsIt holds batches whose
// refused statement is not the first one, or is not written with EXEC. T-SQL
// needs no separator between statements, and the server runs each one; the
// statement boundaries were measured on SQL Server 2022 and 2025, see
// internal/sqlserverbatch.
func TestGuard_RefusesAStatementWhereverTheBatchHoldsIt(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{
			name:      "DROP DATABASE after an ALTER TABLE with no separator",
			statement: "ALTER TABLE dbo.t ADD DEFAULT 1 FOR c DROP DATABASE other",
			operation: "DROP DATABASE",
		},
		{
			name:      "an ALTER TABLE after DROP DATABASE with no separator",
			statement: "DROP DATABASE other ALTER TABLE dbo.t ADD c int",
			operation: "DROP DATABASE",
		},
		{
			name:      "sp_executesql after a CREATE TABLE with no separator",
			statement: "CREATE TABLE dbo.t (id int) EXEC sp_executesql N'DROP TABLE other.dbo.t'",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a CREATE TABLE after sp_executesql with no separator",
			statement: "EXEC sp_executesql N'DROP TABLE other.dbo.t' CREATE TABLE dbo.t (id int)",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "USE after a semicolon",
			statement: "CREATE TABLE dbo.t (id int); USE master",
			operation: "USE",
		},
		{
			name:      "GRANT after a SELECT",
			statement: "SELECT 1 AS a GRANT CONTROL TO public",
			operation: "permission mutation",
		},
		{
			name:      "SELECT INTO another database after the rows of an INSERT",
			statement: "INSERT dbo.log VALUES (1) SELECT 1 AS v INTO other.dbo.x",
			operation: `cross-database reference "other.dbo.x"`,
		},
		{
			name:      "a procedure named with no EXEC",
			statement: "dbo.p",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "sp_executesql named with no EXEC",
			statement: "sp_executesql N'DROP TABLE other.dbo.t'",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "xp_cmdshell named with no EXEC",
			statement: "xp_cmdshell 'dir'",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "xp_cmdshell named through master",
			statement: "master.dbo.xp_cmdshell 'dir'",
			operation: `cross-database reference "master.dbo.xp_cmdshell"`,
		},
		{
			name:      "sp_configure named with no EXEC",
			statement: "sp_configure 'show advanced options', 1",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a bracketed procedure name",
			statement: "[dbo].[p] 1",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a double-quoted procedure name",
			statement: `"dbo"."p"`,
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a procedure in another database",
			statement: "other..p",
			operation: `cross-database reference "other..p"`,
		},
		{
			name:      "a temporary procedure",
			statement: "#tp",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a procedure named with no EXEC, then a DROP DATABASE",
			statement: "sp_executesql N'SELECT 1' DROP DATABASE other",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a parenthesized SELECT INTO another database",
			statement: "(SELECT 1 AS v INTO other.dbo.x)",
			operation: `cross-database reference "other.dbo.x"`,
		},
		{
			name:      "a parenthesized SELECT INTO another database after an alias",
			statement: "SELECT 1 AS a (SELECT 1 AS v INTO other.dbo.x)",
			operation: `cross-database reference "other.dbo.x"`,
		},
		{
			name:      "a parenthesized query after TOP, which is one statement",
			statement: "SELECT TOP 1 (SELECT 1) AS v INTO other.dbo.x",
			operation: `cross-database reference "other.dbo.x"`,
		},
		{
			name:      "the statement IF runs",
			statement: "IF 1=1 DROP DATABASE other",
			operation: "DROP DATABASE",
		},
		{
			name:      "the database the renderer creates where it is missing",
			statement: "IF DB_ID('other') IS NULL\n    CREATE DATABASE [other];",
			operation: "CREATE DATABASE",
		},
		{
			name:      "the statement ELSE runs",
			statement: "IF 1=1 SELECT 1 ELSE GRANT SELECT ON dbo.t TO public",
			operation: "permission mutation",
		},
		{
			name:      "the statement WHILE runs",
			statement: "WHILE 1=0 EXEC dbo.p",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a statement in a block",
			statement: "BEGIN SELECT 1 USE master END",
			operation: "USE",
		},
		{
			name:      "a statement in a CATCH block",
			statement: "BEGIN TRY SELECT 1 END TRY BEGIN CATCH EXEC dbo.p END CATCH",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a statement after a label",
			statement: "retry: EXEC dbo.p",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "the EXEC whose rows an INSERT stores",
			statement: "INSERT dbo.log EXEC dbo.p",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "the table an INSERT stores the rows of an EXEC in",
			statement: "INSERT INTO other.dbo.log EXEC dbo.p",
			operation: `cross-database reference "other.dbo.log"`,
		},
		{
			name:      "USE in a constant EXEC",
			statement: "EXEC('USE master')",
			operation: "USE",
		},
		{
			name:      "a constant EXEC in a constant EXEC",
			statement: "EXEC(N'EXEC(N''DROP DATABASE other'')')",
			operation: "DROP DATABASE",
		},
		{
			name:      "a constant EXEC of a login",
			statement: "EXECUTE (N'CREATE LOGIN escaped WITH PASSWORD = ''secret''')",
			operation: "CREATE LOGIN",
		},
		{
			name:      "a varchar constant outside ASCII",
			statement: "EXEC('SELECT 1 AS a ＩＮＳＥＲＴ dbo.log VALUES (1)')",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "an EXEC of a variable",
			statement: "EXEC (@sql)",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "an EXEC of two constants",
			statement: "EXEC ('SELECT 1 ' + 'INSERT dbo.log VALUES (1)')",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			// N written apart from the literal is a name, so the literal is
			// varchar and not a Unicode constant.
			name:      "an N apart from its literal",
			statement: "EXEC(N 'USE master')",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			// The AT of a remote EXECUTE is a word, so a variable named at is
			// not one.
			name:      "an argument named at",
			statement: "EXEC dbo.p @at = 1",
			operation: "EXEC/EXECUTE sublanguage",
		},
		{
			name:      "a constant EXEC on a linked server",
			statement: "EXEC (N'SELECT 1') AT linked",
			operation: "remote EXECUTE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_RefusesTextWhoseStatementBoundariesItCannotRead holds text where
// the guard cannot tell where a statement ends, or where the server would
// read a word the lexer does not. Trusted or not, it refuses: a statement it
// cannot find is one it cannot judge.
func TestGuard_RefusesTextWhoseStatementBoundariesItCannotRead(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		reason    string
		offset    int
	}{
		{
			name:      "an END that closes no BEGIN",
			statement: "SELECT 1 END",
			reason:    "an END that closes no BEGIN",
			offset:    9,
		},
		{
			name:      "a zero width space, which the server reads as whitespace",
			statement: "SELECT 1\u200bGRANT CONTROL TO public",
			reason:    "a character that is not a T-SQL operator",
			offset:    8,
		},
		{
			name:      "a number written against a word",
			statement: "SELECT 0x1GRANT CONTROL TO public",
			reason:    "a number written against a word",
			offset:    7,
		},
		{
			name:      "a MERGE that does not end with a semicolon",
			statement: "MERGE dbo.log AS t USING (SELECT 1 AS v) AS s ON 1=0 WHEN NOT MATCHED THEN INSERT (v) VALUES (1) USE master;",
			reason:    "a MERGE that does not end with a semicolon",
			offset:    97,
		},
		{
			name:      "a procedure named after the first statement",
			statement: "SELECT 1; dbo.p",
			reason:    `a statement that starts with "dbo"`,
			offset:    10,
		},
		{
			name:      "the first piece of a block a migration split at its semicolons",
			statement: "BEGIN SELECT 1",
			reason:    "a BEGIN with no END",
			offset:    0,
		},
	}
	for _, test := range tests {
		for _, scope := range []replay.Scope{{}, {TrustedSQL: true}} {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)

				c.Assert(validate(test.statement, scope), qt.ErrorMatches, regexp.QuoteMeta(fmt.Sprintf(
					"sqlserver migration replay rejects T-SQL at offset %d because Ptah cannot read where its statements "+
						"begin and end there: %s; end each statement with a semicolon, and put the statements an IF, "+
						"a WHILE or a block runs between BEGIN and END; trusting the replayed SQL does not lift this refusal",
					test.offset, test.reason,
				)))
			})
		}
	}
}

// TestGuard_ReadsEightConstantEXECsOneInsideAnother pins how deep the guard
// reads a constant EXEC that runs another: eight levels are read, and a ninth
// is refused rather than read.
func TestGuard_ReadsEightConstantEXECsOneInsideAnother(t *testing.T) {
	nested := func(levels int) string {
		statement := "USE master"
		for range levels {
			statement = "EXEC(N'" + strings.ReplaceAll(statement, "'", "''") + "')"
		}
		return statement
	}
	t.Run("eight", func(t *testing.T) {
		c := qt.New(t)

		c.Assert(validate(nested(8), replay.Scope{}), qt.ErrorMatches, refusal("USE"))
	})
	t.Run("nine", func(t *testing.T) {
		c := qt.New(t)

		c.Assert(validate(nested(9), replay.Scope{}), qt.ErrorMatches, refusal("EXEC nested in more than 8 constant EXECs"))
	})
}

// TestGuard_TrustedSQLKeepsTheRefusalsOfEveryStatement is the control for
// trust: it lifts the EXEC a batch holds, and keeps a refusal that protects the
// run wherever the batch holds the statement.
func TestGuard_TrustedSQLKeepsTheRefusalsOfEveryStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		operation string
	}{
		{name: "USE after a statement", statement: "SELECT 1 AS a USE master", operation: "USE"},
		{name: "GRANT after a statement", statement: "CREATE TABLE dbo.t (id int) GRANT SELECT ON dbo.t TO public", operation: "permission mutation"},
		{name: "a temporary table IF creates", statement: "IF 1=1 SELECT 1 AS v INTO #x", operation: "temporary SELECT INTO target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{TrustedSQL: true}), qt.ErrorMatches, refusal(test.operation))
		})
	}
}

// TestGuard_TrustedSQLRunsTheEXECsABatchHolds is the control for the EXEC
// refusals above: a run that trusts its SQL replays the same statements.
func TestGuard_TrustedSQLRunsTheEXECsABatchHolds(t *testing.T) {
	tests := []string{
		"CREATE TABLE dbo.t (id int) EXEC sp_executesql N'DROP TABLE other.dbo.t'",
		"sp_executesql N'DROP TABLE other.dbo.t'",
		"INSERT dbo.log EXEC dbo.p",
		"IF 1=1 DROP DATABASE other",
		"CREATE OR ALTER PROCEDURE dbo.p AS BEGIN SELECT 1; SELECT 2; END",
	}
	for _, statement := range tests {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(statement, replay.Scope{TrustedSQL: true}), qt.IsNil)
		})
	}
}

// TestGuard_AcceptsTheBatchesTheRendererWrites replays, without trust,
// batches of more than one statement in the shapes the renderer writes and a
// migration author writes by hand. Each statement is judged, and none reaches
// past the dev database.
func TestGuard_AcceptsTheBatchesTheRendererWrites(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a schema created where it is missing", statement: "IF SCHEMA_ID('app') IS NULL\n    EXEC('CREATE SCHEMA [app]');"},
		{name: "a schema with a quote and a bracket created where it is missing", statement: "IF SCHEMA_ID('it''s]x') IS NULL\n    EXEC('CREATE SCHEMA [it''s]]x]');"},
		{
			name: "a sequence created where it is missing",
			statement: "IF NOT EXISTS (SELECT 1 FROM sys.sequences sq JOIN sys.schemas sc ON sc.schema_id = sq.schema_id " +
				"WHERE sc.name = 'dbo' AND sq.name = 'order_seq')\n    EXEC('CREATE SEQUENCE [dbo].[order_seq] AS bigint START WITH 1');",
		},
		{
			name: "a schema comment added where it is missing",
			statement: "IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 3 AND major_id = SCHEMA_ID('app') " +
				"AND minor_id = 0 AND name = N'MS_Description')\n    EXEC sp_addextendedproperty @name = N'MS_Description', " +
				"@value = N'Tenant data', @level0type = N'SCHEMA', @level0name = N'app';",
		},
		{name: "a table created where it is missing", statement: "IF OBJECT_ID('app.t', 'U') IS NULL\nCREATE TABLE [app].[t] (id int);"},
		{
			name:      "an index created where it is missing",
			statement: "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix' AND object_id = OBJECT_ID('dbo.t'))\nCREATE INDEX [ix] ON [dbo].[t] ([id]);",
		},
		{name: "a table dropped where it exists", statement: "DROP TABLE IF EXISTS [dbo].[t];"},
		{
			// Read as words, the comment is the AT of a remote EXECUTE.
			name: "a comment that reads at",
			statement: "EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'at', @level0type = N'SCHEMA', " +
				"@level0name = N'dbo', @level1type = N'TABLE', @level1name = N't';",
		},
		{name: "two statements with no separator", statement: "CREATE TABLE dbo.t (id int) CREATE INDEX ix ON dbo.t (id)"},
		{name: "a block", statement: "BEGIN UPDATE dbo.t SET a = 1 DELETE FROM dbo.u END"},
		{name: "IF and ELSE with blocks", statement: "IF 1=1 BEGIN UPDATE dbo.t SET a = 1 END ELSE BEGIN DELETE FROM dbo.t END"},
		{name: "TRY and CATCH", statement: "BEGIN TRY UPDATE dbo.t SET a = 1 END TRY BEGIN CATCH SELECT ERROR_NUMBER() AS n END CATCH"},
		{name: "a common table expression", statement: "WITH x AS (SELECT 1 AS a) INSERT dbo.t (a) SELECT a FROM x"},
		{
			name:      "a MERGE",
			statement: "MERGE dbo.t AS t USING (SELECT 1 AS a) AS s ON t.a = s.a WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a);",
		},
		{name: "a view created or altered", statement: "CREATE OR ALTER VIEW dbo.v AS SELECT 1 AS a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validate(test.statement, replay.Scope{}), qt.IsNil)
		})
	}
}
