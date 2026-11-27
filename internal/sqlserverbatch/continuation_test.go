package sqlserverbatch_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverbatch"
)

// TestRead_EndsAStatementRightAfterEachContinuation holds, for every rule
// that lets a statement go on past a statement word, a batch in which SQL
// Server starts a new statement right after the part the rule continues. Each
// batch was run on SQL Server 2022 and 2025, and the server ran both
// statements. A rule that kept reading here would hide the second statement
// from the guard.
//
// The operators and the reserved words no statement ends with (see the
// reader's cannotEnd) have no such batch: after each one the server either
// refuses the batch (Msg 102 or 156, and nothing runs) or reads the next
// statement word as part of the statement, as UNION SELECT and DECLARE c
// CURSOR FOR SELECT do. Nor has a MERGE: a statement word before its
// semicolon fails the batch with Msg 10713.
func TestRead_EndsAStatementRightAfterEachContinuation(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "an asterisk for every column of OUTPUT deleted",
			text: "DELETE FROM dbo.t OUTPUT deleted.* SELECT 'S2'",
			want: []string{"simple: DELETE FROM dbo.t OUTPUT deleted.*", "simple: SELECT 'S2'"},
		},
		{
			name: "an asterisk for every column of OUTPUT inserted",
			text: "UPDATE dbo.t SET a = 2 OUTPUT inserted.* SELECT 'S2'",
			want: []string{"simple: UPDATE dbo.t SET a = 2 OUTPUT inserted.*", "simple: SELECT 'S2'"},
		},
		{
			name: "a bare @, which is a variable",
			text: "DECLARE @ int = 5 SELECT @ SELECT 'S2'",
			want: []string{"simple: DECLARE @ int = 5", "simple: SELECT @", "simple: SELECT 'S2'"},
		},
		{
			name: "the statement common table expressions serve",
			text: "WITH x AS (SELECT 1 AS v) SELECT v FROM x SELECT 'S2'",
			want: []string{"simple: WITH x AS (SELECT 1 AS v) SELECT v FROM x", "simple: SELECT 'S2'"},
		},
		{
			name: "the rows of an INSERT",
			text: "INSERT INTO dbo.t VALUES (1, 1) SELECT 'S2'",
			want: []string{"simple: INSERT INTO dbo.t VALUES (1, 1)", "simple: SELECT 'S2'"},
		},
		{
			name: "the default values of an INSERT",
			text: "INSERT INTO @d DEFAULT VALUES SELECT 'S2'",
			want: []string{"simple: INSERT INTO @d DEFAULT VALUES", "simple: SELECT 'S2'"},
		},
		{
			name: "a parenthesized query after the rows of an INSERT",
			text: "INSERT INTO dbo.t VALUES (1, 1) (SELECT 'S2')",
			want: []string{"simple: INSERT INTO dbo.t VALUES (1, 1)", "simple: (SELECT 'S2')"},
		},
		{
			name: "an EXEC after the query of an INSERT",
			text: "INSERT INTO dbo.t SELECT 1, 1 EXEC sp_helpdb N'm6688'",
			want: []string{"simple: INSERT INTO dbo.t SELECT 1, 1", "simple: EXEC sp_helpdb N'm6688'"},
		},
		{
			name: "the SET clause of an UPDATE",
			text: "UPDATE dbo.t SET a = 1 SET @x = 2",
			want: []string{"simple: UPDATE dbo.t SET a = 1", "simple: SET @x = 2"},
		},
		{
			name: "UPDATE STATISTICS, which takes no SET",
			text: "UPDATE STATISTICS dbo.t SET NOCOUNT ON",
			want: []string{"simple: UPDATE STATISTICS dbo.t", "simple: SET NOCOUNT ON"},
		},
		{
			name: "the action of an ALTER TABLE",
			text: "ALTER TABLE dbo.c ADD c2 int SELECT 'S2'",
			want: []string{"simple: ALTER TABLE dbo.c ADD c2 int", "simple: SELECT 'S2'"},
		},
		{
			name: "the action of an ALTER COLUMN",
			text: "ALTER TABLE dbo.c ALTER COLUMN c2 bigint DROP TABLE IF EXISTS dbo.zz9",
			want: []string{"simple: ALTER TABLE dbo.c ALTER COLUMN c2 bigint", "simple: DROP TABLE IF EXISTS dbo.zz9"},
		},
		{
			name: "IF EXISTS of a DROP",
			text: "DROP TABLE IF EXISTS dbo.zz9 IF EXISTS (SELECT 1) SELECT 'S2'",
			want: []string{"simple: DROP TABLE IF EXISTS dbo.zz9", "if: IF EXISTS (SELECT 1) SELECT 'S2'", "  simple: SELECT 'S2'"},
		},
		{
			name: "ON DELETE of a foreign key",
			text: "ALTER TABLE dbo.c ADD CONSTRAINT f1 FOREIGN KEY (a) REFERENCES dbo.r (id) ON DELETE CASCADE DELETE FROM dbo.c",
			want: []string{
				"simple: ALTER TABLE dbo.c ADD CONSTRAINT f1 FOREIGN KEY (a) REFERENCES dbo.r (id) ON DELETE CASCADE",
				"simple: DELETE FROM dbo.c",
			},
		},
		{
			name: "ON DELETE SET NULL of a foreign key",
			text: "ALTER TABLE dbo.c ADD CONSTRAINT f2 FOREIGN KEY (a) REFERENCES dbo.r (id) ON DELETE SET NULL SET NOCOUNT ON",
			want: []string{
				"simple: ALTER TABLE dbo.c ADD CONSTRAINT f2 FOREIGN KEY (a) REFERENCES dbo.r (id) ON DELETE SET NULL",
				"simple: SET NOCOUNT ON",
			},
		},
		{
			name: "OFFSET of an ORDER BY",
			text: "SELECT a FROM dbo.t ORDER BY a OFFSET 0 ROWS SELECT 'S2'",
			want: []string{"simple: SELECT a FROM dbo.t ORDER BY a OFFSET 0 ROWS", "simple: SELECT 'S2'"},
		},
		{
			name: "FETCH of an ORDER BY",
			text: "SELECT a FROM dbo.t ORDER BY a OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY SELECT 'S2'",
			want: []string{"simple: SELECT a FROM dbo.t ORDER BY a OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY", "simple: SELECT 'S2'"},
		},
		{
			name: "the first column after TOP",
			text: "SELECT TOP 1 (SELECT 'S1') AS v SELECT 'S2'",
			want: []string{"simple: SELECT TOP 1 (SELECT 'S1') AS v", "simple: SELECT 'S2'"},
		},
		{
			name: "a column alias named offset",
			text: "SELECT 1 AS offset (SELECT 'S2')",
			want: []string{"simple: SELECT 1 AS offset", "simple: (SELECT 'S2')"},
		},
		{
			name: "a view that CREATE SCHEMA creates",
			text: "CREATE SCHEMA s9 CREATE VIEW v9 AS SELECT 1 AS a GRANT SELECT ON v9 TO public",
			want: []string{"simple: CREATE SCHEMA s9", "simple: CREATE VIEW v9 AS SELECT 1 AS a", "simple: GRANT SELECT ON v9 TO public"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(render(statements, ""), qt.DeepEquals, test.want)
			assertOffsets(c, test.text, statements)
		})
	}
}

// TestRead_ReadsThePartsAStatementGoesOnWith holds batches whose statement goes
// on past a word that looks like an end: an asterisk that multiplies, a
// variable whose name holds a statement word, the END of a temporal or a
// ledger column, and a view outside CREATE SCHEMA, which runs to the end of
// the batch. Each was run on SQL Server 2025.
func TestRead_ReadsThePartsAStatementGoesOnWith(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "an asterisk between a number and a parenthesized query",
			text: "SELECT 2 * (SELECT 3) AS v",
			want: []string{"simple: SELECT 2 * (SELECT 3) AS v"},
		},
		{
			name: "an asterisk between a name and a parenthesized query",
			text: "SELECT a * (SELECT 3) FROM (SELECT 2 AS a) AS x",
			want: []string{"simple: SELECT a * (SELECT 3) FROM (SELECT 2 AS a) AS x"},
		},
		{
			name: "an asterisk between a literal and a parenthesized query",
			text: "SELECT N'2' * (SELECT 3) AS v",
			want: []string{"simple: SELECT N'2' * (SELECT 3) AS v"},
		},
		{
			name: "an asterisk between a closing parenthesis and a parenthesized query",
			text: "SELECT (1 + 1) * (SELECT 3) AS v",
			want: []string{"simple: SELECT (1 + 1) * (SELECT 3) AS v"},
		},
		{
			name: "variables whose names hold digits, signs and statement words",
			text: "SELECT @1, @1e, @a$b, @a#b, @_, @@ROWCOUNT, @select, @é DROP TABLE dbo.t",
			want: []string{"simple: SELECT @1, @1e, @a$b, @a#b, @_, @@ROWCOUNT, @select, @é", "simple: DROP TABLE dbo.t"},
		},
		{
			// The lexer reads the # alone as an operator, which the server
			// reads as part of the variable @#.
			name: "a variable named with a #",
			text: "DECLARE @# int = 1 SELECT @# SELECT 'S2'",
			want: []string{"simple: DECLARE @# int = 1", "simple: SELECT @#", "simple: SELECT 'S2'"},
		},
		{
			name: "a variable with a string alias",
			text: "SELECT @N'x' DROP TABLE dbo.t",
			want: []string{"simple: SELECT @N'x'", "simple: DROP TABLE dbo.t"},
		},
		{
			name: "a bare @ assigned to itself before USE",
			text: "DECLARE @ int = 1 SET @ = @ USE master",
			want: []string{"simple: DECLARE @ int = 1", "simple: SET @ = @", "simple: USE master"},
		},
		{
			name: "a temporal table",
			text: "CREATE TABLE dbo.tt (id int PRIMARY KEY, vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, " +
				"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt)) " +
				"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tt_history)) SELECT 'S2'",
			want: []string{
				"simple: CREATE TABLE dbo.tt (id int PRIMARY KEY, vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL, " +
					"vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt)) " +
					"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tt_history))",
				"simple: SELECT 'S2'",
			},
		},
		{
			name: "temporal columns added to a table",
			text: "ALTER TABLE dbo.ta ADD vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL " +
				"CONSTRAINT df_ta_vf DEFAULT SYSUTCDATETIME(), vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL " +
				"CONSTRAINT df_ta_vt DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'), PERIOD FOR SYSTEM_TIME (vf, vt) SELECT 'S2'",
			want: []string{
				"simple: ALTER TABLE dbo.ta ADD vf datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL " +
					"CONSTRAINT df_ta_vf DEFAULT SYSUTCDATETIME(), vt datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL " +
					"CONSTRAINT df_ta_vt DEFAULT CONVERT(datetime2, '9999-12-31 23:59:59.9999999'), PERIOD FOR SYSTEM_TIME (vf, vt)",
				"simple: SELECT 'S2'",
			},
		},
		{
			name: "a ROW END column without HIDDEN",
			text: "CREATE TABLE dbo.tt3 (id int PRIMARY KEY, vf datetime2 GENERATED ALWAYS AS ROW START NOT NULL, " +
				"vt datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt))",
			want: []string{
				"simple: CREATE TABLE dbo.tt3 (id int PRIMARY KEY, vf datetime2 GENERATED ALWAYS AS ROW START NOT NULL, " +
					"vt datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (vf, vt))",
			},
		},
		{
			name: "an updatable ledger table",
			text: "CREATE TABLE dbo.tl (id int, ts bigint GENERATED ALWAYS AS TRANSACTION_ID START HIDDEN NOT NULL, " +
				"te bigint GENERATED ALWAYS AS TRANSACTION_ID END HIDDEN NULL, ss bigint GENERATED ALWAYS AS SEQUENCE_NUMBER START HIDDEN NOT NULL, " +
				"se bigint GENERATED ALWAYS AS SEQUENCE_NUMBER END HIDDEN NULL) " +
				"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tl_history), LEDGER = ON)",
			want: []string{
				"simple: CREATE TABLE dbo.tl (id int, ts bigint GENERATED ALWAYS AS TRANSACTION_ID START HIDDEN NOT NULL, " +
					"te bigint GENERATED ALWAYS AS TRANSACTION_ID END HIDDEN NULL, ss bigint GENERATED ALWAYS AS SEQUENCE_NUMBER START HIDDEN NOT NULL, " +
					"se bigint GENERATED ALWAYS AS SEQUENCE_NUMBER END HIDDEN NULL) " +
					"WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.tl_history), LEDGER = ON)",
			},
		},
		{
			name: "a column aliased row in a block",
			text: "BEGIN SELECT always AS row END",
			want: []string{"block: BEGIN SELECT always AS row END", "  simple: SELECT always AS row"},
		},
		{
			name: "a view outside CREATE SCHEMA",
			text: "CREATE VIEW v9 AS SELECT 1 AS a GRANT SELECT ON v9 TO public",
			want: []string{"module: CREATE VIEW v9 AS SELECT 1 AS a GRANT SELECT ON v9 TO public"},
		},
		{
			name: "a name that leaves the server out",
			text: "ALTER TABLE .db.dbo.t DROP COLUMN c",
			want: []string{"simple: ALTER TABLE .db.dbo.t DROP COLUMN c"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(render(statements, ""), qt.DeepEquals, test.want)
			assertOffsets(c, test.text, statements)
		})
	}
}

// TestRead_EndsAStatementAfterAnAsteriskForEveryColumnTheServerRefuses holds
// an asterisk for every column where the server refuses the batch: SELECT *
// with no FROM (Msg 263) and OUTPUT * (Msg 102) on SQL Server 2025. The
// reader ends the statement after the asterisk, so the guard judges what
// follows whatever the server would make of it.
func TestRead_EndsAStatementAfterAnAsteriskForEveryColumnTheServerRefuses(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "SELECT *",
			text: "SELECT * DROP DATABASE other",
			want: []string{"simple: SELECT *", "simple: DROP DATABASE other"},
		},
		{
			name: "OUTPUT *",
			text: "DELETE FROM dbo.t OUTPUT * DROP DATABASE other",
			want: []string{"simple: DELETE FROM dbo.t OUTPUT *", "simple: DROP DATABASE other"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(render(statements, ""), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_RefusesAVariableTheLexerReadsDifferently covers a variable whose
// end falls inside a token the lexer reads: in @1.5 the lexer reads the number
// 1.5, and the server reads the variable @1 and fails the batch at .5.
func TestRead_RefusesAVariableTheLexerReadsDifferently(t *testing.T) {
	c := qt.New(t)
	statements, err := sqlserverbatch.Read("SELECT @1.5 SELECT 'S2'")
	c.Assert(err, qt.ErrorIs, sqlserverbatch.ErrUnreadable)
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
		"cannot find the T-SQL statement boundaries: a variable name the lexer reads differently at offset 7"))
	c.Assert(statements, qt.IsNil)
}
