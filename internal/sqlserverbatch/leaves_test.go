package sqlserverbatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverbatch"
)

// TestLeaves walks each kind of statement to the statements the
// server runs from it. A block, an IF and a WHILE stand for what they run, CREATE
// SCHEMA stands beside its elements, and a routine is one leaf whatever its
// body holds, since the server stores the body rather than runs it.
func TestLeaves(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "statements with no semicolon between them", text: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x", want: []string{"CREATE TABLE dbo.a (id int)", "TRUNCATE TABLE dbo.x"}},
		{name: "a nested block", text: "BEGIN SELECT 1; BEGIN COMMIT; END; END;", want: []string{"SELECT 1", "COMMIT"}},
		{name: "a TRY block and its CATCH block", text: "BEGIN TRY SELECT 1; END TRY BEGIN CATCH ROLLBACK; END CATCH", want: []string{"SELECT 1", "ROLLBACK"}},
		{name: "an IF and its ELSE", text: "IF @@TRANCOUNT > 0 COMMIT; ELSE SELECT 2;", want: []string{"COMMIT", "SELECT 2"}},
		{name: "a WHILE", text: "WHILE 1 = 0 DELETE FROM dbo.x;", want: []string{"DELETE FROM dbo.x"}},
		{name: "CREATE SCHEMA and its elements", text: "CREATE SCHEMA s CREATE TABLE t (id int) GRANT SELECT ON t TO public;", want: []string{"CREATE SCHEMA s", "CREATE TABLE t (id int)", "GRANT SELECT ON t TO public"}},
		{name: "a procedure", text: "CREATE PROCEDURE dbo.p AS BEGIN COMMIT; END", want: []string{"CREATE PROCEDURE dbo.p AS BEGIN COMMIT; END"}},
		{name: "a MERGE", text: "MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN DELETE;", want: []string{"MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN DELETE"}},
		{name: "an EXEC of a literal", text: "EXEC(N'INSERT INTO dbo.t VALUES (1); COMMIT')", want: []string{"INSERT INTO dbo.t VALUES (1)", "COMMIT"}},
		{name: "sp_executesql in a block", text: "BEGIN EXEC sp_executesql N'ROLLBACK', N'@p int', @p = 1; END", want: []string{"ROLLBACK"}},
		{name: "an EXEC of a literal the reader cannot read", text: "EXEC('BEGIN COMMIT')", want: []string{"BEGIN COMMIT"}},
		{name: "an EXEC of a variable", text: "EXEC(@sql)", want: []string{"EXEC(@sql)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(sqlserverbatch.Leaves(statements), qt.DeepEquals, test.want)
		})
	}
}

// TestRunsSeveral_HappyPath lists statements that can commit part of their
// work and then fail when no transaction wraps them.
func TestRunsSeveral_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
	}{
		{name: "a block of two INSERTs", text: "BEGIN INSERT INTO dbo.log VALUES (N'a'); INSERT INTO dbo.missing VALUES (1); END;"},
		{name: "two INSERTs with no semicolon between them", text: "INSERT INTO dbo.log VALUES (N'a') INSERT INTO dbo.missing VALUES (1)"},
		{name: "a WHILE of one INSERT", text: "WHILE @i < 3 BEGIN INSERT INTO dbo.log VALUES (@i); SET @i += 1; END"},
		{name: "a procedure an EXEC names", text: "EXEC dbo.seed"},
		{name: "a procedure named with no EXEC", text: "dbo.seed"},
		{name: "an EXEC of a variable", text: "EXEC(@sql)"},
		{name: "an EXEC of a literal that holds two statements", text: "EXEC(N'INSERT INTO dbo.log VALUES (1); INSERT INTO dbo.log VALUES (2)')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(sqlserverbatch.RunsSeveral(statements), qt.IsTrue)
		})
	}
}

// TestRunsSeveral_FailurePath lists statements that commit all of their work
// or none of it: one statement, whatever a block, an IF or an EXEC of a
// literal wraps around it, beside statements whose effect ends with the batch.
func TestRunsSeveral_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
	}{
		{name: "one INSERT", text: "INSERT INTO dbo.log VALUES (N'a');"},
		{name: "an IF of one INSERT", text: "IF OBJECT_ID(N'dbo.log') IS NOT NULL INSERT INTO dbo.log VALUES (N'a');"},
		{name: "a block of one INSERT and a variable", text: "BEGIN DECLARE @m nvarchar(10) = N'a'; INSERT INTO dbo.log VALUES (@m); PRINT @m; END;"},
		{name: "an EXEC of a literal that holds one statement", text: "EXEC(N'CREATE SCHEMA [s]')"},
		{name: "a procedure the text defines", text: "CREATE PROCEDURE dbo.p AS BEGIN INSERT INTO dbo.log VALUES (1); INSERT INTO dbo.log VALUES (2); END"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(sqlserverbatch.RunsSeveral(statements), qt.IsFalse)
		})
	}
}

// TestChangesNothing_HappyPath lists statements whose effect ends with their
// batch.
func TestChangesNothing_HappyPath(t *testing.T) {
	for _, statement := range []string{"DECLARE @m nvarchar(10) = N'a'", "SET @m = N'b'", "set @i += 1", "PRINT N'done'"} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlserverbatch.ChangesNothing(statement), qt.IsTrue)
		})
	}
}

// TestChangesNothing_FailurePath lists statements that leave something
// behind: a session option, a cursor the session keeps, and a write.
func TestChangesNothing_FailurePath(t *testing.T) {
	for _, statement := range []string{"SET NOCOUNT ON", "DECLARE c CURSOR GLOBAL FOR SELECT 1", "INSERT INTO dbo.log VALUES (1)", "SET @@x = 1", ""} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlserverbatch.ChangesNothing(statement), qt.IsFalse)
		})
	}
}
