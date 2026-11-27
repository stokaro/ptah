package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

const (
	dropTableReason          = "DROP TABLE removes the table and all rows"
	truncateReason           = "TRUNCATE removes all rows from a table"
	deleteWithoutWhereReason = "DELETE without a WHERE clause removes every row it reaches, every row of its table in the plain form"
	mergeDeleteReason        = "MERGE with a THEN DELETE arm removes the target rows the arm matches, " +
		"every row the source does not hold under WHEN NOT MATCHED BY SOURCE"
)

// TestAssessSQL_SQLServerJudgesEveryStatementTheServerRuns rates T-SQL by each
// statement the server runs from it, not by the words it starts with. T-SQL
// needs no semicolon between two statements, so the first three rows are two
// statements each, and the first statement of each reads as safe or as a lesser
// risk: judged by its first words alone, the TRUNCATE, the DELETE and the DROP
// TABLE after it go unseen. A block, a TRY and CATCH, an IF and a WHILE run
// the statements written in them, and CREATE SCHEMA runs its elements.
func TestAssessSQL_SQLServerJudgesEveryStatementTheServerRuns(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		severity  safety.Severity
		reason    string
	}{
		{name: "a TRUNCATE after a CREATE TABLE", statement: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x", severity: safety.Destructive, reason: truncateReason},
		{name: "a DELETE after a SELECT", statement: "SELECT 1 AS a DELETE FROM dbo.x", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{name: "a DROP TABLE after an ALTER TABLE that adds a column", statement: "ALTER TABLE dbo.t ADD c int DROP TABLE dbo.x", severity: safety.Destructive, reason: dropTableReason},
		{name: "a block", statement: "BEGIN SELECT 1; DROP TABLE dbo.x; END", severity: safety.Destructive, reason: dropTableReason},
		{name: "a nested block", statement: "BEGIN SELECT 1; BEGIN TRUNCATE TABLE dbo.x; END; END", severity: safety.Destructive, reason: truncateReason},
		{name: "a CATCH block", statement: "BEGIN TRY SELECT 1; END TRY BEGIN CATCH DROP TABLE dbo.x; END CATCH", severity: safety.Destructive, reason: dropTableReason},
		{name: "an IF", statement: "IF OBJECT_ID(N'dbo.x') IS NOT NULL TRUNCATE TABLE dbo.x", severity: safety.Destructive, reason: truncateReason},
		{name: "an ELSE block", statement: "IF 1 = 0 BEGIN SELECT 1; END ELSE BEGIN DROP TABLE dbo.x; END", severity: safety.Destructive, reason: dropTableReason},
		{name: "a WHILE", statement: "WHILE 1 = 0 BEGIN DELETE FROM dbo.x; END", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{name: "a labeled statement", statement: "retry: TRUNCATE TABLE dbo.x", severity: safety.Destructive, reason: truncateReason},
		{name: "an element of CREATE SCHEMA", statement: "CREATE SCHEMA s CREATE TABLE t (id int) REVOKE SELECT ON t FROM public", severity: safety.Warning, reason: revokeReason},
		{name: "a statement after the body of a procedure", statement: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END; DROP TABLE dbo.x", severity: safety.Destructive, reason: dropTableReason},
		{name: "a statement after GO", statement: "SELECT 1\nGO\nTRUNCATE TABLE dbo.x", severity: safety.Destructive, reason: truncateReason},
		{name: "statements that change nothing held", statement: "CREATE TABLE dbo.a (id int) IF 1 = 1 BEGIN INSERT INTO dbo.a VALUES (1); END", severity: safety.Safe, reason: safeReason},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), "sqlserver", []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, test.severity)
			c.Assert(got[0].Reason, qt.Equals, test.reason)
			c.Assert(got[0].Statement, qt.Equals, test.statement)
		})
	}
}

// TestAssessSQL_SQLServerRatesTextItCannotReadDestructive: where the reader
// cannot find where the statements of the text begin and end, what the server
// runs from it is not established, so nothing says it is safe.
func TestAssessSQL_SQLServerRatesTextItCannotReadDestructive(t *testing.T) {
	c := qt.New(t)

	got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), "sqlserver", []string{"SELECT 1 END"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 1)
	c.Assert(got[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(got[0].Reason, qt.Matches, `Ptah cannot find where the T-SQL statements in it begin and end \(.+\), so what it runs is not established`)
}

// TestAssessSQL_ReadsTwoStatementsAsTwoOnlyInTSQL is the control for the
// reading above: on a dialect whose text is not T-SQL, the text is one
// statement and is read by its first words, so the same text rates as its
// first statement does.
func TestAssessSQL_ReadsTwoStatementsAsTwoOnlyInTSQL(t *testing.T) {
	c := qt.New(t)

	got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), "postgres", []string{"CREATE TABLE a (id int) TRUNCATE TABLE x"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 1)
	c.Assert(got[0].Severity, qt.Equals, safety.Safe)
}

// TestAssessSQL_DeleteWithoutWhereIsDestructive: a DELETE that no WHERE clause
// limits removes every row it reaches, as TRUNCATE does, on every dialect. A
// DELETE with a WHERE clause is the shape the data stage writes for a row the
// declaration no longer holds, and the stage rates it; its words alone say
// nothing.
func TestAssessSQL_DeleteWithoutWhereIsDestructive(t *testing.T) {
	for _, test := range []struct {
		target    string
		statement string
		severity  safety.Severity
		reason    string
	}{
		{target: "postgres", statement: "DELETE FROM users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "mysql", statement: "DELETE FROM `users`;", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "sqlite", statement: "delete from users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "sqlserver", statement: "DELETE TOP (10) FROM dbo.users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "postgres", statement: "DELETE FROM users WHERE id = 1", severity: safety.Safe, reason: safeReason},
		{target: "sqlserver", statement: "DELETE FROM dbo.users WHERE id = 1", severity: safety.Safe, reason: safeReason},
		{target: "postgres", statement: "WITH d AS (SELECT id FROM users) DELETE FROM users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "sqlserver", statement: "WITH d AS (SELECT id FROM dbo.users) DELETE FROM dbo.users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "mysql", statement: "WITH d (id) AS (SELECT id FROM users WHERE id > 1), e AS (SELECT 1) DELETE FROM users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "postgres", statement: "WITH d AS (SELECT id FROM users) DELETE FROM users WHERE id IN (SELECT id FROM d)", severity: safety.Safe, reason: safeReason},
		{target: "postgres", statement: "DELETE FROM users -- WHERE id = 1", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "sqlite", statement: "DELETE FROM users /* WHERE id = 1 */", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "postgres", statement: "-- note\nDELETE FROM users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "mysql", statement: "/* note */ DELETE FROM users", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "sqlserver", statement: "DELETE FROM dbo.users -- WHERE id = 1", severity: safety.Destructive, reason: deleteWithoutWhereReason},
		{target: "postgres", statement: "-- note\nDROP TABLE users", severity: safety.Destructive, reason: dropTableReason},
		{target: "postgres", statement: "DROP /* old */ TABLE users", severity: safety.Destructive, reason: dropTableReason},
		{target: "sqlserver", statement: "SELECT 1 DROP /* old */ TABLE dbo.users", severity: safety.Destructive, reason: dropTableReason},
		{target: "postgres", statement: "DELETE FROM users WHERE note = '-- x'", severity: safety.Safe, reason: safeReason},
	} {
		t.Run(test.target+" "+test.statement, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), test.target, []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, test.severity)
			c.Assert(got[0].Reason, qt.Equals, test.reason)
		})
	}
}

// TestAssessSQL_MergeThatDeletesIsDestructive: a MERGE with a THEN DELETE arm
// removes the target rows the arm matches, and under WHEN NOT MATCHED BY
// SOURCE every row the source does not hold, so its words rate it as a DELETE
// without a WHERE clause is rated. A MERGE that only inserts and updates
// removes nothing.
func TestAssessSQL_MergeThatDeletesIsDestructive(t *testing.T) {
	for _, test := range []struct {
		target    string
		statement string
		severity  safety.Severity
		reason    string
	}{
		{
			target:    "sqlserver",
			statement: "MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE;",
			severity:  safety.Destructive, reason: mergeDeleteReason,
		},
		{
			target:    "sqlserver",
			statement: "IF 1 = 1 MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN DELETE;",
			severity:  safety.Destructive, reason: mergeDeleteReason,
		},
		{
			target:    "postgres",
			statement: "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
			severity:  safety.Destructive, reason: mergeDeleteReason,
		},
		{
			target:    "postgres",
			statement: "WITH v AS (SELECT 1 AS id) MERGE INTO t USING v ON t.id = v.id WHEN MATCHED THEN DELETE",
			severity:  safety.Destructive, reason: mergeDeleteReason,
		},
		{
			target:    "sqlserver",
			statement: "MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);",
			severity:  safety.Safe, reason: safeReason,
		},
	} {
		t.Run(test.target+" "+test.statement, func(t *testing.T) {
			c := qt.New(t)

			got, err := safety.AssessSQL(c.Context(), must.Must(builtin.New()), test.target, []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Severity, qt.Equals, test.severity)
			c.Assert(got[0].Reason, qt.Equals, test.reason)
		})
	}
}
