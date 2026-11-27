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
