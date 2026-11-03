package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasreport"
)

// batchSeparatorPlan is a statement-bodied trigger followed by a table, as
// `migrate diff` hands them to the report: the trigger keeps the semicolon
// that ends its body.
func batchSeparatorPlan() []atlasreport.SchemaChange {
	return []atlasreport.SchemaChange{
		{Cmd: "CREATE TRIGGER [trg] ON [t] AFTER INSERT AS SET NOCOUNT ON;", Comment: `create "trg" trigger`},
		{Cmd: "CREATE TABLE [u] (\n  [id] int\n)", Comment: `create "u" table`},
	}
}

// TestWriteMigrateDiff_BatchSeparators_HappyPath pins where the `sql` helper
// ends a SQL Server batch. A trigger's body runs to the end of its batch, so
// in a native migration file every statement ends its batch with GO, and the
// file splits back into the statements it was written from. A file of a
// layout another tool reads takes no GO, which such a tool may send to the
// server as written.
func TestWriteMigrateDiff_BatchSeparators_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		foreign   bool
		want      string
		wantSplit int
	}{
		{
			name: "a native file",
			want: "-- Create \"trg\" trigger\nCREATE TRIGGER [trg] ON [t] AFTER INSERT AS SET NOCOUNT ON;\nGO\n" +
				"-- Create \"u\" table\nCREATE TABLE [u] (\n  [id] int\n);\nGO\n",
			wantSplit: 2,
		},
		{
			name:    "a file of a layout another tool reads",
			foreign: true,
			want: "-- create \"trg\" trigger\nCREATE TRIGGER [trg] ON [t] AFTER INSERT AS SET NOCOUNT ON;\n" +
				"-- create \"u\" table\nCREATE TABLE [u] (\n  [id] int\n);\n",
			wantSplit: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{
				Dialect:           platform.SQLServer,
				Changes:           batchSeparatorPlan(),
				CommentsAsWritten: test.foreign,
				ForeignLayout:     test.foreign,
			})
			var out bytes.Buffer

			err := atlasreport.WriteMigrateDiff(&out, `{{ sql . "  " }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(out.String(), platform.SQLServer), qt.HasLen, test.wantSplit)
		})
	}
}
