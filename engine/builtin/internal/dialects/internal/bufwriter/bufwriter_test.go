package bufwriter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/engine/builtin/internal/dialects/internal/bufwriter"
)

// TestJoinAlterTable_HappyPath is the statements one table's operations render.
func TestJoinAlterTable_HappyPath(t *testing.T) {
	const prefix = `ALTER TABLE "t" `
	tests := []struct {
		name    string
		dialect string
		text    string
		want    string
	}{
		{name: "one statement", dialect: platform.Postgres, text: "ALTER TABLE \"t\" ADD c int;\n", want: "ALTER TABLE \"t\" ADD c int;\n"},
		{name: "two statements", dialect: platform.Postgres, text: "ALTER TABLE \"t\" ADD c int;\nALTER TABLE \"t\" DROP d;\n", want: "ALTER TABLE \"t\" ADD c int, DROP d;\n"},
		{
			name: "a clause on two lines", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" DROP k;\nALTER TABLE \"t\" ADD\nCONSTRAINT k FOREIGN KEY (a) REFERENCES p (a);\n",
			want: "ALTER TABLE \"t\" DROP k, ADD\nCONSTRAINT k FOREIGN KEY (a) REFERENCES p (a);\n",
		},
		{
			name: "a blank line behind the statement stays", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD c int;\nALTER TABLE \"t\" DROP d;\n\n",
			want: "ALTER TABLE \"t\" ADD c int, DROP d;\n\n",
		},
		{
			name: "a comment goes ahead of the statement", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD c int;\n-- a report\nALTER TABLE \"t\" DROP d;\n",
			want: "-- a report\nALTER TABLE \"t\" ADD c int, DROP d;\n",
		},
		{
			name: "a literal holding what reads as a statement", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD c text DEFAULT 'a;\nALTER TABLE \"t\" b';\nALTER TABLE \"t\" DROP d;\n",
			want: "ALTER TABLE \"t\" ADD c text DEFAULT 'a;\nALTER TABLE \"t\" b', DROP d;\n",
		},
		{
			name: "a comment holding a quote", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD c int;\n-- it's a report\nALTER TABLE \"t\" ADD e text DEFAULT 'x';\nALTER TABLE \"t\" DROP d;\n",
			want: "-- it's a report\nALTER TABLE \"t\" ADD c int, ADD e text DEFAULT 'x', DROP d;\n",
		},
		{
			name: "an escaped quote on MySQL, behind which a semicolon is still in a literal", dialect: platform.MySQL,
			text: "ALTER TABLE \"t\" ADD a text DEFAULT 'it\\'s';\nALTER TABLE \"t\" ADD c text DEFAULT 'y;z';\n",
			want: "ALTER TABLE \"t\" ADD a text DEFAULT 'it\\'s', ADD c text DEFAULT 'y;z';\n",
		},
		{
			name: "an escape string on PostgreSQL", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD CHECK (a <> E'it\\'s');\nALTER TABLE \"t\" ADD c text DEFAULT 'y;z';\n",
			want: "ALTER TABLE \"t\" ADD CHECK (a <> E'it\\'s'), ADD c text DEFAULT 'y;z';\n",
		},
		{
			name: "a dollar quote on PostgreSQL", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD CHECK (a <> $$it's;$$);\nALTER TABLE \"t\" ADD c text DEFAULT 'y;z';\n",
			want: "ALTER TABLE \"t\" ADD CHECK (a <> $$it's;$$), ADD c text DEFAULT 'y;z';\n",
		},
		{
			name: "a backslash on PostgreSQL, where it escapes nothing", dialect: platform.Postgres,
			text: "ALTER TABLE \"t\" ADD a text DEFAULT 'x\\';\nALTER TABLE \"t\" DROP d;\n",
			want: "ALTER TABLE \"t\" ADD a text DEFAULT 'x\\', DROP d;\n",
		},
		{name: "nothing", dialect: platform.Postgres, text: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := bufwriter.JoinAlterTable(test.text, prefix, test.dialect)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestJoinAlterTable_FailurePath is a statement that cannot be a clause of the ALTER
// TABLE, wherever it stands. The caller has to write the operations in another order, so
// no part of the text is joined.
func TestJoinAlterTable_FailurePath(t *testing.T) {
	const prefix = `ALTER TABLE "t" `
	tests := []struct {
		name string
		text string
	}{
		{name: "a comment on the table between two", text: "ALTER TABLE \"t\" ADD c int;\nCOMMENT ON TABLE \"t\" IS 'x';\nALTER TABLE \"t\" DROP d;\n"},
		{name: "a backfill", text: "UPDATE \"t\" SET a = 1 WHERE a IS NULL;\nALTER TABLE \"t\" ALTER a SET NOT NULL;\n"},
		{name: "a statement on another table", text: "ALTER TABLE \"u\" ADD c int;\nALTER TABLE \"t\" DROP d;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := bufwriter.JoinAlterTable(test.text, prefix, platform.Postgres)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
