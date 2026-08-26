package mysql

// White-box testing required: the EXTRA reader is package-local and no exported
// API reports what it made of one column's metadata string.

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
)

// TestMySQLUpdateExpression_ReadsTheClauseOutOfExtra is the defect.
//
// `information_schema.COLUMNS.EXTRA` reports the ON UPDATE clause plainly and
// the reader looked past it, so an inspected schema rendered the column without
// it -- and replaying that document built a column that silently stopped
// maintaining itself. Every row is a value measured on MySQL 8.4
// (stokaro/ptah#1215).
func TestMySQLUpdateExpression_ReadsTheClauseOutOfExtra(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		want  string
	}{
		{
			name:  "DATETIME ON UPDATE CURRENT_TIMESTAMP",
			extra: "on update CURRENT_TIMESTAMP",
			want:  "CURRENT_TIMESTAMP",
		},
		{
			// EXTRA is a list of facts, and a column with a default carries
			// both. The clause is the tail, not the whole string.
			name:  "a column that also has a generated default",
			extra: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
			want:  "CURRENT_TIMESTAMP",
		},
		{
			// The parameterized form is written back by the engine, and it is
			// returned as the server spelled it: rewriting it would be Ptah
			// deciding what the column says.
			name:  "the parameterized form",
			extra: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
			want:  "CURRENT_TIMESTAMP(3)",
		},
		{
			name:  "an upper-case keyword",
			extra: "ON UPDATE CURRENT_TIMESTAMP",
			want:  "CURRENT_TIMESTAMP",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlUpdateExpression(test.extra), qt.Equals, test.want)
		})
	}
}

// TestMySQLUpdateExpression_IsEmptyForEveryOtherExtra is the control.
//
// EXTRA carries auto_increment and the generated kind too, and a reader that
// answered a clause for those would put `ON UPDATE auto_increment` into a
// rendered column.
func TestMySQLUpdateExpression_IsEmptyForEveryOtherExtra(t *testing.T) {
	tests := []struct {
		name  string
		extra string
	}{
		{name: "nothing at all", extra: ""},
		{name: "auto increment", extra: "auto_increment"},
		{name: "a stored generated column", extra: "STORED GENERATED"},
		{name: "a virtual generated column", extra: "VIRTUAL GENERATED"},
		{name: "a generated default alone", extra: "DEFAULT_GENERATED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysqlUpdateExpression(test.extra), qt.Equals, "")
		})
	}
}

// TestApplyMySQLColumnMetadata_PutsTheClauseOnTheColumn is the wiring.
//
// Reading the clause out of a string proves nothing about the column reaching a
// renderer with it. This calls the function the row scan calls, with the EXTRA
// a server sends, and asserts the column carries it -- and that the other facts
// on the same string still land where they did.
func TestApplyMySQLColumnMetadata_PutsTheClauseOnTheColumn(t *testing.T) {
	tests := []struct {
		name              string
		extra             string
		wantExpression    string
		wantAutoIncrement bool
		wantGeneratedKind string
	}{
		{
			name:           "a self-maintaining column",
			extra:          "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
			wantExpression: "CURRENT_TIMESTAMP",
		},
		{
			name:              "an auto-increment column",
			extra:             "auto_increment",
			wantAutoIncrement: true,
		},
		{
			name:              "a stored generated column",
			extra:             "STORED GENERATED",
			wantGeneratedKind: "STORED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := catalog.Column{Name: "updated_at"}

			applyMySQLColumnMetadata(&column,
				sql.NullString{}, sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{},
				sql.NullString{}, sql.NullString{},
				sql.NullString{String: test.extra, Valid: true},
				sql.NullString{})

			c.Assert(column.UpdateExpression, qt.Equals, test.wantExpression)
			c.Assert(column.IsAutoIncrement, qt.Equals, test.wantAutoIncrement)
			c.Assert(column.GeneratedKind, qt.Equals, test.wantGeneratedKind)
		})
	}
}
