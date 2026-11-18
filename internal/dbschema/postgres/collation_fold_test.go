package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestWriterHintsAtAFoldedCollationName pins the line a refusal of a collation
// whose name holds no upper-case letter carries. PostgreSQL 18.6 answers
// `COLLATE "c"` with the refusal below, and a SQL schema file writes that for
// an unquoted `COLLATE C`.
func TestWriterHintsAtAFoldedCollationName(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "C",
			message: `collation "c" for encoding "UTF8" does not exist`,
			want:    `(?s).*\nhint: an unquoted collation name folds to lower case, so COLLATE C names "c"; write COLLATE "C"`,
		},
		{
			name:    "POSIX",
			message: `collation "posix" for encoding "UTF8" does not exist`,
			want:    `(?s).*\nhint: an unquoted collation name folds to lower case, so COLLATE POSIX names "posix"; write COLLATE "POSIX"`,
		},
		{
			name:    "another name",
			message: `collation "en_us.utf8" for encoding "UTF8" does not exist`,
			want: `(?s).*\nhint: an unquoted collation name folds to lower case, so "en_us.utf8" may name a collation spelled ` +
				`with upper-case letters; write the name in double quotes to keep its case`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := &pgconn.PgError{Code: "42704", Message: test.message}

			err := passwordWriter(&passwordRunner{refusal: refusal}).ExecuteSQL(c.Context(), `CREATE TABLE t (n text COLLATE "c")`)

			c.Assert(err, qt.ErrorIs, error(refusal))
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestWriterLeavesOtherRefusalsWithoutAHint is the control: a name with an
// upper-case letter was quoted, a name with no letter has no case, and another
// code or message is not a missing collation.
func TestWriterLeavesOtherRefusalsWithoutAHint(t *testing.T) {
	tests := []struct {
		name    string
		code    string
		message string
	}{
		{name: "a quoted name", code: "42704", message: `collation "MixedCase" for encoding "UTF8" does not exist`},
		{name: "no letter", code: "42704", message: `collation "1" for encoding "UTF8" does not exist`},
		{name: "a missing type", code: "42704", message: `type "c" does not exist`},
		{name: "another code", code: "42P01", message: `collation "c" for encoding "UTF8" does not exist`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			refusal := &pgconn.PgError{Code: test.code, Message: test.message}

			err := passwordWriter(&passwordRunner{refusal: refusal}).ExecuteSQL(c.Context(), `CREATE TABLE t (n text COLLATE "c")`)

			c.Assert(err, qt.ErrorIs, error(refusal))
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*hint:.*`)
		})
	}
}
