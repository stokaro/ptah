package migrator

// White-box testing required: both helpers exist only as bound arguments on a
// revision UPDATE, so neither is reachable through an exported API, and the
// index one has to be exercised at values the exported surface cannot produce
// on demand.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestAtlasFailureError pins what the Atlas revision table's `error` column
// records (stokaro/ptah#1196 item 1).
//
// The pinned community binary v1.3.0 records the database's own message and
// nothing else. Ptah recorded the chain it built on the way up, whose first
// line carried a `failed to execute migration SQL:` prefix and whose second
// repeated the statement that `error_stmt` already holds in full.
func TestAtlasFailureError(t *testing.T) {
	driverFailure := errors.New("SQL logic error: no such table: missing_table (1)")

	tests := []struct {
		name    string
		failure error
		want    string
	}{
		{
			name:    "no failure records nothing",
			failure: nil,
			want:    "",
		},
		{
			name:    "a bare driver error is recorded as it is",
			failure: driverFailure,
			want:    "SQL logic error: no such table: missing_table (1)",
		},
		{
			name:    "Ptah's own wrapping is dropped",
			failure: fmt.Errorf("failed to execute migration SQL: %w", driverFailure),
			want:    "SQL logic error: no such table: missing_table (1)",
		},
		{
			name: "every layer of it, however deep",
			failure: fmt.Errorf("apply migration 20260101000001: %w",
				fmt.Errorf("failed to execute migration SQL: %w", driverFailure)),
			want: "SQL logic error: no such table: missing_table (1)",
		},
		{
			name:    "an unwrapped error that merely mentions a cause is left alone",
			failure: errors.New("failed to execute migration SQL: something"),
			want:    "failed to execute migration SQL: something",
		},
		{
			// A driver whose message arrives padded, which is why the recorded
			// value is trimmed rather than passed through.
			name:    "surrounding whitespace is trimmed",
			failure: paddedError{},
			want:    "no such table: t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(atlasFailureError(test.failure), qt.Equals, test.want)
		})
	}
}

// paddedError stands in for a driver that pads its own message. Written as a
// type rather than errors.New because the linter reads a padded literal as a
// malformed error string, which is exactly the shape being covered here.
type paddedError struct{}

func (paddedError) Error() string { return "  no such table: t \n" }

// TestAtlasFailureStatement pins the `error_stmt` column (item 2).
//
// The row that matters is the tx-mode-all one. The applied count is zeroed
// whenever the transaction rolled the body back, so indexing the source by it
// would record the file's FIRST statement for every such failure — a plausible
// value, in the column an operator reads to find out what broke.
func TestAtlasFailureStatement(t *testing.T) {
	const source = "-- atlas:txmode none\n\nCREATE TABLE a (id int);\nINSERT INTO missing_table (id) VALUES (1);\n"

	tests := []struct {
		name        string
		sql         string
		failedIndex int
		stmt        string
		want        string
	}{
		{
			name:        "the failing statement carries its terminator",
			sql:         source,
			failedIndex: 2,
			stmt:        "INSERT INTO missing_table (id) VALUES (1)",
			want:        "INSERT INTO missing_table (id) VALUES (1);",
		},
		{
			name:        "the first statement is reachable too",
			sql:         source,
			failedIndex: 1,
			stmt:        "CREATE TABLE a (id int)",
			want:        "CREATE TABLE a (id int);",
		},
		{
			name:        "a terminator on its own line comes back as written",
			sql:         "CREATE TABLE q (id int)\n;\nINSERT INTO nope (id) VALUES (1);\n",
			failedIndex: 1,
			stmt:        "CREATE TABLE q (id int)",
			want:        "CREATE TABLE q (id int)\n;",
		},
		{
			name:        "an index past the source falls back to the executor's text",
			sql:         source,
			failedIndex: 9,
			stmt:        "INSERT INTO missing_table (id) VALUES (1)",
			want:        "INSERT INTO missing_table (id) VALUES (1)",
		},
		{
			name:        "no reported index falls back too",
			sql:         source,
			failedIndex: 0,
			stmt:        "INSERT INTO missing_table (id) VALUES (1)",
			want:        "INSERT INTO missing_table (id) VALUES (1)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			c.Assert(
				atlasFailureStatement(test.sql, "sqlite", test.failedIndex, test.stmt),
				qt.Equals,
				test.want,
			)
		})
	}
}
