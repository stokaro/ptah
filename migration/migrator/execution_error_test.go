package migrator_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

// TestMigrationExecutionErrorRendersOneStatementLine pins that a failure names
// the statement once (stokaro/ptah#1196).
//
// Every dialect writer appends its own `SQL: <statement>` line before the error
// reaches here — `sqlite: SQL execution failed: …\nSQL: DROP TABLE t` — and
// this type appended another from its own Statement field. One failed statement
// therefore printed twice, in the CLI message and in the recorded revision
// `error` column, which is where the issue found it.
//
// The last row is why the check is against this error's own statement rather
// than a search for any SQL line: a wrapped error naming a DIFFERENT statement
// leaves nothing to deduplicate, and dropping this one's line there would lose
// the only mention of what actually failed.
func TestMigrationExecutionErrorRendersOneStatementLine(t *testing.T) {
	const statement = "DROP TABLE does_not_exist"

	tests := []struct {
		name          string
		err           error
		wantLines     int
		wantRemaining string
	}{
		{
			name:          "a writer that already named the statement",
			err:           fmt.Errorf("sqlite: SQL execution failed: no such table\nSQL: %s", statement),
			wantLines:     1,
			wantRemaining: statement,
		},
		{
			name:          "a writer that did not",
			err:           errors.New("no such table"),
			wantLines:     1,
			wantRemaining: statement,
		},
		{
			name:          "a wrapped error naming a different statement",
			err:           fmt.Errorf("no such table\nSQL: %s", "DROP TABLE other"),
			wantLines:     2,
			wantRemaining: statement,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			execErr := &migrator.MigrationExecutionError{
				Err:            test.err,
				Statement:      statement,
				StatementIndex: 2,
				Total:          2,
			}

			rendered := execErr.Error()

			c.Assert(countStatementLines(rendered), qt.Equals, test.wantLines)
			c.Assert(rendered, qt.Contains, test.wantRemaining)
			// Unwrapping is unaffected, so callers that inspect the cause keep
			// reaching it however the message was rendered.
			c.Assert(errors.Unwrap(execErr), qt.Equals, test.err)
		})
	}
}

// countStatementLines counts the `SQL: ` lines a rendered error carries. It
// matches at the start of a line so the words "migration SQL:" inside a
// sentence are not counted as one.
func countStatementLines(rendered string) int {
	count := 0
	for line := range strings.SplitSeq(rendered, "\n") {
		if strings.HasPrefix(line, "SQL: ") {
			count++
		}
	}
	return count
}
