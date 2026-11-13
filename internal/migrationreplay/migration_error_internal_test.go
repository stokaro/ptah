package migrationreplay

// White-box testing required: answerOf decides which error of a failed
// statement's chain a report may show, and the shapes it must stop at -- a
// PostgreSQL writer's password redaction, a provider's classified execution
// error -- are produced by engines a replay test on the bundled SQLite cannot
// reach, while the one it passes through is the only shape SQLite produces.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

// rewrittenError rewrites the text of the error it wraps, the way a writer
// that redacts a secret does.
type rewrittenError struct {
	err  error
	text string
}

func (e rewrittenError) Error() string { return e.text }
func (e rewrittenError) Unwrap() error { return e.err }

// classifiedError unwraps to a sentinel its message does not quote, the way a
// provider's execution error unwraps to the outcome it classifies.
type classifiedError struct{ reason string }

func (e classifiedError) Error() string { return "provider execution: NOT_APPLIED: " + e.reason }
func (classifiedError) Unwrap() error   { return errExecution }

var errExecution = errors.New("provider execution did not complete")

// passThrough has its wrapped error's message, as the migrator's marker of a
// statement that was sent does.
type passThrough struct{ err error }

func (e passThrough) Error() string { return e.err.Error() }
func (e passThrough) Unwrap() error { return e.err }

// migratorWraps wraps a writer's error the way the migrator wraps a statement
// that was sent and failed.
func migratorWraps(writer error) error {
	return fmt.Errorf("failed to execute migration SQL: %w", writer)
}

func TestAnswerOf(t *testing.T) {
	const statement = "ALTER TABLE users RENAME COLUMN nope TO x"
	driver := errors.New(`SQL logic error: no such column: "nope" (1)`)
	secret := errors.New(`password "hunter2" does not meet the policy`)
	sentinel := errors.New("operation refused")

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "a dialect writer's context is passed through to the driver's error",
			err:  migratorWraps(fmt.Errorf("sqlite: SQL execution failed: %w\nSQL: %s", passThrough{err: driver}, statement)),
			want: `SQL logic error: no such column: "nope" (1)`,
		},
		{
			name: "a redaction is kept, so the secret it removed stays removed",
			err: migratorWraps(fmt.Errorf("SQL execution failed: %w\nSQL: %s",
				rewrittenError{err: secret, text: `password "[secret]" does not meet the policy`}, statement)),
			want: `password "[secret]" does not meet the policy`,
		},
		{
			name: "a classification is kept rather than the sentinel it unwraps to",
			err:  migratorWraps(classifiedError{reason: "no such table: gadgets"}),
			want: "provider execution: NOT_APPLIED: no such table: gadgets",
		},
		{
			name: "a sentinel attached in front of its cause is kept with the cause",
			err:  migratorWraps(fmt.Errorf("%w: no such table: gadgets", sentinel)),
			want: "operation refused: no such table: gadgets",
		},
		{
			name: "a branch is kept whole",
			err:  migratorWraps(errors.Join(driver, errors.New("rollback failed"))),
			want: "SQL logic error: no such column: \"nope\" (1)\nrollback failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := answerOf(test.err)

			c.Assert(got.Error(), qt.Equals, test.want)
		})
	}
}
