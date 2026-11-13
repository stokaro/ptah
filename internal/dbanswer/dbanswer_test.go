package dbanswer_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbanswer"
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

// writerWraps wraps a driver's error the way a built-in dialect writer does:
// context in front of the message and the statement after it.
func writerWraps(driver error, statement string) error {
	return fmt.Errorf("SQL execution failed: %w\nSQL: %s", driver, statement)
}

func TestOf(t *testing.T) {
	const statement = "ALTER TABLE users RENAME COLUMN nope TO x"
	driver := errors.New(`SQL logic error: no such column: "nope" (1)`)
	quoted := errors.New(`ERROR: syntax error at or near "E'hunter2zz'" (SQLSTATE 42601)`)
	sentinel := errors.New("operation refused")

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "an error that wraps nothing is the answer",
			err:  driver,
			want: `SQL logic error: no such column: "nope" (1)`,
		},
		{
			name: "a dialect writer's context is passed through to the driver's error",
			err:  migratorWraps(writerWraps(passThrough{err: driver}, statement)),
			want: `SQL logic error: no such column: "nope" (1)`,
		},
		{
			name: "a redaction is kept, so the secret it removed stays removed",
			err: migratorWraps(writerWraps(
				rewrittenError{err: quoted, text: `ERROR: syntax error at or near "E'[secret]'" (SQLSTATE 42601)`}, statement)),
			want: `ERROR: syntax error at or near "E'[secret]'" (SQLSTATE 42601)`,
		},
		{
			name: "a redaction that found nothing to take out is passed through",
			err:  migratorWraps(writerWraps(rewrittenError{err: driver, text: driver.Error()}, statement)),
			want: `SQL logic error: no such column: "nope" (1)`,
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
			t.Parallel()
			c := qt.New(t)

			got := dbanswer.Of(test.err)

			c.Assert(got.Error(), qt.Equals, test.want)
		})
	}
}

// The answer is an error of the chain rather than a copy of its text, so a
// caller that classifies it still reaches what is below it.
func TestOf_AnswerStillUnwrapsToTheDriversError(t *testing.T) {
	c := qt.New(t)
	quoted := errors.New(`ERROR: syntax error at or near "E'hunter2zz'" (SQLSTATE 42601)`)

	got := dbanswer.Of(migratorWraps(rewrittenError{err: quoted, text: "ERROR: [secret]"}))

	c.Assert(got, qt.ErrorIs, quoted)
}

func TestOf_NilIsNil(t *testing.T) {
	c := qt.New(t)

	c.Assert(dbanswer.Of(nil), qt.IsNil)
}
