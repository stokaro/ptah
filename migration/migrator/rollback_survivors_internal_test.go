package migrator

// White-box testing required: statementsRan splits a body at the position the
// failure's type names, and the shapes it reads are a refusal raised before any
// statement was sent, a connection that broke, and a failure past the body. None
// of them can be told apart from outside on an engine that takes the whole body
// back, and the live tests cannot make a connection break at a chosen statement.

import (
	"context"
	"errors"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestStatementsRan(t *testing.T) {
	statements := []string{"one", "two", "three"}
	refused := errors.New("relation does not exist")
	broken := io.ErrUnexpectedEOF
	tests := []struct {
		name           string
		cause          error
		resumeFrom     int
		stage          bodyStage
		wantRan        []string
		wantUnanswered []string
	}{
		{
			name: "no statement was sent", cause: refused, resumeFrom: 1, stage: bodyNotStarted,
		},
		{
			name: "the body finished", resumeFrom: 1, stage: bodyFinished, wantRan: statements,
		},
		{
			name: "the body finished after a resume", resumeFrom: 3, stage: bodyFinished, wantRan: []string{"three"},
		},
		{
			name: "the server refused the first statement", resumeFrom: 1, stage: bodyStopped,
			cause: sentStatementError(refused, "one", 1, 3),
		},
		{
			name: "the server refused the third statement", resumeFrom: 1, stage: bodyStopped,
			cause: sentStatementError(refused, "three", 3, 3), wantRan: []string{"one", "two"},
		},
		{
			name: "the server refused a statement after a resume", resumeFrom: 2, stage: bodyStopped,
			cause: sentStatementError(refused, "three", 3, 3), wantRan: []string{"two"},
		},
		{
			name: "a refusal raised before any statement was sent names a statement too", resumeFrom: 1, stage: bodyStopped,
			cause: &MigrationExecutionError{Err: refused, Statement: "two", StatementIndex: 2, Total: 3},
		},
		{
			name: "the connection broke on the second statement", resumeFrom: 1, stage: bodyStopped,
			cause: sentStatementError(broken, "two", 2, 3), wantRan: []string{"one"}, wantUnanswered: []string{"two"},
		},
		{
			name: "the context ended on the second statement", resumeFrom: 1, stage: bodyStopped,
			cause: sentStatementError(context.Canceled, "two", 2, 3), wantRan: []string{"one"}, wantUnanswered: []string{"two"},
		},
		{
			name: "an observer failed after the second statement ran", resumeFrom: 1, stage: bodyStopped,
			cause:   &StatementObservationError{Err: refused, Event: StatementEvent{Statement: "two", Index: 2, Total: 3}},
			wantRan: []string{"one", "two"},
		},
		{
			name: "a failure that names no statement leaves the position open", resumeFrom: 2, stage: bodyStopped,
			cause: refused, wantUnanswered: []string{"two", "three"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			ran, unanswered := statementsRan(test.cause, statements, test.resumeFrom, test.stage)

			c.Assert(append([]string(nil), ran...), qt.DeepEquals, test.wantRan)
			c.Assert(append([]string(nil), unanswered...), qt.DeepEquals, test.wantUnanswered)
		})
	}
}
