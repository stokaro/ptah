package dbtest

// White-box testing required: context interruption after a database connection
// is established must escape runner step reporting as an operational error.
// The exported API opens its own connection, so it cannot deterministically
// inject cancellation at this internal boundary.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

func TestRunnerRunCase_ContextCancellation(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	run := &runner{conn: conn}

	result, err := run.runCase(ctx, Case{
		Name: "interrupted",
		Steps: []Step{{Name: "query", Assert: &Assertion{
			Query:         "SELECT 1",
			ErrorContains: "context canceled",
		}}},
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Steps, qt.HasLen, 1)

	// The step does NOT pass, and this assertion was the other way round until
	// #2866: an `error_contains` naming the cancellation was counted as the
	// expected failure occurring. A caught step passes because the DATABASE
	// refused what it was asked, and a canceled context is not that -- the
	// statement may never have reached the server. Counting it made an
	// interrupted suite report that all of its refusals occurred, which the
	// issue's fail-closed list names as a defect in its own right.
	c.Assert(result.Steps[0].Passed, qt.IsFalse)
	c.Assert(result.Steps[0].Detail, qt.Contains, "interrupted rather than the statement refused")
}
