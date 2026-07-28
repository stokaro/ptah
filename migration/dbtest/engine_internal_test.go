package dbtest

// White-box testing required: context interruption after a database connection
// is established must escape runner step reporting as an operational error.
// The exported API opens its own connection, so it cannot deterministically
// inject cancellation at this internal boundary.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
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
	c.Assert(result.Steps[0].Passed, qt.IsTrue)
}
