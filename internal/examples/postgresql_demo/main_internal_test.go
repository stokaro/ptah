package main

// White-box testing required: a main package cannot be imported, so run is
// reachable only from a test inside the package.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRun drives the demo the way `go run ./internal/examples/postgresql_demo`
// does from the repository root; no CI job runs it otherwise. A parse of the
// working directory reads every fixture in the tree from there, and four
// integration fixtures declare one policy with different definitions.
func TestRun(t *testing.T) {
	t.Chdir("../../..")
	c := qt.New(t)
	var out strings.Builder

	err := run(&out)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "PostgreSQL parser demo completed successfully!")
	c.Assert(out.String(), qt.Contains, `CONSTRAINT "positive_capacity" CHECK (capacity > 0)`)
	c.Assert(out.String(), qt.Contains, `ALTER TABLE "bookings" ADD CONSTRAINT "no_overlapping_bookings" EXCLUDE USING gist (room_id WITH =, during WITH &&)`)
	c.Assert(out.String(), qt.Contains, "Found 4 tables with constraints:")
}
