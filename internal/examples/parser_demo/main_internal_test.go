package main

// White-box testing required: a main package cannot be imported, so run is
// reachable only from a test inside the package.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRun runs the demo the way `go run ./internal/examples/parser_demo` does;
// no CI job runs it otherwise.
func TestRun(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := run(&out)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Successfully parsed 4 SQL statements:")
	c.Assert(out.String(), qt.Contains, "Parser demo completed successfully!")
}
