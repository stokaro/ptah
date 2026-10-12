package main

// White-box testing required: a main package cannot be imported, so run is
// reachable only from a test inside the package.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRun runs every demo the command names, the way
// `go run ./internal/examples/ast_demo <command>` does; no CI job runs it
// otherwise. Each demo returns the first error it meets, so a render that
// fails fails the test.
func TestRun(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "usage", args: nil, want: "Available commands:"},
		{name: "demo", args: []string{"demo"}, want: `CREATE TYPE "order_status" AS ENUM`},
		{name: "compare", args: []string{"compare"}, want: "BENEFITS OF AST APPROACH:"},
		{name: "advanced", args: []string{"advanced"}, want: "Analysis result: 2 tables, 4 columns, 1 indexes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out strings.Builder

			err := run(&out, test.args)

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}
