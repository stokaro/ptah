package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// TestParse_SuggestsTheSameFunctionOnEveryRun reads a call to an unknown
// function many times and gets one message. The HCL library picked its
// suggestion by walking a map, so `now()` was answered with "pow" on one run
// and "log" on the next; the reader names the nearest function, and the first
// in name order among equally near ones.
func TestParse_SuggestsTheSameFunctionOnEveryRun(t *testing.T) {
	c := qt.New(t)
	document := []byte("table \"t\" {\n  column \"state\" {\n    type    = text\n    default = now()\n  }\n}\n")
	for range 100 {
		_, err := atlashcl.Parse(document, "schema.hcl")

		c.Assert(err, qt.ErrorMatches,
			`parse HCL schema at schema\.hcl:4,15-18: call to unknown function: There is no function named "now"\. Did you mean "pow"\?`)
	}
}
