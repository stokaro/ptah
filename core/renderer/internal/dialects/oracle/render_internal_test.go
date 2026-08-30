package oracle

// White-box testing required: bareReferenceInExpression is unexported and, since
// core/renderer.prepareColumnNode began refusing an unnamed column, no public
// path reaches it with the input that used to hang. The guarantee under test is
// that the scan terminates for every input rather than for the inputs a caller
// currently sends, and that is not observable through the exported API once the
// caller validates -- a black-box test would be asserting the caller's refusal
// a second time (stokaro/ptah#2608).

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestBareReferenceInExpression_TerminatesForEveryColumnName is the reproduction
// from stokaro/ptah#2608 reduced to the loop that did not end.
//
// `strings.Index(s, "")` answers 0 for every s, and the scan advanced by
// `len(target)`, so an empty column name left the index where it was. Measured
// through the CLI before the fix: `ptah schema render --dialect oracle` spun at
// 100% of one core and did not stop for SIGTERM.
//
// The deadline is what makes the failure readable. Without it a regression is a
// package-wide ten-minute timeout naming no test; with it the assertion fails in
// a second and names this one.
func TestBareReferenceInExpression_TerminatesForEveryColumnName(t *testing.T) {
	tests := []struct {
		name       string
		columnName string
		expression string
		want       bool
	}{
		{
			name:       "an unnamed column names nothing in the expression",
			columnName: "",
			expression: "size * 2",
			want:       false,
		},
		{
			name:       "a quoted column referenced bare is still reported",
			columnName: "size",
			expression: "size * 2",
			want:       true,
		},
		{
			name:       "a quoted column referenced quoted is not reported",
			columnName: "size",
			expression: `"size" * 2`,
			want:       false,
		},
		{
			name:       "a column that needs no quoting is not reported",
			columnName: "amount",
			expression: "amount * 2",
			want:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			answered := make(chan bool, 1)
			go func() {
				answered <- bareReferenceInExpression(test.columnName, test.expression)
			}()

			var got bool
			select {
			case got = <-answered:
			case <-time.After(5 * time.Second):
				c.Fatalf("bareReferenceInExpression(%q, %q) did not return",
					test.columnName, test.expression)
			}

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
