package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/deporder"
)

func viewNames(objects []deporder.ViewLike) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}

// TestViewLikesForCreateNeedsTheDialectToSeeAQuotedReference pins why a caller
// must pass its dialect, in the one spelling that separates the two functions.
//
// A view body referring to `"analytics"."zbase"` is referring to the declaration
// named `analytics.zbase`, and only the dialect-aware matcher knows that: the
// bare matcher compares the text as written, finds no reference, derives no
// dependency edge, and leaves the dependent first.
//
// The PostgreSQL planner asked the bare one for years, so a plan that read
// perfectly failed on execution with `relation "analytics.zbase" does not
// exist` (stokaro/ptah#1664).
func TestViewLikesForCreateNeedsTheDialectToSeeAQuotedReference(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "a quoted qualified reference",
			body: `SELECT id FROM "analytics"."zbase"`,
			want: []string{"analytics.zbase", "analytics.asummary"},
		},
		{
			name: "an unquoted qualified reference",
			body: "SELECT id FROM analytics.zbase",
			want: []string{"analytics.zbase", "analytics.asummary"},
		},
		{
			// A KNOWN LIMITATION, pinned rather than hidden: the matcher
			// recognizes a reference quoted wholly or not at all, and a mixed
			// spelling resolves against neither. The order is therefore left
			// as declared, and a plan built from it creates the dependent
			// first.
			//
			// It is out of scope here -- this change makes the PostgreSQL
			// planner ASK with its dialect, it does not rewrite the matcher --
			// and the row exists so a later fix flips a visible assertion
			// instead of quietly starting to work.
			name: "a partly quoted reference is not recognized",
			body: `SELECT id FROM analytics."zbase"`,
			want: []string{"analytics.asummary", "analytics.zbase"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// Declared dependent-first, and named so alphabetical order is the
			// wrong order, so only the dependency edge can reorder them.
			objects := []deporder.ViewLike{
				{Name: "analytics.asummary", Body: test.body},
				{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
			}

			ordered := deporder.ViewLikesForCreateForDialect(objects, "postgres")

			c.Assert(viewNames(ordered), qt.DeepEquals, test.want)
		})
	}
}

// TestViewLikesForCreateKeepsIndependentViewsInCallerOrder is the control: the
// sort must reorder only what a reference requires.
//
// A sort that always moved the second declaration ahead would satisfy every row
// above without deriving anything.
func TestViewLikesForCreateKeepsIndependentViewsInCallerOrder(t *testing.T) {
	c := qt.New(t)
	objects := []deporder.ViewLike{
		{Name: "analytics.asummary", Body: "SELECT id FROM analytics.events"},
		{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
	}

	ordered := deporder.ViewLikesForCreateForDialect(objects, "postgres")

	c.Assert(viewNames(ordered), qt.DeepEquals,
		[]string{"analytics.asummary", "analytics.zbase"})
}
