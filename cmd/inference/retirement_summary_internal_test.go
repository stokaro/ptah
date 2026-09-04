package inference

// White-box testing required: retirementSummary is the sentence an operator
// reads to decide whether storage was reclaimed, and it is unexported. No CLI
// test reaches its column-kept branch, because a successful `--drop-column=false`
// retirement needs a live generation that has an index, and every end-to-end
// retirement fixture in the suite runs with the default.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcutover"
)

// The vectors live in the target column, so a retirement that keeps the column
// keeps them. Saying "is gone, with N vectors" for that run reported a
// destruction that did not happen. See stokaro/ptah#2743.
func TestRetirementSummary_SaysWhatWasDestroyed(t *testing.T) {
	tests := []struct {
		name string
		plan embedcutover.RetirementPlan
		want string
	}{
		{
			name: "the column goes, and the vectors with it",
			plan: embedcutover.RetirementPlan{Column: "embedding", DropsIndex: true, DropsColumn: true},
			want: "generation g1 is gone, with 7 vectors",
		},
		{
			name: "the table goes, and it was the generation's own",
			plan: embedcutover.RetirementPlan{
				Schema: "public", Table: "article_vectors", Column: "embedding",
				DropsIndex: true, DropsTable: true,
			},
			want: "generation g1 is gone, with 7 vectors and the table public.article_vectors they were in",
		},
		{
			name: "the column stays, so the vectors stay",
			plan: embedcutover.RetirementPlan{Column: "embedding", DropsIndex: true, DropsColumn: false},
			want: "generation g1 is retired, and its 7 vectors are still in column embedding: " +
				"the run kept the column, so it dropped the index and nothing else",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := retirementSummary("g1", 7, test.plan)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRetirementSummary_ATableRetirementNamesTheTable is why the two
// destructions do not share a sentence.
//
// "is gone, with 7 vectors" is true of both, and it is the sentence an operator
// reads to decide whether storage was reclaimed. Under the own-table layout a
// whole relation went with them, and the name of that relation is the thing
// they would have to look up afterwards -- from a registry row the retirement
// has just marked retired.
func TestRetirementSummary_ATableRetirementNamesTheTable(t *testing.T) {
	c := qt.New(t)

	got := retirementSummary("g1", 7, embedcutover.RetirementPlan{
		Schema: "public", Table: "article_vectors", Column: "embedding",
		DropsIndex: true, DropsTable: true,
	})

	c.Assert(got, qt.Contains, "public.article_vectors")
	c.Assert(got, qt.Contains, "is gone")
}

// The column-kept sentence must not claim the generation is gone, because that
// is the word an operator scans for.
func TestRetirementSummary_ColumnKeptDoesNotSayGone(t *testing.T) {
	c := qt.New(t)

	got := retirementSummary("g1", 7, embedcutover.RetirementPlan{
		Column: "embedding", DropsIndex: true, DropsColumn: false,
	})

	c.Assert(got, qt.Not(qt.Contains), "is gone")
}
