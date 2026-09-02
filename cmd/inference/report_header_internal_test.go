package inference

// White-box testing required: printReport renders the line an operator reads
// after every verification, and it is reached through a full cutover decision
// against a live database. The value under test is the sentence, so this calls
// the renderer with reports built directly.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedverify"
)

// TestPrintReport_TheHeaderExplainsATargetCountThatIsNotAVectorCount covers
// stokaro/ptah#2742.
//
// `2 source rows, 3 target rows` sat beside a column holding two vectors, after
// catch-up tombstoned a row. The total is the shape the verification record
// stores, so it stays; what was missing is why it differs from the number a
// reader would get from the database.
func TestPrintReport_TheHeaderExplainsATargetCountThatIsNotAVectorCount(t *testing.T) {
	tests := []struct {
		name   string
		report embedverify.Report
		want   string
	}{
		{
			name: "a tombstone",
			report: embedverify.Report{
				Generation: "4d42572104c3", SourceRows: 2,
				TargetRows: 3, TargetVectors: 2, Tombstones: 1,
			},
			want: "generation 4d42572104c3: 2 source rows, 3 target rows (2 with a vector, 1 tombstoned)",
		},
		{
			name: "a skip",
			report: embedverify.Report{
				Generation: "4d42572104c3", SourceRows: 2,
				TargetRows: 2, TargetVectors: 1, SkippedTargets: 1,
			},
			want: "generation 4d42572104c3: 2 source rows, 2 target rows (1 with a vector, 1 skipped)",
		},
		{
			name: "both, and a row nothing wrote",
			report: embedverify.Report{
				Generation: "4d42572104c3", SourceRows: 4,
				TargetRows: 4, TargetVectors: 1, Tombstones: 1, SkippedTargets: 1,
			},
			want: "generation 4d42572104c3: 4 source rows, 4 target rows (1 with a vector, 1 tombstoned, 1 skipped)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			c.Assert(printReport(&out, test.report), qt.IsNil)

			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}

// TestPrintReport_AHealthyGenerationSaysNothingExtra is the control.
//
// Every target row holds a vector, so there is no difference to explain and
// "(2 with a vector)" after "2 target rows" is noise on the line every
// verification prints.
func TestPrintReport_AHealthyGenerationSaysNothingExtra(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	c.Assert(printReport(&out, embedverify.Report{
		Generation: "4d42572104c3", SourceRows: 2, TargetRows: 2, TargetVectors: 2,
	}), qt.IsNil)

	c.Assert(out.String(), qt.Contains, "generation 4d42572104c3: 2 source rows, 2 target rows\n")
	c.Assert(out.String(), qt.Not(qt.Contains), "with a vector")
}
