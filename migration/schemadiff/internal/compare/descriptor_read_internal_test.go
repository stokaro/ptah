package compare

// White-box testing required: the descriptor readers below are unexported,
// and every exported comparison allocates for reasons of its own, so only a
// measurement of the readers themselves can say that they copy no descriptor.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestDescriptorReaders_CopyNothing holds the cost of the questions a
// comparison asks once per column, index and key it visits. Read through
// platform.Lookup, each answer copies the whole descriptor, dozens of
// allocations; read from tables kept per registry, none allocates.
func TestDescriptorReaders_CopyNothing(t *testing.T) {
	readers := []struct {
		name string
		read func(dialect string)
	}{
		{name: "comparison", read: func(dialect string) { comparisonOf(dialect) }},
		{name: "column facts", read: func(dialect string) { columnFactsOf(dialect) }},
		{name: "foreign key name rules", read: func(dialect string) { foreignKeyNameRulesOf(dialect) }},
		{name: "constraint naming", read: func(dialect string) { constraintNamingOf(dialect) }},
		{name: "foreign key naming", read: func(dialect string) { foreignKeyNamingOf(dialect) }},
		{name: "described", read: func(dialect string) { isDescribed(dialect) }},
		{name: "generated expressions", read: func(dialect string) { generatedExpressionIsRewritten(dialect) }},
	}
	for _, reader := range readers {
		for _, dialect := range append(platform.DialectSpellings(), "", "nonesuch") {
			t.Run(reader.name+"/"+dialect, func(t *testing.T) {
				c := qt.New(t)
				reader.read(dialect)

				c.Assert(testing.AllocsPerRun(20, func() { reader.read(dialect) }), qt.Equals, float64(0))
			})
		}
	}
}
