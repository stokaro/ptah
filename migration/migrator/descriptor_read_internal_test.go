package migrator

// White-box testing required: the descriptor readers below are unexported,
// and every exported migrator call allocates for reasons of its own, so only a
// measurement of the readers themselves can say that they copy no descriptor.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestDescriptorReaders_CopyNothing holds the cost of the questions the
// migrator asks once per statement it writes or reads about a revision. Read
// through platform.Lookup, each answer copies the whole descriptor, dozens of
// allocations; read from tables kept per registry, none allocates.
func TestDescriptorReaders_CopyNothing(t *testing.T) {
	readers := []struct {
		name string
		read func(dialect string)
	}{
		{name: "revision table", read: func(dialect string) { revisionTableFor(dialect) }},
		{name: "default schema", read: func(dialect string) { defaultSchemaFor(dialect) }},
		{name: "atomic index creation", read: func(dialect string) { atomicIndexCreations.Get(dialect) }},
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
