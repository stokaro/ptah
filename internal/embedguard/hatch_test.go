package embedguard_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedguard"
)

// TestHatchedByInterface_IsWrittenDown keeps the guard from going blind by
// degrees.
//
// `Scan` reports a declaration in the inference tree that no non-test file
// calls. It answers "calls" by reach -- the caller has to import the package or
// be in it -- with one deliberate widening: a name some interface declares is
// matched by NAME alone anywhere, because an interface exists so that a caller
// need not import the implementation.
//
// That widening cannot tell a legitimate case from a name that merely collides,
// and its potential reach is most of the tree: 151 of the 233 declarations
// carry a name some interface declares. Its actual reach is the eleven below.
// Nothing reports the gap between those two numbers, so one interface with an
// ordinary method name -- `Close`, `Run`, `Name` -- would take a large part of
// the guard's coverage with it and every test here would stay green.
//
// The set is written out rather than counted, for the reason the emission
// census writes out the statement shapes it cannot classify: a count says how
// much was hidden and a list says what. A declaration leaving is fine; one
// arriving is a decision about whether the guard still watches it.
func TestHatchedByInterface_IsWrittenDown(t *testing.T) {
	c := qt.New(t)

	hatched, err := embedguard.HatchedByInterface("../..")

	c.Assert(err, qt.IsNil)
	// Sorted, because that is what HatchedByInterface answers with. The two
	// reasons below are interleaved by that ordering rather than grouped, and
	// the alternative -- grouping and sorting in the assertion -- would compare
	// the set and stop comparing the order the function promises.
	c.Assert(hatched, qt.DeepEquals, []string{
		// An error type's Unwrap, matched by errors.Is and errors.As rather
		// than by a call anybody writes.
		"ptah.run/internal/embedengine.Unwrap",
		// The store, reached through the interfaces its callers declare. The
		// engine and the report layer each take a narrow interface rather than
		// importing embedpg, which is what keeps the persistence choice out of
		// them -- and what puts these here.
		"ptah.run/internal/embedpg.AbandonRun",
		"ptah.run/internal/embedpg.AppendEvent",
		"ptah.run/internal/embedpg.ClaimRun",
		"ptah.run/internal/embedpg.CreateRun",
		"ptah.run/internal/embedpg.Events",
		"ptah.run/internal/embedpg.MovePointer",
		"ptah.run/internal/embedpg.RegisterGeneration",
		"ptah.run/internal/embedpg.RunsForGeneration",
		// The catch-up loop calls this through the Changes interface, and
		// embedengine does not import embedpg. It is the case the widening was
		// added for, and the one the rule's own comment names.
		"ptah.run/internal/embedpg.Since",
		// The second error type's Unwrap.
		"ptah.run/internal/embedprovider.Unwrap",
	})
}
