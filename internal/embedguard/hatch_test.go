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
// and its potential reach is a large part of the tree: 135 of the 322
// declarations carry a name some interface declares. Its actual reach is the
// list below.
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
		// The PostgreSQL store, source, outbox and searcher, reached through
		// the interfaces their callers declare. The engine takes a narrow
		// interface, and the verbs, the report layer and the agent tools take
		// embedbackend's contract rather than importing an engine, which is
		// what keeps the persistence choice out of them -- and what puts these
		// here.
		"ptah.run/internal/embedpg.AbandonRun",
		"ptah.run/internal/embedpg.AppendEvent",
		"ptah.run/internal/embedpg.CheckMode",
		"ptah.run/internal/embedpg.ClaimRun",
		"ptah.run/internal/embedpg.CreateRun",
		"ptah.run/internal/embedpg.Current",
		"ptah.run/internal/embedpg.Events",
		"ptah.run/internal/embedpg.Maintain",
		"ptah.run/internal/embedpg.Measure",
		"ptah.run/internal/embedpg.MovePointer",
		"ptah.run/internal/embedpg.MovePointerWithMaintenance",
		"ptah.run/internal/embedpg.MovePointerWithRollback",
		"ptah.run/internal/embedpg.Prune",
		"ptah.run/internal/embedpg.QueryParameters",
		"ptah.run/internal/embedpg.ReachPhase",
		"ptah.run/internal/embedpg.RecordVerification",
		"ptah.run/internal/embedpg.RegisterGeneration",
		"ptah.run/internal/embedpg.RetireGeneration",
		"ptah.run/internal/embedpg.RunsForGeneration",
		"ptah.run/internal/embedpg.Search",
		// The catch-up loop calls this through the Changes interface, and
		// embedengine does not import embedpg. It is the case the widening was
		// added for, and the one the rule's own comment names.
		"ptah.run/internal/embedpg.Since",
		"ptah.run/internal/embedpg.Unprocessed",
		// The second error type's Unwrap.
		"ptah.run/internal/embedprovider.Unwrap",
		// The backend over a provider's embedding store, reached through
		// embedbackend's contract and the engine's narrow interfaces for the
		// same reason as the two above. integration/providercli drives each of
		// them through the command line against the fixture provider.
		"ptah.run/internal/embedrpc.AppendEvent",
		"ptah.run/internal/embedrpc.Commit",
		"ptah.run/internal/embedrpc.CountGenerationRows",
		"ptah.run/internal/embedrpc.Current",
		"ptah.run/internal/embedrpc.Dialect",
		"ptah.run/internal/embedrpc.EnsureRunIndex",
		"ptah.run/internal/embedrpc.EnsureSchema",
		"ptah.run/internal/embedrpc.Events",
		"ptah.run/internal/embedrpc.GenerationIndexExists",
		"ptah.run/internal/embedrpc.Measure",
		"ptah.run/internal/embedrpc.NewSearcher",
		"ptah.run/internal/embedrpc.NewSource",
		"ptah.run/internal/embedrpc.NewTarget",
		"ptah.run/internal/embedrpc.PrepareRun",
		"ptah.run/internal/embedrpc.QueryParameters",
		"ptah.run/internal/embedrpc.ReadStructure",
		"ptah.run/internal/embedrpc.RunsForGeneration",
		"ptah.run/internal/embedrpc.Scan",
		"ptah.run/internal/embedrpc.Search",
		"ptah.run/internal/embedrpc.VerificationCorpus",
		"ptah.run/internal/embedrpc.WithResolvedRelations",
		// The YDB backend, reached through embedbackend's contract for the
		// same reason as the PostgreSQL one above. The integration suite's YDB
		// lifecycle test drives each of them through the command line.
		"ptah.run/internal/embedydb.AppendEvent",
		"ptah.run/internal/embedydb.Current",
		"ptah.run/internal/embedydb.EnsureRunIndex",
		"ptah.run/internal/embedydb.EnsureSchema",
		"ptah.run/internal/embedydb.Events",
		"ptah.run/internal/embedydb.Measure",
		"ptah.run/internal/embedydb.NewSearcher",
		"ptah.run/internal/embedydb.NewSource",
		"ptah.run/internal/embedydb.NewTarget",
		"ptah.run/internal/embedydb.PrepareRun",
		"ptah.run/internal/embedydb.QueryParameters",
		"ptah.run/internal/embedydb.ReadStructure",
		"ptah.run/internal/embedydb.RunsForGeneration",
		"ptah.run/internal/embedydb.Search",
		"ptah.run/internal/embedydb.VerificationCorpus",
		"ptah.run/internal/embedydb.WithResolvedRelations",
	})
}
