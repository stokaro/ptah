package embedverify_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedverify"
)

// tombstonedThirdRow is the corpus stokaro/ptah#2742 reports, reachable through
// Ptah's own verbs: catch-up saw a source row disappear and wrote a tombstone
// for it, so the column holds three rows and two vectors.
func tombstonedThirdRow(target []embedverify.TargetRow) []embedverify.TargetRow {
	tombstone := target[0]
	tombstone.Key = "3"
	tombstone.Tombstone = true
	tombstone.Dimension = 0
	return append(target, tombstone)
}

// TestVerify_ATombstoneIsATargetRowWithoutAVectorHappyPath is the count the
// header was reporting as vectors.
//
// TargetRows stays what it is -- every position the walk stood opposite on the
// target side, which is the shape the verification record stores and what its
// digest binds. What was missing is that three of them are not three vectors,
// so a reader comparing the header against `SELECT count(*) WHERE embedding IS
// NOT NULL` found two.
func TestVerify_ATombstoneIsATargetRowWithoutAVectorHappyPath(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()

	report := verify(c, expectation, structure, source, tombstonedThirdRow(target), state)

	c.Assert(report.TargetRows, qt.Equals, 3)
	c.Assert(report.TargetVectors, qt.Equals, 2)
	c.Assert(report.Tombstones, qt.Equals, 1)
	c.Assert(report.SkippedTargets, qt.Equals, 0)
	// The generation is healthy: a tombstone is what a deleted source row is
	// supposed to leave behind, so this is a count to explain and not a finding.
	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_ASkippedTargetHoldsNoVectorEitherHappyPath is the other deliberate
// absence, and it is why the breakdown names which one it was rather than
// reporting the difference.
func TestVerify_ASkippedTargetHoldsNoVectorEitherHappyPath(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	source[1].Skipped = true
	target[1].Skipped = true
	target[1].Dimension = 0

	report := verify(c, expectation, structure, source, target, state)

	c.Assert(report.TargetRows, qt.Equals, 2)
	c.Assert(report.TargetVectors, qt.Equals, 1)
	c.Assert(report.SkippedTargets, qt.Equals, 1)
	c.Assert(report.Tombstones, qt.Equals, 0)
}

// TestVerify_AHealthyGenerationCountsEveryTargetRowAsAVectorHappyPath is the
// control, and it is what keeps the header quiet where there is nothing to
// explain.
//
// Without it a walk that counted no vectors at all would satisfy both tests
// above -- 0 vectors is as different from 3 target rows as 2 is.
func TestVerify_AHealthyGenerationCountsEveryTargetRowAsAVectorHappyPath(t *testing.T) {
	c := qt.New(t)

	expectation, structure, source, target, state := healthy()

	report := verify(c, expectation, structure, source, target, state)

	c.Assert(report.TargetRows, qt.Equals, 2)
	c.Assert(report.TargetVectors, qt.Equals, report.TargetRows)
	c.Assert(report.Tombstones, qt.Equals, 0)
	c.Assert(report.SkippedTargets, qt.Equals, 0)
}
