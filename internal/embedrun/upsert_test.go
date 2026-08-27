package embedrun_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrun"
)

const generation = "gen-1"

// write is one target write for key "1" under the usual generation.
func write(kind embedrun.WriteKind, version, hash string) embedrun.TargetWrite {
	return embedrun.TargetWrite{
		Key: []string{"1"}, Generation: generation, Kind: kind,
		Version: version, InputHash: hash, Vector: []float32{1, 2},
	}
}

// TestResolveWrite_RepeatedWorkIsHarmless is what makes at-least-once delivery
// safe.
//
// The engine may deliver one batch twice -- a retry, a restart, a worker that
// lost its answer and asked again -- and the same source version with the same
// input hash landing twice has to be a no-op rather than a second write
// (stokaro/ptah#2068).
func TestResolveWrite_RepeatedWorkIsHarmless(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "7", "hash-7")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(resolved.Version, qt.Equals, "7")
}

// TestResolveWrite_ANewerVersionWins is the other half of the same rule.
func TestResolveWrite_ANewerVersionWins(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "7", "hash-7")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "8", "hash-8"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue)
	c.Assert(resolved.Version, qt.Equals, "8")
}

// TestResolveWrite_ALateAnswerDoesNotWin is Scenario 3, and the reason the
// decision is made against the TARGET rather than against what a worker
// believes.
//
// A provider request issued before the row changed can arrive after it: the
// vector it carries was computed from text the source has moved past, and
// storing it would leave the corpus holding a stale answer that looks current.
func TestResolveWrite_ALateAnswerDoesNotWin(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "9", "hash-9")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(resolved.Version, qt.Equals, "9")
}

// TestResolveWrite_ATombstoneSurvivesALateUpdate is Scenario 4.
//
// A source delete during a backfill produces a tombstone, and a retry of an
// update issued BEFORE the delete must not recreate the row. Without the
// tombstone being terminal, the corpus keeps serving a document the source no
// longer has -- which is the failure a deletion is supposed to prevent.
func TestResolveWrite_ATombstoneSurvivesALateUpdate(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteTombstone, "9", "")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(resolved.Kind, qt.Equals, embedrun.WriteTombstone)
}

// TestResolveWrite_ANewerSourceVersionRevivesATombstonedRow is the control.
//
// A tombstone that nothing could ever supersede would make a re-created source
// row permanently unembeddable, and the source is entitled to bring a key back.
func TestResolveWrite_ANewerSourceVersionRevivesATombstonedRow(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteTombstone, "9", "")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "10", "hash-10"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue)
	c.Assert(resolved.Kind, qt.Equals, embedrun.WriteUpsert)
	c.Assert(resolved.Version, qt.Equals, "10")
}

// TestResolveWrite_ATombstoneSurvivesAnUnorderedUpdate is what the tombstone
// rule holds that the version rule cannot.
//
// A late update whose version is merely OLDER is already refused by the
// ordering. This one carries the SAME version as the tombstone and different
// text, so nothing about the order refuses it -- only the tombstone being
// terminal does. Without that, a redelivered update from the moment of the
// delete brings the row back.
func TestResolveWrite_ATombstoneSurvivesAnUnorderedUpdate(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteTombstone, "9", "")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "9", "hash-9"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(resolved.Kind, qt.Equals, embedrun.WriteTombstone)
}

// TestResolveWrite_AnUnversionedWriteDoesNotLoseToAVersionedRow pins the choice
// where no order exists.
//
// A row written under a version strategy and an answer computed without one
// cannot be ordered against each other. Treating the absent version as "older"
// would silently discard every write after a strategy change; treating it as
// incomparable and letting the newer input hash decide is the freshness answer
// the input-hash strategy is built on.
func TestResolveWrite_AnUnversionedWriteDoesNotLoseToAVersionedRow(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "5", "hash-old")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-new"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue)
	c.Assert(resolved.InputHash, qt.Equals, "hash-new")
}

// TestResolveWrite_AWriteNeverCrossesGenerations is Decision 6 at the row
// level.
//
// Generations live side by side, and a write that landed on another
// generation's row would overwrite a corpus somebody is still querying -- from
// a run that has not been verified, let alone cut over.
func TestResolveWrite_AWriteNeverCrossesGenerations(t *testing.T) {
	c := qt.New(t)
	existing := embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-0", Kind: embedrun.WriteUpsert, Version: "7",
	}

	_, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "8", "hash-8"))

	c.Assert(err, qt.ErrorMatches, `.*belongs to generation gen-0 and the write is for gen-1.*`)
	c.Assert(changed, qt.IsFalse)
}

// TestResolveWrite_AnUnversionedGenerationStillStoresTheNewestAnswer is the
// input-hash strategy, which establishes freshness and not order.
//
// With no version on either side there is no ordering to appeal to, so a write
// whose input hash differs is the newer answer by construction -- and one whose
// hash matches is the same work arriving again.
func TestResolveWrite_AnUnversionedGenerationStillStoresTheNewestAnswer(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "", "hash-a")

	same, changedSame, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-a"))
	c.Assert(err, qt.IsNil)
	different, changedDifferent, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-b"))
	c.Assert(err, qt.IsNil)

	c.Assert(changedSame, qt.IsFalse)
	c.Assert(same.InputHash, qt.Equals, "hash-a")
	c.Assert(changedDifferent, qt.IsTrue)
	c.Assert(different.InputHash, qt.Equals, "hash-b")
}

// TestResolveWrite_TheFirstWriteForAKeyIsStored is the ordinary case a rule
// this careful could easily break.
func TestResolveWrite_TheFirstWriteForAKeyIsStored(t *testing.T) {
	c := qt.New(t)

	resolved, changed, err := embedrun.ResolveWrite(nil, write(embedrun.WriteUpsert, "1", "hash-1"))

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue)
	c.Assert(resolved.Version, qt.Equals, "1")
}

// TestResolveWrite_RefusesAWriteWithNoGeneration is the guard that stops a row
// being written outside every generation, where nothing would ever find it.
func TestResolveWrite_RefusesAWriteWithNoGeneration(t *testing.T) {
	c := qt.New(t)
	orphan := write(embedrun.WriteUpsert, "1", "hash-1")
	orphan.Generation = ""

	_, _, err := embedrun.ResolveWrite(nil, orphan)

	c.Assert(err, qt.ErrorMatches, `.*names no generation.*`)
}

// TestResolveWrite_VersionOrderSurvivesADigitBoundary is where a lexical
// comparison alone is wrong.
//
// "10" is newer than "9" and sorts before it as a string, so a counter passing
// ten would start losing to its own past. Comparing length first is what orders
// a monotonic counter correctly, and it leaves an equal-length comparison to do
// the rest -- which is what an RFC 3339 timestamp needs.
func TestResolveWrite_VersionOrderSurvivesADigitBoundary(t *testing.T) {
	tests := []struct {
		name        string
		existing    string
		incoming    string
		wantChanged bool
	}{
		{name: "ten beats nine", existing: "9", incoming: "10", wantChanged: true},
		{name: "nine loses to ten", existing: "10", incoming: "9"},
		{name: "one hundred beats ninety-nine", existing: "99", incoming: "100", wantChanged: true},
		{
			name:     "a later timestamp wins",
			existing: "2026-08-27T10:00:00Z", incoming: "2026-08-27T11:00:00Z", wantChanged: true,
		},
		{name: "an earlier timestamp loses", existing: "2026-08-27T11:00:00Z", incoming: "2026-08-27T10:00:00Z"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			existing := write(embedrun.WriteUpsert, test.existing, "hash-old")

			_, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, test.incoming, "hash-new"))

			c.Assert(err, qt.IsNil)
			c.Assert(changed, qt.Equals, test.wantChanged)
		})
	}
}
