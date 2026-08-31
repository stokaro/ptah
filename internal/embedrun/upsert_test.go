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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"), embedrun.OrderNumeric)

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsFalse)
	c.Assert(resolved.Version, qt.Equals, "7")
}

// TestResolveWrite_ANewerVersionWins is the other half of the same rule.
func TestResolveWrite_ANewerVersionWins(t *testing.T) {
	c := qt.New(t)
	existing := write(embedrun.WriteUpsert, "7", "hash-7")

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "8", "hash-8"), embedrun.OrderNumeric)

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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"), embedrun.OrderNumeric)

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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "7", "hash-7"), embedrun.OrderNumeric)

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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "10", "hash-10"), embedrun.OrderNumeric)

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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "9", "hash-9"), embedrun.OrderNumeric)

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

	resolved, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-new"), embedrun.OrderNumeric)

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

	_, changed, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "8", "hash-8"), embedrun.OrderNumeric)

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

	same, changedSame, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-a"), embedrun.OrderNumeric)
	c.Assert(err, qt.IsNil)
	different, changedDifferent, err := embedrun.ResolveWrite(&existing, write(embedrun.WriteUpsert, "", "hash-b"), embedrun.OrderNumeric)
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

	resolved, changed, err := embedrun.ResolveWrite(nil, write(embedrun.WriteUpsert, "1", "hash-1"), embedrun.OrderNumeric)

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

	_, _, err := embedrun.ResolveWrite(nil, orphan, embedrun.OrderNumeric)

	c.Assert(err, qt.ErrorMatches, `.*names no generation.*`)
}

// TestResolveWrite_TheStrategyDecidesTheOrder is where one comparison for both
// shapes is wrong.
//
// A counter needs "10" to beat "9", which no lexical comparison gives; a
// rendered timestamp needs 11:00:00.1 to beat 10:00:00.123456, which no
// length-first comparison gives. Ordering by length then lexicographically gets
// the first right and the second exactly backwards, and the second is the
// shape stokaro/ptah#2635 measured: a driver trims trailing zeros from the
// fractional seconds, so 9.85% of `clock_timestamp()` values render short and
// the fresh answer was discarded as stale.
//
// The order is a column of the table because it is data the case carries, and
// the two halves are rows of one table because the property is one: the
// strategy that produced a version decides how two of them compare.
func TestResolveWrite_TheStrategyDecidesTheOrder(t *testing.T) {
	tests := []struct {
		name        string
		order       embedrun.VersionOrder
		existing    string
		incoming    string
		wantChanged bool
	}{
		{
			name: "ten beats nine", order: embedrun.OrderNumeric,
			existing: "9", incoming: "10", wantChanged: true,
		},
		{name: "nine loses to ten", order: embedrun.OrderNumeric, existing: "10", incoming: "9"},
		{
			name: "one hundred beats ninety-nine", order: embedrun.OrderNumeric,
			existing: "99", incoming: "100", wantChanged: true,
		},
		{
			name: "a later timestamp wins", order: embedrun.OrderTimestamp,
			existing: "2026-08-27T10:00:00Z", incoming: "2026-08-27T11:00:00Z", wantChanged: true,
		},
		{
			name: "an earlier timestamp loses", order: embedrun.OrderTimestamp,
			existing: "2026-08-27T11:00:00Z", incoming: "2026-08-27T10:00:00Z",
		},
		{
			// The measured case. The incoming version is an hour later and
			// three characters shorter, because the driver trimmed the zeros.
			name: "a shorter rendering of a later instant wins", order: embedrun.OrderTimestamp,
			existing: "2026-01-01T11:00:00.123456+01:00", incoming: "2026-01-01T12:00:00.1+01:00",
			wantChanged: true,
		},
		{
			// And the other direction: longer is not newer either.
			name: "a longer rendering of an earlier instant loses", order: embedrun.OrderTimestamp,
			existing: "2026-01-01T12:00:00.1+01:00", incoming: "2026-01-01T11:00:00.123456+01:00",
		},
		{
			// Zones are read, not compared as text. These are the same
			// instant, so the incoming is not newer and the hashes differ, so
			// it wins as a change rather than as a later version.
			name: "one instant in two zones is one instant", order: embedrun.OrderTimestamp,
			existing: "2026-01-01T12:00:00+01:00", incoming: "2026-01-01T11:00:00Z",
			wantChanged: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			existing := write(embedrun.WriteUpsert, test.existing, "hash-old")

			_, changed, err := embedrun.ResolveWrite(
				&existing, write(embedrun.WriteUpsert, test.incoming, "hash-new"), test.order)

			c.Assert(err, qt.IsNil)
			c.Assert(changed, qt.Equals, test.wantChanged)
		})
	}
}

// TestResolveWrite_AnUnorderableVersionDoesNotLoseFreshWork states the fallback.
//
// A strategy that records no version, or a value the order cannot read, puts
// nothing in order — so neither the no-op check nor the late-answer check
// fires and the incoming write lands. That is the direction that does not throw
// away a provider answer already paid for, which is the harm stokaro/ptah#2635
// measured.
func TestResolveWrite_AnUnorderableVersionDoesNotLoseFreshWork(t *testing.T) {
	tests := []struct {
		name     string
		order    embedrun.VersionOrder
		existing string
		incoming string
	}{
		{
			name: "a strategy that records no version", order: embedrun.OrderUnknown,
			existing: "anything", incoming: "anything else",
		},
		{
			name: "a timestamp neither layout parses", order: embedrun.OrderTimestamp,
			existing: "last tuesday", incoming: "the tuesday before",
		},
		{
			name: "a counter that is not a number", order: embedrun.OrderNumeric,
			existing: "v2", incoming: "v1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			existing := write(embedrun.WriteUpsert, test.existing, "hash-old")

			_, changed, err := embedrun.ResolveWrite(
				&existing, write(embedrun.WriteUpsert, test.incoming, "hash-new"), test.order)

			c.Assert(err, qt.IsNil)
			c.Assert(changed, qt.IsTrue)
		})
	}
}

// TestResolveWrite_TheRenderingsAVersionArrivesIn pins each layout to something
// that produces it.
//
// The first version of this fix parsed RFC 3339 only, and a live test reading
// the column through `::text` got PostgreSQL's space-separated form — which
// parsed as nothing, so both directions were "not comparable" and the test
// passed for the wrong reason. Its control is what caught that
// (stokaro/ptah#2635).
func TestResolveWrite_TheRenderingsAVersionArrivesIn(t *testing.T) {
	tests := []struct {
		name     string
		earlier  string
		later    string
		producer string
	}{
		{
			name:     "RFC 3339 with a zone",
			earlier:  "2026-01-01T10:00:00.123456+00:00",
			later:    "2026-01-01T11:00:00.1+00:00",
			producer: "the pgx driver, scanning a timestamptz into a string",
		},
		{
			name:     "RFC 3339 with no zone",
			earlier:  "2026-01-01T10:00:00.123456",
			later:    "2026-01-01T11:00:00.1",
			producer: "the same driver, scanning a plain timestamp",
		},
		{
			name:     "PostgreSQL's text cast with a zone",
			earlier:  "2026-01-01 10:00:00.123456+00",
			later:    "2026-01-01 11:00:00.1+00",
			producer: "a ::text cast in a view or a generated column",
		},
		{
			name:     "PostgreSQL's text cast with no zone",
			earlier:  "2026-01-01 10:00:00.123456",
			later:    "2026-01-01 11:00:00.1",
			producer: "the same cast over a plain timestamp",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// Every row is the defect's shape: the later instant renders
			// SHORTER, so a length-first order calls it stale.
			c.Assert(len(test.later) < len(test.earlier), qt.IsTrue,
				qt.Commentf("%s", test.producer))
			existing := write(embedrun.WriteUpsert, test.earlier, "hash-old")

			_, changed, err := embedrun.ResolveWrite(
				&existing, write(embedrun.WriteUpsert, test.later, "hash-new"),
				embedrun.OrderTimestamp)

			c.Assert(err, qt.IsNil)
			c.Assert(changed, qt.IsTrue)

			// And the control that catches "neither parsed, so neither is
			// older": the pair the other way round must lose.
			newer := write(embedrun.WriteUpsert, test.later, "hash-new")
			_, backwards, err := embedrun.ResolveWrite(
				&newer, write(embedrun.WriteUpsert, test.earlier, "hash-old"),
				embedrun.OrderTimestamp)

			c.Assert(err, qt.IsNil)
			c.Assert(backwards, qt.IsFalse,
				qt.Commentf("a layout nothing parses makes both directions win"))
		})
	}
}

// TestResolveWrite_AVersionTheStrategyCannotReadOrdersNothing states a decision
// rather than leaving it to be discovered.
//
// A specification declaring `updated_at` over a column holding "9" produces a
// version no timestamp layout parses. Nothing is comparable, so neither the
// no-op check nor the late-answer check fires and the incoming write lands —
// which means a late retry can replace a newer vector.
//
// That is deliberate and it is the lesser evil. The alternative is to fall back
// to ordering opaque strings by length, and that fallback IS the defect
// stokaro/ptah#2635 is about: it reads a shorter rendering of a later instant
// as older and throws away work already paid for. Guessing an order for a value
// the strategy cannot read is how the bug got in.
//
// What closes the hole is configuration, not a fallback: the version column has
// to be readable under the strategy that names it, and the specification
// reference says so.
func TestResolveWrite_AVersionTheStrategyCannotReadOrdersNothing(t *testing.T) {
	c := qt.New(t)
	// A counter under a timestamp strategy: the shape a misconfigured
	// specification produces.
	newer := write(embedrun.WriteUpsert, "9", "hash-new")

	_, changed, err := embedrun.ResolveWrite(
		&newer, write(embedrun.WriteUpsert, "7", "hash-old"), embedrun.OrderTimestamp)

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue,
		qt.Commentf("nothing orders these, so the incoming write lands"))

	// And the control: under the order these values actually have, the older
	// one loses. The pair is what makes the sentence above a decision about
	// unreadable values rather than a claim that ordering is broken.
	_, ordered, err := embedrun.ResolveWrite(
		&newer, write(embedrun.WriteUpsert, "7", "hash-old"), embedrun.OrderNumeric)

	c.Assert(err, qt.IsNil)
	c.Assert(ordered, qt.IsFalse)
}
