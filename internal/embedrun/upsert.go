package embedrun

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TargetWrite is one row's effect on the target generation.
//
// The natural identity is the generation plus the source key, which is what
// makes a repeated write harmless: writing the same source version and the same
// input hash twice lands on the same row with the same content
// (stokaro/ptah#2068).
type TargetWrite struct {
	// Key is the source key, in the specification's key order.
	Key []string
	// Generation is the generation identity this write belongs to.
	Generation string
	// InputHash and Version are what the vector was computed from.
	InputHash string
	Version   string
	// Vector is the embedding, empty for a skip or a delete.
	Vector []float32
	// Kind is what the write does.
	Kind WriteKind
	// SkipReason is why a skip has no vector, and is empty otherwise.
	SkipReason string
	// Ordinal is this write's position in its source key's set, and is zero
	// for a specification that does not chunk.
	//
	// It is deliberately absent from [ResolveWrite]. The rules that function
	// carries -- a write never crosses generations, a stale answer does not
	// win, a tombstone survives a late update -- have the SOURCE KEY as their
	// subject, and comparing a chunk to whatever held its ordinal before would
	// be comparing two pieces of text that a re-split moved past each other
	// (ADR 0017 section 3.2).
	//
	// InputHash is the source row's whole canonical input for the same reason:
	// a set whose first chunk is unchanged and whose fourth is not must not
	// read as the same work arriving again.
	Ordinal int
}

// WriteKind is what a target write does to the row.
type WriteKind string

const (
	// WriteUpsert stores a vector.
	WriteUpsert WriteKind = "upsert"
	// WriteSkip records that this row has deliberately no vector in this
	// generation. It is a WRITE rather than an omission because verification
	// has to tell a row nobody embedded from a row the specification declined,
	// and silence cannot carry that difference.
	WriteSkip WriteKind = "skip"
	// WriteTombstone records that the source row is gone.
	//
	// A tombstone rather than a delete, because a late retry of an older update
	// must not recreate the row: the tombstone is what that retry loses against.
	WriteTombstone WriteKind = "tombstone"
)

// ResolveWrite decides what a new result does to a row that may already exist.
//
// This is the rule that makes at-least-once transformation safe. The engine may
// deliver the same batch twice, and a request whose answer arrived late may
// carry a version the source has already moved past -- so the decision is made
// against what the target holds, not against what the worker believes.
func ResolveWrite(
	existing *TargetWrite, incoming TargetWrite, order VersionOrder,
) (TargetWrite, bool, error) {
	if incoming.Generation == "" {
		return TargetWrite{}, false, fmt.Errorf("target write: the write names no generation")
	}
	if existing == nil {
		return incoming, true, nil
	}
	if existing.Generation != incoming.Generation {
		// A row belonging to another generation is not this run's to touch.
		// Generations live side by side (Decision 6), and a write that crossed
		// between them would overwrite a corpus somebody is still querying.
		return TargetWrite{}, false, fmt.Errorf(
			"target write: the row belongs to generation %s and the write is for %s",
			existing.Generation, incoming.Generation)
	}
	// A tombstone is terminal within a generation until the source says
	// otherwise with a NEWER version. Without that rule, a retry of an update
	// issued before the delete recreates a row the source no longer has.
	if existing.Kind == WriteTombstone && !newerVersion(incoming.Version, existing.Version, order) {
		return *existing, false, nil
	}
	if olderOrEqual(incoming.Version, existing.Version, order) && incoming.InputHash == existing.InputHash {
		// The same work arriving again. Harmless, and doing nothing is what
		// makes it harmless.
		return *existing, false, nil
	}
	if olderVersion(incoming.Version, existing.Version, order) {
		// A late answer computed from a version the row has moved past. It is
		// not an error -- at-least-once delivery produces these -- and it must
		// not win.
		return *existing, false, nil
	}
	return incoming, true, nil
}

// VersionOrder is how two versions of one row are put in order.
//
// It exists because "compare as opaque strings, by length then
// lexicographically" is right for a counter and WRONG for a timestamp, and the
// comparison cannot tell which it is holding. A driver renders a timestamptz as
// RFC 3339 with trailing zeros trimmed, so an update at 11:00:00.1 renders
// shorter than its predecessor at 10:00:00.123456 and was classified as older:
// the fresh provider answer was discarded, catch-up exited 0 reporting success,
// and the row kept the vector of text it no longer contained. Measured, 9.85%
// of `clock_timestamp()` values render with fewer than six fractional digits
// (stokaro/ptah#2635).
//
// The strategy that produced the version is what decides, so the caller states
// it. The zero value is "not comparable", which is the honest answer for a
// strategy that records no version at all.
type VersionOrder string

// The orders a version can be read under.
const (
	// OrderUnknown is a version nothing can put in order. Neither the
	// no-op check nor the late-answer check fires, so an incoming write wins
	// -- which is the direction that does not lose fresh work.
	OrderUnknown VersionOrder = ""
	// OrderNumeric is a counter or a sequence: a decimal integer.
	OrderNumeric VersionOrder = "numeric"
	// OrderTimestamp is an instant, rendered by whatever the driver chose.
	OrderTimestamp VersionOrder = "timestamp"
)

// newerVersion reports whether left is strictly newer than right, under order.
//
// An empty version on either side is not comparable, and neither is a pair this
// order cannot read: both answer false, which is what keeps this from inventing
// an order for something it does not understand.
func newerVersion(left, right string, order VersionOrder) bool {
	if left == "" || right == "" {
		return false
	}
	switch order {
	case OrderNumeric:
		return numericallyNewer(left, right)
	case OrderTimestamp:
		return instantAfter(left, right)
	default:
		return false
	}
}

// numericallyNewer compares two decimal integers as numbers.
//
// As numbers rather than by length then lexicographically, because the two
// agree only for non-negative integers with no leading zeros -- which is what a
// sequence usually is and not what the comparison can assume it is holding.
func numericallyNewer(left, right string) bool {
	leftValue, leftErr := strconv.ParseInt(strings.TrimSpace(left), 10, 64)
	rightValue, rightErr := strconv.ParseInt(strings.TrimSpace(right), 10, 64)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return leftValue > rightValue
}

// instantAfter compares two rendered timestamps as instants.
//
// Two layouts are read: RFC 3339 with a zone, which is what a driver renders a
// timestamptz as, and the same without one, which is what it renders a plain
// timestamp as. A value neither layout parses is not comparable.
func instantAfter(left, right string) bool {
	leftTime, leftOK := parseInstant(left)
	rightTime, rightOK := parseInstant(right)
	if !leftOK || !rightOK {
		return false
	}
	return leftTime.After(rightTime)
}

// parseInstant reads a rendered timestamp, or reports that it could not.
//
// The layouts are the renderings a version is observed to arrive in, and each
// is here because something produces it:
//
//   - RFC 3339 with a zone, which is what the pgx driver renders a timestamptz
//     as, and the rendering stokaro/ptah#2635 measured;
//   - the same with no zone, for a plain `timestamp` column;
//   - PostgreSQL's own `::text` forms, space-separated, with and without a
//     zone -- a version can reach here through a cast in a view or a
//     generated column rather than off the column itself.
//
// A value none of them parses is not comparable, and the caller treats that as
// "no order", not as "older".
func parseInstant(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, trimmed); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

// olderVersion is newerVersion the other way round.
func olderVersion(left, right string, order VersionOrder) bool {
	return newerVersion(right, left, order)
}

// olderOrEqual reports that left does not come after right.
func olderOrEqual(left, right string, order VersionOrder) bool {
	return left == right || olderVersion(left, right, order)
}
