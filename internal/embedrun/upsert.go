package embedrun

import "fmt"

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
func ResolveWrite(existing *TargetWrite, incoming TargetWrite) (TargetWrite, bool, error) {
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
	if existing.Kind == WriteTombstone && !newerVersion(incoming.Version, existing.Version) {
		return *existing, false, nil
	}
	if olderOrEqual(incoming.Version, existing.Version) && incoming.InputHash == existing.InputHash {
		// The same work arriving again. Harmless, and doing nothing is what
		// makes it harmless.
		return *existing, false, nil
	}
	if olderVersion(incoming.Version, existing.Version) {
		// A late answer computed from a version the row has moved past. It is
		// not an error -- at-least-once delivery produces these -- and it must
		// not win.
		return *existing, false, nil
	}
	return incoming, true, nil
}

// newerVersion reports whether left is strictly newer than right.
//
// Versions are compared as opaque strings by length then lexicographically,
// which orders a monotonic counter and an RFC 3339 timestamp correctly and
// refuses to invent an order for anything else: an empty version on either side
// is not comparable, and this says so by answering false.
func newerVersion(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	if len(left) != len(right) {
		return len(left) > len(right)
	}
	return left > right
}

// olderVersion is newerVersion the other way round.
func olderVersion(left, right string) bool {
	return newerVersion(right, left)
}

// olderOrEqual reports that left does not come after right.
func olderOrEqual(left, right string) bool {
	return left == right || olderVersion(left, right)
}
