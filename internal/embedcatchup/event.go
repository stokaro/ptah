package embedcatchup

import (
	"slices"
	"time"

	"ptah.run/internal/embeddigest"
)

// Operation is what happened to a source row.
type Operation string

const (
	// OperationInsert is a row that appeared.
	OperationInsert Operation = "insert"
	// OperationUpdate is a row that changed.
	OperationUpdate Operation = "update"
	// OperationDelete is a row that is gone.
	OperationDelete Operation = "delete"
)

// Event is one source change.
//
// It carries a key and a version and no row content, which is the epic's rule
// and is enforced rather than remembered: TestEvent_CarriesNoRowContent
// enumerates this struct and refuses a field that could hold any. An outbox is
// a second copy of the corpus the moment somebody adds a `body` column to it,
// with a different retention policy and nobody's attention.
type Event struct {
	// Sequence orders events within one transaction's worth of changes.
	Sequence int64
	// Transaction is the identity of the transaction that wrote the row.
	//
	// It answers whether an event is SETTLED, and only that. It does not order
	// events: a transaction can take its identity from an earlier write to some
	// other table and reach this source afterwards, by which time a transaction
	// with a later identity has already written and committed here. Ordering by
	// it would put those two the wrong way round. See Barrier for what it is
	// for, and Collapse for what orders.
	Transaction uint64
	// Key is the source key, in the specification's key order.
	Key []string
	// Operation is what happened.
	Operation Operation
	// Version is the source version at the moment of the change, empty under a
	// strategy that establishes none.
	Version string
	// At is when the change was written.
	At time.Time
}

// Collapse folds repeated events for one key into the one that decides its
// fate, keeping the original order of first appearance.
//
// This is the epic's "updates may be collapsed by key by rereading current
// source state", made explicit. Five updates to one row during a backfill are
// five provider requests for four vectors nobody will ever read, and the fifth
// is the only one the target should hold.
//
// A delete does not collapse into an update: it is the last word about a key
// regardless of what came before it, and an update that arrived earlier
// describes a row that no longer exists. A later insert reopens it, which is
// why the LAST event wins rather than the delete specifically.
//
// Last is by SEQUENCE, which is when the row was written, and not by
// transaction identity, which is only when a transaction first needed one.
// Writes to a single row are serialized by that row's lock, so for the events
// that can contradict each other -- the events about one key -- the sequence is
// the order they happened in.
func Collapse(events []Event) []Event {
	ordered := slices.Clone(events)
	slices.SortStableFunc(ordered, func(left, right Event) int {
		return compareInt64(left.Sequence, right.Sequence)
	})

	last := make(map[string]Event, len(ordered))
	var order []string
	for _, event := range ordered {
		identity := keyOf(event.Key)
		if _, seen := last[identity]; !seen {
			order = append(order, identity)
		}
		last[identity] = event
	}

	collapsed := make([]Event, 0, len(order))
	for _, identity := range order {
		collapsed = append(collapsed, last[identity])
	}
	return collapsed
}

// KeyIdentity renders a key so two of them can be compared as map keys.
//
// Exported because catch-up outside this package has to agree with the collapse
// inside it about which events are about one row. Two answers to that would let
// a row be tombstoned by a change to its neighbour.
func KeyIdentity(key []string) string {
	return keyOf(key)
}

// keyOf renders a key so two of them can be compared as map keys.
//
// Through the same length-prefixed encoder every other content address in the
// lifecycle uses, because a key of ["a", "b.c"] and one of ["a.b", "c"] are
// different rows and a joiner folds them into one -- which, here, means one
// row's change silently deciding another row's vector.
func keyOf(key []string) string {
	return embeddigest.Of(key...)
}

// compareInt64 orders two sequences.
func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
