package embedcatchup

import (
	"slices"
	"time"

	"go.5x5.cz/ptah/internal/embeddigest"
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
	// It is here rather than derived from Sequence because a sequence is
	// allocated before a transaction commits, so two events can commit out of
	// sequence order. See Barrier.
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
func Collapse(events []Event) []Event {
	ordered := slices.Clone(events)
	slices.SortStableFunc(ordered, func(left, right Event) int {
		if left.Transaction != right.Transaction {
			return compareUint64(left.Transaction, right.Transaction)
		}
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

// keyOf renders a key so two of them can be compared as map keys.
//
// Through the same length-prefixed encoder every other content address in the
// lifecycle uses, because a key of ["a", "b.c"] and one of ["a.b", "c"] are
// different rows and a joiner folds them into one -- which, here, means one
// row's change silently deciding another row's vector.
func keyOf(key []string) string {
	return embeddigest.Of(key...)
}

// compareUint64 orders two transaction identities.
func compareUint64(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
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
