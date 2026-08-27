package embedcatchup_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcatchup"
)

// change is one source change, spelled short so a table of them reads.
func change(transaction uint64, sequence int64, key string, operation embedcatchup.Operation) embedcatchup.Event {
	return embedcatchup.Event{
		Transaction: transaction, Sequence: sequence, Key: []string{key},
		Operation: operation, Version: "v", At: now,
	}
}

// summarize renders collapsed events for an assertion.
func summarize(events []embedcatchup.Event) []string {
	rendered := make([]string, 0, len(events))
	for _, event := range events {
		rendered = append(rendered, event.Key[0]+":"+string(event.Operation))
	}
	return rendered
}

// TestCollapse_RepeatedUpdatesBecomeOne is the epic's "updates may be collapsed
// by key".
//
// Five updates to one row during a backfill are five provider requests for four
// vectors nobody will ever read.
func TestCollapse_RepeatedUpdatesBecomeOne(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(10, 1, "a", embedcatchup.OperationUpdate),
		change(11, 2, "a", embedcatchup.OperationUpdate),
		change(12, 3, "a", embedcatchup.OperationUpdate),
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(collapsed, qt.HasLen, 1)
	c.Assert(collapsed[0].Transaction, qt.Equals, uint64(12))
}

// TestCollapse_TheLastWordAboutAKeyWins walks what a sequence of operations on
// one key means.
//
// A delete does not win because it is a delete: it wins when it is last. A row
// deleted and re-inserted during a backfill exists, and folding to the delete
// would leave a tombstone over a live row -- which verification then reports as
// a coverage gap forever.
func TestCollapse_TheLastWordAboutAKeyWins(t *testing.T) {
	tests := []struct {
		name   string
		events []embedcatchup.Event
		want   string
	}{
		{
			name: "updated then deleted",
			events: []embedcatchup.Event{
				change(10, 1, "a", embedcatchup.OperationUpdate),
				change(11, 2, "a", embedcatchup.OperationDelete),
			},
			want: "a:delete",
		},
		{
			name: "deleted then inserted again",
			events: []embedcatchup.Event{
				change(10, 1, "a", embedcatchup.OperationDelete),
				change(11, 2, "a", embedcatchup.OperationInsert),
			},
			want: "a:insert",
		},
		{
			name: "inserted then updated",
			events: []embedcatchup.Event{
				change(10, 1, "a", embedcatchup.OperationInsert),
				change(11, 2, "a", embedcatchup.OperationUpdate),
			},
			want: "a:update",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			collapsed := embedcatchup.Collapse(test.events)

			c.Assert(summarize(collapsed), qt.DeepEquals, []string{test.want})
		})
	}
}

// TestCollapse_OrdersByTransactionAndNotByArrival is what makes the rule above
// mean anything.
//
// Events come back in whatever order a query returned them. A collapse that
// took the last one it happened to see would resolve a delete-then-insert to a
// delete about half the time, and the half it got wrong leaves a tombstone over
// a live row.
func TestCollapse_OrdersByTransactionAndNotByArrival(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(12, 3, "a", embedcatchup.OperationInsert),
		change(10, 1, "a", embedcatchup.OperationDelete),
		change(11, 2, "a", embedcatchup.OperationUpdate),
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(summarize(collapsed), qt.DeepEquals, []string{"a:insert"})
}

// TestCollapse_TwoEventsInOneTransactionAreOrderedBySequence is the tiebreak.
//
// A transaction that touches one row twice writes two events with one
// transaction identity, and only the sequence separates them.
func TestCollapse_TwoEventsInOneTransactionAreOrderedBySequence(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(10, 2, "a", embedcatchup.OperationDelete),
		change(10, 1, "a", embedcatchup.OperationUpdate),
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(summarize(collapsed), qt.DeepEquals, []string{"a:delete"})
}

// TestCollapse_DifferentKeysAreNotCollapsedIntoEachOther is the control.
func TestCollapse_DifferentKeysAreNotCollapsedIntoEachOther(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(10, 1, "a", embedcatchup.OperationUpdate),
		change(11, 2, "b", embedcatchup.OperationDelete),
		change(12, 3, "a", embedcatchup.OperationDelete),
		change(13, 4, "c", embedcatchup.OperationInsert),
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(summarize(collapsed), qt.DeepEquals, []string{"a:delete", "b:delete", "c:insert"})
}

// TestCollapse_ACompositeKeyCannotBorrowAnotherRowsBoundary is why the key is
// length-prefixed rather than joined.
//
// Two rows keyed ["a", "b.c"] and ["a.b", "c"] are different rows, and a joiner
// folds them into one -- which here means one row's delete deciding another
// row's vector.
func TestCollapse_ACompositeKeyCannotBorrowAnotherRowsBoundary(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		{Transaction: 10, Sequence: 1, Key: []string{"a", "b.c"},
			Operation: embedcatchup.OperationUpdate, At: now},
		{Transaction: 11, Sequence: 2, Key: []string{"a.b", "c"},
			Operation: embedcatchup.OperationDelete, At: now},
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(collapsed, qt.HasLen, 2)
	c.Assert(collapsed[0].Operation, qt.Equals, embedcatchup.OperationUpdate)
	c.Assert(collapsed[1].Operation, qt.Equals, embedcatchup.OperationDelete)
}

// TestCollapse_KeepsTheOrderOfFirstAppearance keeps two runs over one batch
// producing one plan.
func TestCollapse_KeepsTheOrderOfFirstAppearance(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(10, 1, "b", embedcatchup.OperationUpdate),
		change(11, 2, "a", embedcatchup.OperationUpdate),
		change(12, 3, "b", embedcatchup.OperationUpdate),
	}

	first := embedcatchup.Collapse(events)
	second := embedcatchup.Collapse(events)

	c.Assert(summarize(first), qt.DeepEquals, []string{"b:update", "a:update"})
	c.Assert(summarize(first), qt.DeepEquals, summarize(second))
}

// TestCollapse_DoesNotReorderTheCallersSlice keeps a caller's own record of
// what arrived intact.
//
// The events are also what gets checkpointed. Sorting them in place would make
// the audit trail agree with the collapse rather than with the source.
func TestCollapse_DoesNotReorderTheCallersSlice(t *testing.T) {
	c := qt.New(t)
	events := []embedcatchup.Event{
		change(12, 3, "a", embedcatchup.OperationInsert),
		change(10, 1, "a", embedcatchup.OperationDelete),
	}

	embedcatchup.Collapse(events)

	c.Assert(events[0].Transaction, qt.Equals, uint64(12))
	c.Assert(events[1].Transaction, qt.Equals, uint64(10))
}

// TestCollapse_NothingIsNothing pins the empty case.
func TestCollapse_NothingIsNothing(t *testing.T) {
	c := qt.New(t)

	c.Assert(embedcatchup.Collapse(nil), qt.HasLen, 0)
}

// TestEvent_AtIsCarriedRatherThanInvented keeps a collapsed event pointing at
// the change it actually is.
func TestEvent_AtIsCarriedRatherThanInvented(t *testing.T) {
	c := qt.New(t)
	later := now.Add(time.Hour)
	events := []embedcatchup.Event{
		change(10, 1, "a", embedcatchup.OperationUpdate),
		{Transaction: 11, Sequence: 2, Key: []string{"a"},
			Operation: embedcatchup.OperationDelete, At: later},
	}

	collapsed := embedcatchup.Collapse(events)

	c.Assert(collapsed[0].At, qt.Equals, later)
}
