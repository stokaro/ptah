package embedcatchup

import (
	"fmt"
	"strconv"
	"strings"
)

// Cursor is where catch-up resumes: the first event it has not processed.
//
// It is a pair rather than a transaction identity because the two questions the
// outbox answers are asked in different orders. Whether an event is SETTLED is
// a question about its transaction; where an event sits among its neighbours is
// a question about its sequence. A cursor holding only the transaction has to
// advance to the highest transaction in a page, and a page is a LIMIT -- so an
// unread event sharing that transaction is stepped over and never read again.
// That is stokaro/ptah#2628: at the default page size, an ordinary bulk update
// left committed rows carrying a stale vector, with the outbox reporting
// nothing unprocessed and no later run able to reach them.
//
// The pair makes the page a prefix of a total order. Reading orders by
// (transaction, sequence) and resuming after the last event read cannot skip
// an event, because every event not yet read sorts after that one.
type Cursor struct {
	// Transaction is the transaction identity to resume at.
	Transaction uint64
	// Sequence is the first sequence within that transaction that has not been
	// processed. Zero means the whole transaction is still owed, which is what
	// a boundary between transactions looks like.
	Sequence int64
}

// After is the cursor that resumes immediately past an event.
func After(event Event) Cursor {
	return Cursor{Transaction: event.Transaction, Sequence: event.Sequence + 1}
}

// AtTransaction is the cursor that owes a transaction in full.
func AtTransaction(transaction uint64) Cursor {
	return Cursor{Transaction: transaction}
}

// Before reports whether this cursor resumes earlier than another.
func (c Cursor) Before(other Cursor) bool {
	if c.Transaction != other.Transaction {
		return c.Transaction < other.Transaction
	}
	return c.Sequence < other.Sequence
}

// String renders a cursor for the run record and for an operator to read.
//
// A cursor sitting on a transaction boundary renders as the transaction alone,
// which is what the overwhelming majority of them are and what every catch-up
// that drained its outbox records. A cursor stopped INSIDE a transaction --
// a page filled before that transaction ended -- renders both halves, because
// the difference between "transaction 4446 is next" and "the rest of 4446 is
// next" is the difference the pair exists to keep.
func (c Cursor) String() string {
	if c.Sequence == 0 {
		return strconv.FormatUint(c.Transaction, 10)
	}
	return strconv.FormatUint(c.Transaction, 10) + ":" + strconv.FormatInt(c.Sequence, 10)
}

// ParseCursor reads a cursor back from a run record.
//
// Both spellings String writes are accepted, and nothing else is. A value that
// does not parse is refused rather than defaulted: zero means "every change
// ever recorded", which on a long-lived outbox is a different migration, and
// the highest transaction identity would silently discard the backlog.
func ParseCursor(raw, what string) (Cursor, error) {
	transaction, sequence, split := strings.Cut(raw, ":")
	parsed, err := strconv.ParseUint(transaction, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("the %s %q is not a transaction identity: %w", what, raw, err)
	}
	cursor := Cursor{Transaction: parsed}
	if !split {
		return cursor, nil
	}
	offset, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("the %s %q does not carry a sequence: %w", what, raw, err)
	}
	if offset < 0 {
		return Cursor{}, fmt.Errorf("the %s %q carries a negative sequence", what, raw)
	}
	cursor.Sequence = offset
	return cursor, nil
}
