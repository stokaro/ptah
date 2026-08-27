package embedcatchup

import (
	"fmt"
)

// Barrier is the completion condition an outbox has to satisfy.
//
// The three watermarks are transaction identities rather than sequence numbers,
// and that is the whole design rather than a detail. A sequence is allocated
// when a row is inserted and a transaction becomes visible when it commits, so
// two events routinely commit out of sequence order: a reader that advanced a
// sequence cursor past a committed event would step over an earlier one still
// in flight, and that event is then never processed by anything, ever.
//
// A transaction identity below the snapshot's own minimum is one that has
// concluded -- committed and visible, or aborted and gone. Nothing can appear
// below it afterwards, which is what makes advancing past it safe.
type Barrier struct {
	// Snapshot is the boundary recorded before the backfill began. Every
	// change from this transaction onward is catch-up's to process.
	Snapshot uint64
	// Processed is how far catch-up has got.
	Processed uint64
	// Horizon is the current boundary below which every transaction has
	// concluded. It is context for a reader rather than a target to reach: see
	// Reached.
	Horizon uint64
	// Unprocessed counts the events between Processed and Horizon that catch-up
	// has not handled.
	Unprocessed int
	// Installed reports whether the outbox mechanism is actually in place.
	Installed bool
}

// Reached reports whether catch-up has processed everything it can, and why not.
func (b Barrier) Reached() (bool, []string) {
	var blockers []string
	if !b.Installed {
		// A mode selected and not installed is the worst of both: the operator
		// believes changes are being captured, and the table has no trigger on
		// it.
		blockers = append(blockers, "the outbox is not installed on the source, so no change was ever captured")
		return false, blockers
	}
	if b.Snapshot == 0 {
		blockers = append(blockers,
			"no snapshot boundary was recorded, so there is nothing to say which changes catch-up owes")
	}
	if b.Processed < b.Snapshot {
		blockers = append(blockers, fmt.Sprintf(
			"catch-up has reached transaction %d and the backfill's boundary is %d, so changes "+
				"between them are unprocessed", b.Processed, b.Snapshot))
	}
	if b.Unprocessed > 0 {
		blockers = append(blockers, fmt.Sprintf("%d source changes are unprocessed", b.Unprocessed))
	}
	// There is deliberately no rule that the processed watermark has to equal
	// the horizon. It never does on a live server: the horizon moves with every
	// transaction anywhere in the database, and asking a catch-up to catch up
	// to transactions that wrote nothing to the source is asking it to chase
	// something that recedes. This was such a rule until it met one -- it
	// refused a caught-up run over two transactions the reads in the check
	// itself had started.
	//
	// What being caught up means is that nothing between the watermark and the
	// horizon is unprocessed, which is the count above. Recording how far
	// catch-up got is a separate concern and belongs to the loop, which moves
	// its watermark on an empty page for exactly that reason.
	return len(blockers) == 0, blockers
}

// assessOutbox holds the outbox mode to its barrier.
func assessOutbox(guarantee *Guarantee, barrier Barrier) {
	reached, blockers := barrier.Reached()
	if !reached {
		guarantee.Blockers = append(guarantee.Blockers, blockers...)
	}
}
