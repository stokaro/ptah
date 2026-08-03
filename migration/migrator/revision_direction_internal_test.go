package migrator

// White-box testing required: encodeRevisionState and decodeRevisionState are
// the two halves of what the ptah revision table stores in its state column,
// and the property that matters -- that an up-direction row is stored with the
// bytes it has always been stored with, so every `state = 'applied'` predicate
// keeps matching -- is a statement about the stored value, not about anything a
// caller can observe. Reaching it end to end would need a failing migration per
// state on a live database of each dialect. The behavior these values drive is
// covered as black box in directional_repair_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRevisionStateDirectionRoundTrip pins both halves of the encoding at once:
// what each direction stores, and what every stored value reads back as.
//
// The up rows carry the load here. If encodeRevisionState ever suffixed them,
// every `state = 'applied'` predicate in the metadata SQL would stop matching
// applied rows on every dialect, and this table prints stored "applied:up",
// want "applied".
func TestRevisionStateDirectionRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		state     string
		direction MigrationDirection
		stored    string
	}{
		{name: "applied up is unsuffixed", state: migrationStateApplied, direction: MigrationDirectionUp, stored: "applied"},
		{name: "pending up is unsuffixed", state: migrationStatePending, direction: MigrationDirectionUp, stored: "pending"},
		{name: "failed up is unsuffixed", state: migrationStateFailed, direction: MigrationDirectionUp, stored: "failed"},
		{name: "pending down is suffixed", state: migrationStatePending, direction: MigrationDirectionDown, stored: "pending:down"},
		{name: "failed down is suffixed", state: migrationStateFailed, direction: MigrationDirectionDown, stored: "failed:down"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(encodeRevisionState(test.state, test.direction), qt.Equals, test.stored)
			state, direction := decodeRevisionState(test.stored)
			c.Assert(state, qt.Equals, test.state)
			c.Assert(direction, qt.Equals, test.direction)
		})
	}
}

// TestDecodeRevisionStateTolerates covers values encodeRevisionState never
// writes but a database can still hold: rows from a Ptah that recorded no
// direction, and anything an operator typed into the column by hand. None of
// them may read as a rollback, because reading a rollback is what routes
// --resume-from to the down body.
func TestDecodeRevisionStateTolerates(t *testing.T) {
	tests := []struct {
		name   string
		stored string
		state  string
	}{
		{name: "legacy failed", stored: "failed", state: "failed"},
		{name: "unknown suffix keeps the whole value", stored: "failed:sideways", state: "failed:sideways"},
		{name: "empty suffix keeps the whole value", stored: "failed:", state: "failed:"},
		{name: "empty value", stored: "", state: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			state, direction := decodeRevisionState(test.stored)
			c.Assert(state, qt.Equals, test.state)
			c.Assert(direction, qt.Equals, MigrationDirectionUp)
		})
	}
}
