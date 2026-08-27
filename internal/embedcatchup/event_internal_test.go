package embedcatchup

// White-box testing required: the content ratchet enumerates Event's own fields
// and there is nothing exported to enumerate them through.

import (
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestEvent_CarriesNoRowContent is the epic's rule about what an outbox holds.
//
// A change event identifies a key and a version. The moment one carries the row
// it describes, the outbox is a second copy of the corpus -- with a different
// retention policy, a different set of people who can read it, and nobody's
// attention. The field would be added for a good reason, too: rereading the
// source costs a query, and the row is right there.
//
// So the struct is enumerated and a field whose name suggests content fails
// (stokaro/ptah#2068).
func TestEvent_CarriesNoRowContent(t *testing.T) {
	forbidden := []string{
		"content", "text", "body", "payload", "row", "value", "data", "vector", "embedding", "input",
	}
	for _, word := range forbidden {
		t.Run(word, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(fieldsMentioning(word), qt.HasLen, 0, qt.Commentf(
				"an outbox event carries a key and a version; a field holding %s makes it a "+
					"second copy of the corpus", word))
		})
	}
}

// TestEvent_KeepsWhatItNeedsToBeOrdered is the other half.
//
// A ratchet that only forbids things is satisfied by an empty struct. These are
// the fields catch-up cannot work without, and a rename that dropped one would
// otherwise be caught by nothing until a collapse silently reordered.
func TestEvent_KeepsWhatItNeedsToBeOrdered(t *testing.T) {
	c := qt.New(t)

	c.Assert(fieldNames(), qt.DeepEquals,
		[]string{"Sequence", "Transaction", "Key", "Operation", "Version", "At"})
}

// fieldsMentioning lists the Event fields whose name contains a word.
//
// The walk lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func fieldsMentioning(word string) []string {
	var matching []string
	for _, name := range fieldNames() {
		if strings.Contains(strings.ToLower(name), word) {
			matching = append(matching, name)
		}
	}
	return matching
}

// fieldNames lists Event's fields in declaration order.
func fieldNames() []string {
	typ := reflect.TypeFor[Event]()
	names := make([]string, 0, typ.NumField())
	for field := range typ.Fields() {
		names = append(names, field.Name)
	}
	return names
}
