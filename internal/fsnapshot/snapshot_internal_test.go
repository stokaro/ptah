package fsnapshot

// White-box testing required: ownership transfer cannot be observed through
// the immutable public API without mutating data the caller promised to
// relinquish. This test verifies that TakeFiles retains the owned bytes.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestTakeFiles_RetainsOwnedBytes(t *testing.T) {
	c := qt.New(t)
	contents := []byte("SELECT 1;\n")

	snapshot, err := TakeFiles(map[string][]byte{"migration.sql": contents})
	c.Assert(err, qt.IsNil)
	c.Assert(&snapshot.files["migration.sql"][0], qt.Equals, &contents[0])
}
