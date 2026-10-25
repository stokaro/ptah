package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// Each spelling is asked for once, in the order it was first written, and
// none names a subject: only the owner reads the spelling.
func TestWrittenRequests(t *testing.T) {
	c := qt.New(t)

	requests := schemaext.WrittenRequests(schemaext.RotateAction, []string{"b", "a", "b", "c", "a"})

	c.Assert(requests, qt.DeepEquals, []schemaext.ChangeRequest{
		{Action: schemaext.RotateAction, Written: "b"},
		{Action: schemaext.RotateAction, Written: "a"},
		{Action: schemaext.RotateAction, Written: "c"},
	})
	c.Assert(schemaext.WrittenRequests(schemaext.RotateAction, nil), qt.HasLen, 0)
}
