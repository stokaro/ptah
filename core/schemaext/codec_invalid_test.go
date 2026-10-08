package schemaext_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

func TestInvalidModelErrorRetainsRepresentationIdentity(t *testing.T) {
	c := qt.New(t)
	failure := &schemaext.InvalidModelError{Kind: "example.org/table", Representation: schemaext.Desired, Message: "setting cannot be null"}
	c.Assert(failure, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(failure.Error(), qt.Equals, `desired model "example.org/table": setting cannot be null`)
	received, found := errors.AsType[*schemaext.InvalidModelError](failure)
	c.Assert(found, qt.IsTrue)
	c.Assert(received.Kind, qt.Equals, schemaext.Kind("example.org/table"))
	c.Assert(received.Representation, qt.Equals, schemaext.Desired)
}
