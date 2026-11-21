package parseutils

import (
	"errors"
	"fmt"
)

// ErrNotBoolean is the cause BoolAttribute reports for a written value that
// is neither true nor false.
var ErrNotBoolean = errors.New("not a boolean; write true or false")

// BoolAttribute reads the boolean attribute name from the attributes of one
// directive. present is false when the directive does not write the attribute.
// A written value must be exactly true or false; a bare word the catalog
// declares as a bare boolean reaches here as true. Anything else, an empty
// value or another case included, is refused with ErrNotBoolean rather than
// read as false, because a misspelled true read as false declares the
// opposite of what its author wrote.
func BoolAttribute(attributes map[string]string, name string) (value, present bool, err error) {
	written, present := attributes[name]
	switch {
	case !present:
		return false, false, nil
	case written == "true":
		return true, true, nil
	case written == "false":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("%s=%q is %w", name, written, ErrNotBoolean)
	}
}
