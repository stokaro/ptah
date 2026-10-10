package schemaext

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ValidText refuses text a statement or a catalog cannot carry faithfully:
// invalid UTF-8, and a NUL byte, which C-string APIs and several catalogs end
// a value at. field names the value in the refusal, such as "security policy
// struct name". The refusal wraps [ErrInvalidValue]; an empty value is valid
// text, so whether one is allowed is the caller's rule.
func ValidText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is not valid UTF-8", ErrInvalidValue, field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: %s contains a NUL byte", ErrInvalidValue, field)
	}
	return nil
}
