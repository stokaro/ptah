// Package viewcheck reads and writes the spelling a Go annotation and a YAML
// document give a view's check option: the `with_check` attribute of
// //ptah:schema:view and the `with_check` key of a YAML view.
//
// The value names the level as well as the presence of the check option, so
// one attribute says what the model holds in
// [ptah.run/core/schemamodel.View.WithCheck] and
// [ptah.run/core/schemamodel.View.LocalCheck]. Both frontends and the Go
// export read and write it here, so a document the export writes reads back
// as the view it was written from.
package viewcheck

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidLevel is the error [Parse] wraps when it refuses a value.
var ErrInvalidLevel = errors.New("invalid view check option")

// Parse reads a with_check value, compared without regard to case or
// surrounding space:
//
//   - empty or "false": no check option;
//   - "true" or "cascaded": WITH CHECK OPTION, which is the CASCADED level,
//     checking a row written through the view against the conditions of the
//     views it reads too;
//   - "local": WITH LOCAL CHECK OPTION, checking a row against the view's own
//     condition alone.
//
// Any other value is refused, wrapping [ErrInvalidLevel]: a mistyped level
// read as no check option would create a view that checks nothing.
func Parse(value string) (withCheck, local bool, err error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "false":
		return false, false, nil
	case "true", "cascaded":
		return true, false, nil
	case "local":
		return true, true, nil
	default:
		return false, false, fmt.Errorf("%w %q: want true, false, cascaded or local", ErrInvalidLevel, value)
	}
}

// Spell is the value [Parse] reads back as withCheck and local: "true" for
// the CASCADED level, "local" for the LOCAL level, and "false" for no check
// option. local means nothing without withCheck.
func Spell(withCheck, local bool) string {
	switch {
	case withCheck && local:
		return "local"
	case withCheck:
		return "true"
	default:
		return "false"
	}
}
