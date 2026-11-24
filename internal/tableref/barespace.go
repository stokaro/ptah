package tableref

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// ErrSpacedBareName is what [SpacedBareName] returns: a name written bare
// begins or ends with white space, which the reader removes.
var ErrSpacedBareName = errors.New("a bare name begins or ends with white space")

// BareSpace reports whether white space begins or ends value beside a part
// written bare: one that does not begin with a byte of opening, at the start,
// or end with a byte of closing, at the end. A reader that trims a value it
// reads removes that space, so the name read is not the one written, and a
// catalog holds " t" and t as two tables. Space beside a quoted part is the
// spelling of the value and is not reported, nor is a value of white space
// alone, which names nothing.
func BareSpace(value, opening, closing string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	leading := len(strings.TrimLeftFunc(value, unicode.IsSpace)) != len(value)
	trailing := len(strings.TrimRightFunc(value, unicode.IsSpace)) != len(value)
	return leading && !strings.ContainsRune(opening, rune(trimmed[0])) ||
		trailing && !strings.ContainsRune(closing, rune(trimmed[len(trimmed)-1]))
}

// DocumentName is the name value declares in a document, such as a table entry
// or a schema field of a YAML schema. A document is text an author typed, so
// the space around the name is removed, and a name written as one quoted
// identifier in double quotes, '" t"', is the name inside the quotes, bytes
// and all. PostgreSQL holds " t" and t as two tables, and the quoted spelling
// is how a document names the first: kept with its quotes, it would name a
// third table, """ t""".
//
// A bare name that begins or ends with white space is refused with
// [SpacedBareName] of what: read without its space, `name: " t"` would name
// t, and a plan would create t and drop the live " t" the author meant. Only
// double quotes make a name quoted, so `[orders]` and a name in backticks are
// bare names that keep their brackets and backticks.
func DocumentName(what, value string) (string, error) {
	if BareSpace(value, `"`, `"`) {
		return "", SpacedBareName(what, value)
	}
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, `"`) {
		return trimmed, nil
	}
	if ref, ok := Parse(trimmed); ok && !ref.Qualified {
		return ref.Name, nil
	}
	return trimmed, nil
}

// SpacedBareName returns the refusal of value, a name [BareSpace] reports,
// which what names, such as "table name". The refusal says how to keep the
// space: the name written as one quoted identifier. It matches
// [ErrSpacedBareName].
func SpacedBareName(what, value string) error {
	return spacedBareNameError{what: what, value: value}
}

type spacedBareNameError struct {
	what, value string
}

func (e spacedBareNameError) Error() string {
	remedy := "quote the part that holds the space to keep it, or remove the space"
	if !strings.Contains(e.value, ".") {
		remedy = fmt.Sprintf("write it as one quoted identifier, %s, or remove the space", Canonical("", e.value))
	}
	return fmt.Sprintf("%s %q begins or ends with white space, which a bare name loses: %s", e.what, e.value, remedy)
}

func (e spacedBareNameError) Is(target error) bool {
	return target == ErrSpacedBareName
}
