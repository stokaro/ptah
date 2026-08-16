// Package defaultlit decides whether a column default already carries SQL
// literal syntax, so that a renderer quotes it exactly once.
//
// `ast.DefaultValue.Value` has two producers that disagree about the form they
// store, and both are legitimate:
//
//   - struct tags keep the tag verbatim, so `default="active"` arrives bare, as
//     `active`, and has to be quoted before it can be rendered;
//   - the SQL parser keeps the literal as it was written, so
//     `DEFAULT 'active'` arrives as `'active'`, already quoted.
//
// Quoting the second form again is not a cosmetic fault. Measured against
// PostgreSQL 18, a varchar default written in a schema file rendered as
//
//	DEFAULT '''x'''
//
// which applies without an error and stores the three-character value `'x'`
// where `x` was meant. The same treatment of a boolean default is refused
// outright, with `invalid input syntax for type boolean: "'t'"`.
package defaultlit

import "strings"

// IsSQLLiteral reports whether s is already written as a single-quoted SQL
// string literal, optionally followed by a cast.
//
// It walks the literal instead of testing its two ends. Testing the ends is
// enough for a plain `'active'`, but it misclassifies both a literal that
// carries a cast, `'{}'::jsonb`, which does not end in a quote, and a bare
// value that merely happens to start with one.
func IsSQLLiteral(s string) bool {
	if len(s) < 2 || s[0] != '\'' {
		return false
	}

	i := 1
	for i < len(s) {
		if s[i] != '\'' {
			i++
			continue
		}
		// A doubled quote is an escaped quote within the literal, so the
		// literal continues past it.
		if i+1 < len(s) && s[i+1] == '\'' {
			i += 2
			continue
		}
		break
	}
	if i >= len(s) {
		// The opening quote was never closed, so this is not a literal that a
		// renderer can pass through untouched.
		return false
	}

	// Everything after the closing quote has to be a cast. The type name is not
	// checked any further than that: it spans spellings as unlike each other as
	// `jsonb`, `character varying`, `numeric(10,2)` and `text[]`, and the only
	// producer of this shape is the SQL parser, which builds it from a literal
	// it has already recognized.
	rest := s[i+1:]
	return rest == "" || (strings.HasPrefix(rest, "::") && len(rest) > 2)
}

// Render returns value ready to follow `DEFAULT`, quoting it with quote only
// when it is not already a literal.
func Render(value string, quote func(string) string) string {
	trimmed := strings.TrimSpace(value)
	if IsSQLLiteral(trimmed) {
		return trimmed
	}
	return quote(value)
}
