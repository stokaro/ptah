// Package atlasscript implements the Atlas Scripts surface on the
// compatibility binary: the `script` HCL grammar and the verbs that run it.
//
// The surface is reproduced from publicly documented behavior. Where that
// material is silent the verb refuses by name rather than guessing, which is
// the rule the rest of this tree follows for a form it cannot express: a
// wrong answer that runs is worse than a refusal that does not
// (stokaro/ptah#1017).
package atlasscript

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// MaskMethod is how a mask rewrites a value.
type MaskMethod string

const (
	// MaskRedact replaces the whole value with a token.
	MaskRedact MaskMethod = "REDACT"
	// MaskPartial keeps a suffix and redacts the rest.
	MaskPartial MaskMethod = "PARTIAL"
	// MaskHash replaces the value with a keyed digest of it.
	MaskHash MaskMethod = "HASH"
	// MaskReplace rewrites the parts of the value a pattern matches.
	MaskReplace MaskMethod = "REPLACE"
)

// DefaultRedactToken is what REDACT writes when the mask names no token.
const DefaultRedactToken = "***"

// Mask is one rule.
type Mask struct {
	// Name is the mask's own name when it was declared as a reusable
	// `mask "<name>"` block, and empty for one written inline.
	Name string
	// Method selects the rewrite.
	Method MaskMethod
	// Columns are the column names this mask applies to. Empty means every
	// column, which is what an unqualified mask inside a query means.
	Columns []string
	// Token is REDACT's replacement. Empty means [DefaultRedactToken].
	Token string
	// KeepRight is how many trailing characters PARTIAL keeps.
	KeepRight int
	// Salt keys HASH. A hash with no salt is still deterministic, and that is
	// the point of the field: two runs with the same salt agree, and two
	// deployments with different salts do not correlate.
	Salt string
	// Match is REPLACE's pattern and With its replacement.
	Match string
	With  string

	// matcher is Match compiled once, so a mask applied to a million rows
	// compiles its pattern once rather than a million times.
	matcher *regexp.Regexp
}

// Compile validates a mask and prepares it for use.
//
// It is separate from applying so a script is refused before it touches a
// database rather than halfway through a result set: a REPLACE whose pattern
// does not compile is a broken script, and finding that out on row 400,000 is
// finding it out too late.
func (m *Mask) Compile() error {
	switch m.Method {
	case MaskRedact:
		return nil
	case MaskPartial:
		if m.KeepRight < 0 {
			return fmt.Errorf("mask %s: keep_right is negative", m.describe())
		}
		return nil
	case MaskHash:
		return nil
	case MaskReplace:
		if m.Match == "" {
			return fmt.Errorf("mask %s: REPLACE has no match pattern", m.describe())
		}
		matcher, err := regexp.Compile(m.Match)
		if err != nil {
			return fmt.Errorf("mask %s: match pattern: %w", m.describe(), err)
		}
		m.matcher = matcher
		return nil
	default:
		return fmt.Errorf("mask %s: unknown method %q", m.describe(), string(m.Method))
	}
}

// Apply rewrites one value.
func (m *Mask) Apply(value string) string {
	switch m.Method {
	case MaskRedact:
		return m.redactToken()
	case MaskPartial:
		return m.partial(value)
	case MaskHash:
		return m.hash(value)
	case MaskReplace:
		if m.matcher == nil {
			// Compile refuses an uncompiled REPLACE, so reaching here means a
			// caller skipped it. Returning the value unchanged would leak it.
			return m.redactToken()
		}
		return m.matcher.ReplaceAllString(value, m.With)
	default:
		return m.redactToken()
	}
}

// Covers reports whether this mask applies to a column.
//
// A mask naming no column covers every column. Comparison is
// case-insensitive, because a result set reports a column in whatever case the
// query produced and a mask is written in the author's.
func (m *Mask) Covers(column string) bool {
	if len(m.Columns) == 0 {
		return true
	}
	for _, candidate := range m.Columns {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(column)) {
			return true
		}
	}
	return false
}

func (m *Mask) redactToken() string {
	if m.Token != "" {
		return m.Token
	}
	return DefaultRedactToken
}

// partial keeps the last KeepRight characters and redacts what precedes them.
//
// Counted in runes rather than bytes: keeping "the last four characters" of a
// value that is not ASCII would otherwise cut a character in half and emit
// invalid UTF-8 into a report.
func (m *Mask) partial(value string) string {
	runes := []rune(value)
	if m.KeepRight <= 0 {
		return m.redactToken()
	}
	if m.KeepRight >= len(runes) {
		// Keeping at least as much as there is would return the value
		// unchanged, which is a mask that does nothing. The whole value is
		// redacted instead: a short value is the case where a partial mask
		// protects least and matters most.
		return m.redactToken()
	}
	return m.redactToken() + string(runes[len(runes)-m.KeepRight:])
}

// hash is a keyed HMAC-SHA256 of the value, hex encoded.
//
// Keyed rather than a bare digest: an unkeyed hash of a low-cardinality column
// -- a national identifier, a postcode, a date of birth -- is reversible by
// anybody willing to hash the candidates, so a bare SHA-256 would publish the
// column it was asked to protect.
func (m *Mask) hash(value string) string {
	mac := hmac.New(sha256.New, []byte(m.Salt))
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *Mask) describe() string {
	if m.Name != "" {
		return fmt.Sprintf("%q", m.Name)
	}
	return "(inline)"
}

// MaskSet is the masks a query applies, in declaration order.
type MaskSet []Mask

// Compile prepares every mask, and reports the first that is broken.
func (s MaskSet) Compile() error {
	for index := range s {
		if err := s[index].Compile(); err != nil {
			return err
		}
	}
	return nil
}

// Apply rewrites one column's value with the FIRST mask that covers it.
//
// Declaration order, first match winning, is the documented rule and it is not
// interchangeable with last-match: a script that redacts one column and then
// declares a broad partial mask expects the narrow rule to hold. Applying both
// in turn would also be wrong -- hashing a redacted value publishes a digest of
// the token rather than of the datum, which is a different value that looks
// like a mask working.
func (s MaskSet) Apply(column, value string) string {
	for index := range s {
		if s[index].Covers(column) {
			return s[index].Apply(value)
		}
	}
	return value
}
