package renderer

import (
	"fmt"
	"strings"
)

// Annotation is non-executable prose positioned before an execution unit.
// Before equals the unit count for a trailing annotation, including an empty
// plan. Entries at the same position retain their authored order.
type Annotation struct {
	Before int    `json:"before"`
	Text   string `json:"text"`
}

// ValidateAnnotations refuses unordered or out-of-range positions. Annotation
// text has no effect on execution, transaction requirements, or schema identity.
func ValidateAnnotations(annotations []Annotation, units int) error {
	previous := 0
	for i, annotation := range annotations {
		if annotation.Before < previous || annotation.Before > units {
			return fmt.Errorf("%w: annotation %d has invalid position %d for %d units", ErrInvalidResult, i, annotation.Before, units)
		}
		previous = annotation.Before
	}
	return nil
}

// Comment projects prose to SQL line comments. Every physical line receives a
// prefix, so multiline text and block-comment delimiters cannot introduce SQL.
func Comment(text string) string {
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	var out strings.Builder
	for line := range strings.SplitSeq(text, "\n") {
		out.WriteString("--")
		if line != "" {
			out.WriteByte(' ')
			out.WriteString(line)
		}
		out.WriteByte('\n')
	}
	return out.String()
}
