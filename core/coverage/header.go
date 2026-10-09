package coverage

import (
	"iter"
	"strings"
)

// HeaderComments yields trimmed line-comment bodies before the first content
// line. Blank lines do not end the header. Common and owned coverage codecs
// share this boundary so a string inside a schema cannot become a directive.
func HeaderComments(document string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for line := range strings.SplitSeq(document, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			body, ok := commentBody(trimmed)
			if !ok || !yield(body) {
				return
			}
		}
	}
}
