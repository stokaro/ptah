// Package ydbsyntax provides YQL identifier and string literal spelling for
// YDB-owned implementations. It has no dependency on Ptah host internals.
package ydbsyntax

import (
	"fmt"
	"strings"
)

var identifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// QuoteIdentifier quotes one identifier verbatim, without splitting paths or
// folding case. YQL reads backslash escapes inside backticks, so both backslashes
// and backticks are escaped. An empty name produces an empty quoted identifier;
// name validity belongs to the operation that uses it.
func QuoteIdentifier(name string) string {
	return "`" + identifierEscaper.Replace(name) + "`"
}

// StringLiteral writes arbitrary bytes as a single-quoted YQL string. It escapes
// quotes, backslashes, and control bytes without changing UTF-8 or binary data.
// Unlike SQL quote doubling, this spelling follows YQL's backslash grammar.
func StringLiteral(value string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c == '\\' || c == '\'':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
