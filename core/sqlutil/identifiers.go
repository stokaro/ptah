package sqlutil

import "strings"

var yqlIdentifierEscaper = strings.NewReplacer(`\`, `\\`, "`", "\\`")

// QuoteYQLIdentifier quotes one YQL identifier verbatim, without splitting a
// path or folding case. YQL reads backslash escapes inside backticks, so both
// a backslash and a backtick are escaped; doubling the backtick, as MySQL
// does, would end the name early. An empty name produces an empty quoted
// identifier; whether a name is valid is the caller's question.
func QuoteYQLIdentifier(name string) string {
	return "`" + yqlIdentifierEscaper.Replace(name) + "`"
}
