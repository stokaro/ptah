package lexer

import "unicode/utf8"

// DollarTagRule is how an engine spells the tag of a dollar-quoted string, the
// text between the dollar signs of `$tag$`. The lexer reads it from
// [Options.DollarTags], and internal/clientdelimiter reads the same rule, so
// the statement splitter and the client delimiter rewrite agree on where a
// body opens.
type DollarTagRule uint8

const (
	// PostgreSQLDollarTags is the PostgreSQL family's rule. A tag holds ASCII
	// letters, underscores and any byte above 0x7F, so any character outside
	// ASCII, and a digit after the first: `$1abc$` is the parameter `$1`
	// followed by a name. PostgreSQL 19, CockroachDB v26.4.1, YugabyteDB
	// 2026.1.2 and psql 19 read it so. A tag that no closing tag follows
	// still opens a string, which runs to the end of the text, since the
	// server refuses it as an unterminated dollar-quoted string. It is the
	// zero value.
	PostgreSQLDollarTags DollarTagRule = iota
	// HeredocDollarTags is the rule of a ClickHouse heredoc. A tag holds ASCII
	// letters, digits, underscores and any byte above 0x7F, a digit in any
	// position: ClickHouse 26.10 reads `$1abc$` as a heredoc. A tag that no
	// closing tag follows opens nothing, so `SELECT 1 AS $ab$` names a column
	// and the statement ends at the next semicolon.
	HeredocDollarTags
)

// OpeningTag reports the length of the tag that opens a dollar-quoted string
// at the start of text, both dollar signs included, so `$$` has length 2. It
// reports false where text does not start with a tag the rule allows. Whether
// a closing tag follows is the caller's question.
func (rule DollarTagRule) OpeningTag(text string) (int, bool) {
	if text == "" || text[0] != '$' {
		return 0, false
	}
	for pos := 1; pos < len(text); pos++ {
		if text[pos] == '$' {
			return pos + 1, true
		}
		if !rule.allows(text[pos], pos == 1) {
			return 0, false
		}
	}
	return 0, false
}

// allows reports whether b may stand in a tag, first saying it would be the
// tag's first byte. A character outside ASCII is a run of bytes above 0x7F,
// so reading bytes is reading characters here.
func (rule DollarTagRule) allows(b byte, first bool) bool {
	switch {
	case b == '_', 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', b >= utf8.RuneSelf:
		return true
	case '0' <= b && b <= '9':
		return rule == HeredocDollarTags || !first
	default:
		return false
	}
}
