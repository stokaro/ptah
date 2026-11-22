package lexer

import (
	"strings"
	"unicode/utf8"
)

// DollarTagRule is how an engine spells the tag of a dollar-quoted string, the
// text between the dollar signs of `$tag$`. The lexer reads it from
// [Options.DollarTags], and every other reader of dollar-quoted strings reads
// it through [DollarQuotes], so the statement splitter, the client delimiter
// rewrite, the lint scanner and the dependency order agree on where a body
// opens and ends. internal/dialectlexer maps a dialect's descriptor to its
// rule.
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

// DollarQuotes reads where the dollar-quoted strings of one text open and end
// under one [DollarTagRule]. The first time a tag needs a closer it indexes the
// last offset each tag starts at over the whole text, so a text holding
// thousands of tags that never close is read in linear time, where a search of
// the rest of the text for each of them would take quadratic time. A
// DollarQuotes reads one text from one goroutine.
type DollarQuotes struct {
	rule DollarTagRule
	text string
	last map[string]int
}

// NewDollarQuotes returns a reader of the dollar-quoted strings of text under
// rule.
func NewDollarQuotes(rule DollarTagRule, text string) *DollarQuotes {
	return &DollarQuotes{rule: rule, text: text}
}

// Opening reports the length of the tag that opens a dollar-quoted string at
// offset pos of the text, both dollar signs included. Under
// [PostgreSQLDollarTags] it is [DollarTagRule.OpeningTag]. Under
// [HeredocDollarTags] the same tag must also start again at or after the end
// of this one, since ClickHouse reads a tag nothing closes as part of a name.
// Whether the dollar sign starts a token is the caller's question; see
// [DollarInName].
func (q *DollarQuotes) Opening(pos int) (int, bool) {
	length, ok := q.rule.OpeningTag(q.text[pos:])
	if !ok || q.rule != HeredocDollarTags {
		return length, ok
	}
	if !q.Closes(q.text[pos:pos+length], pos+length) {
		return 0, false
	}
	return length, true
}

// End reports the offset just past the dollar-quoted string that opens at
// offset pos of the text, its closing tag included, and false where
// [DollarQuotes.Opening] opens none there. A string no closing tag follows
// ends at the end of the text, which only [PostgreSQLDollarTags] opens.
func (q *DollarQuotes) End(pos int) (int, bool) {
	length, ok := q.Opening(pos)
	if !ok {
		return 0, false
	}
	tag := q.text[pos : pos+length]
	closing := strings.Index(q.text[pos+length:], tag)
	if closing < 0 {
		return len(q.text), true
	}
	return pos + 2*length + closing, true
}

// Closes reports whether tag, a tag the rule allows, starts at an offset of the
// text at or after from. The start may be inside a string or a comment: an
// engine searches for the closing tag without reading what lies between.
func (q *DollarQuotes) Closes(tag string, from int) bool {
	if q.last == nil {
		q.last = make(map[string]int)
		for pos := range len(q.text) {
			if q.text[pos] != '$' {
				continue
			}
			if length, found := q.rule.OpeningTag(q.text[pos:]); found {
				q.last[q.text[pos:pos+length]] = pos
			}
		}
	}
	at, found := q.last[tag]
	return found && at >= from
}

// DollarInName reports whether the dollar sign at text[pos] stands inside a
// name, right after an ASCII letter, a digit, an underscore, a byte above 0x7F
// or another dollar sign, where it opens no dollar-quoted string. PostgreSQL
// 19 and ClickHouse 26.10 both read `SELECT 1 AS a$b$, 2 AS c` as two
// columns, the first named a$b$, and end the statement at the next semicolon.
func DollarInName(text string, pos int) bool {
	if pos == 0 {
		return false
	}
	switch b := text[pos-1]; {
	case b == '_', b == '$', '0' <= b && b <= '9', 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', b >= utf8.RuneSelf:
		return true
	default:
		return false
	}
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
