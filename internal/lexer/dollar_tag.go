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
	// position: ClickHouse 26.10 reads `$1abc$` as a heredoc. A character
	// outside ASCII is a tag character as ClickHouse 24.10 reads one; 26.10
	// refuses such a tag with code 62, a syntax error. A tag that no closing
	// tag follows opens nothing, so `SELECT 1 AS $ab$` names a column and the
	// statement ends at the next semicolon.
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
// [NameStart].
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

// NameStart reports whether b, read where no token is open, begins a name: an
// ASCII letter, an underscore or a byte above 0x7F. The name runs to
// [NameEnd], and a dollar sign inside it opens no dollar-quoted string:
// PostgreSQL 19 and ClickHouse 26.10 both read `SELECT 1 AS a$b$, 2 AS c` as
// two columns, the first named a$b$, and so read `a1$b$` and `_1$b$`.
//
// A digit begins a number rather than a name, so a dollar sign right after one
// can open a string: PostgreSQL 19 and CockroachDB v26.4.1 read
// `SELECT 1$a$x$a$` as the number 1 and the string $a$x$a$, and ClickHouse
// 26.10 and 24.10 read a heredoc there. So can a dollar sign right after a
// closing tag: ClickHouse 26.10 reads `$a$p$a$$b$q$b$` as two heredocs.
//
// A dollar sign that opens no string begins a name when a name byte, a digit
// or another dollar sign follows it, and that name runs to [NameEnd] from the
// byte after the sign: ClickHouse 26.10 reads `$1$b$x$b$`, where nothing closes
// `$1$`, as one name. This is how the lexer reads a text, and a reader that
// walks a text byte by byte follows it by skipping each name whole.
func NameStart(b byte) bool {
	return b == '_' || 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || b >= utf8.RuneSelf
}

// NameEnd returns the offset of the first byte at or after pos that does not
// continue a name: an ASCII letter, a digit, an underscore, a dollar sign or a
// byte above 0x7F continues one. It returns pos where text[pos] continues
// none, and len(text) where the name runs to the end. See [NameStart] for
// where a name begins.
func NameEnd(text string, pos int) int {
	for pos < len(text) {
		switch b := text[pos]; {
		case NameStart(b), b == '$', '0' <= b && b <= '9':
			pos++
		default:
			return pos
		}
	}
	return pos
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
