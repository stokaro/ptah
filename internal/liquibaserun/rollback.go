package liquibaserun

import (
	"fmt"
	"strings"
)

// Rollback is a formatted-SQL changeset's rollback, built the way Liquibase's
// parser builds it from `--rollback` lines and `/* liquibase rollback` blocks,
// in the order they appear. The zero value is a changeset that writes no
// rollback.
//
// Liquibase adds a `--rollback` line's SQL and then a line break, and adds a
// block's lines, and the text before its `*/`, with nothing between them. So
// `DELETE FROM t` and `WHERE id = 1;` on two lines of a block run as
// `DELETE FROM tWHERE id = 1;`, and a block runs into a `--rollback` line that
// follows it. Measured with Liquibase 5.0.4 on SQLite: rollback-count ran
// exactly that text, and failed on it.
type Rollback struct {
	// text is the rollback as Liquibase holds it.
	text string
	// joins are the places where Liquibase put two pieces of text side by side
	// with nothing between them, where the author wrote a line break.
	joins []rollbackJoin
	// last is the last piece added that is not blank, and lastAt the offset in
	// text where it begins: the first half of the next join.
	last   string
	lastAt int
}

// rollbackJoin is one place where Liquibase ran two lines together.
type rollbackJoin struct {
	// at is the offset in the text where the second piece begins.
	at int
	// before and after are the two pieces as written, and joined is the text
	// from the start of the first to the end of the second, as Liquibase
	// holds it.
	before, after, joined string
}

// addLine adds a `--rollback` line's SQL, and the line break Liquibase puts
// after it.
func (r *Rollback) addLine(sql string) {
	r.addPiece(sql)
	r.text += "\n"
}

// addPiece adds one piece of rollback text with nothing after it: a line of a
// block, or the text before its `*/`. A piece that is not blank, and follows a
// block's piece with no line break between them, makes a join.
func (r *Rollback) addPiece(piece string) {
	if strings.Trim(piece, sqlBlanks) != "" {
		if r.text != "" && !strings.HasSuffix(r.text, "\n") {
			r.joins = append(r.joins, rollbackJoin{
				at: len(r.text), before: r.last, after: piece, joined: r.text[r.lastAt:] + piece,
			})
		}
		r.last, r.lastAt = piece, len(r.text)
	}
	r.text += piece
}

// Text is the rollback exactly as Liquibase holds it, before it decides what
// the text is ([Rollback.Kind]).
func (r Rollback) Text() string {
	return r.text
}

// RollbackKind is what Liquibase makes of a changeset's rollback text.
type RollbackKind int

const (
	// NoRollback is a changeset whose rollback, if any, is blank. Liquibase
	// then derives one, and raw SQL has none to derive.
	NoRollback RollbackKind = iota
	// EmptyRollback is a rollback that says none is needed: text that starts
	// with "not required" or "empty", in any case, on one line. Rolling the
	// changeset back runs nothing.
	EmptyRollback
	// ChangesetRollback is a rollback that names changesetId, anywhere and in
	// any case. Liquibase reads it as a reference to another changeset, whose
	// changes are the rollback.
	ChangesetRollback
	// SQLRollback is SQL that Liquibase runs to roll the changeset back.
	SQLRollback
)

// Kind is what Liquibase makes of the rollback, by the rules of its
// formatted-SQL parser (handleRollbackSequence), checked in that order.
func (r Rollback) Kind() RollbackKind {
	trimmed := javaTrim(r.text)
	lowered := asciiLower(trimmed)
	switch {
	case trimmed == "":
		return NoRollback
	case oneJavaLine(lowered, "not required"), oneJavaLine(lowered, "empty"):
		return EmptyRollback
	case strings.Contains(lowered, "changesetid"):
		return ChangesetRollback
	default:
		return SQLRollback
	}
}

// oneJavaLine reports whether text matches Java's `^prefix.*`: it starts with
// prefix, and no Java line terminator follows.
func oneJavaLine(text, prefix string) bool {
	rest, ok := strings.CutPrefix(text, prefix)
	return ok && !strings.ContainsAny(rest, "\n\r\u0085\u2028\u2029")
}

// SQL is the rollback as the SQL of a down migration: what Liquibase runs, with
// the blanks around it trimmed. It is empty when the changeset writes no
// rollback, or writes that none is needed.
//
// The error continues a sentence whose subject the caller names: "<changeset>
// " + err.Error(). It refuses a rollback that names another changeset, whose
// changes Ptah does not look up, and one where Liquibase ran two lines together
// so that the SQL it runs is not the SQL written: two words joined into one, a
// line comment that runs on over the next line, a line break taken out of a
// quoted string. Where the SQL is the same either way, as with a statement per
// line or a continued line that begins with a blank, the rollback is Liquibase's
// text. When the reading cannot tell, it refuses.
func (r Rollback) SQL() (string, error) {
	switch r.Kind() {
	case NoRollback, EmptyRollback:
		return "", nil
	case ChangesetRollback:
		return "", fmt.Errorf("has a rollback that names changesetId, which Liquibase reads as a reference to another " +
			"changeset whose changes are the rollback; Ptah does not follow the reference, so write the rollback out")
	}
	for _, join := range r.joins {
		if joinChangesSQL(r.text, join.at) {
			return "", fmt.Errorf("has a rollback Liquibase does not run as written: Liquibase joins the lines of a "+
				"/* liquibase rollback block, and the block and a --rollback line after it, with nothing between "+
				"them, so %q and %q run as %q -- write the rollback as --rollback lines",
				join.before, join.after, join.joined)
		}
	}
	return javaTrim(r.text), nil
}

// javaTrim is Java's String.trim, which removes every character up to U+0020
// from both ends.
func javaTrim(text string) string {
	return strings.TrimFunc(text, func(c rune) bool { return c <= ' ' })
}

// joinChangesSQL reports whether the line break the author wrote at offset at,
// which Liquibase left out, would change the SQL. It reads the text with and
// without backslash escapes in quoted strings, since the dialects differ, and
// answers yes when either reading does.
func joinChangesSQL(text string, at int) bool {
	return joinChangesSQLWith(text, at, false) || joinChangesSQLWith(text, at, true)
}

// joinChangesSQLWith answers [joinChangesSQL] for one reading of quoted
// strings.
func joinChangesSQLWith(text string, at int, backslashEscapes bool) bool {
	before, after := text[at-1], text[at]
	switch sqlContextAt(text, at, backslashEscapes) {
	case inLineComment, inQuoted:
		// A line comment runs on over the next line, which is not blank since
		// a blank piece makes no join, and a quoted string or name loses the
		// line break written inside it.
		return true
	case inBlockComment:
		// Inside a comment only a new `*/` or `/*` changes anything.
		return before == '*' && after == '/' || before == '/' && after == '*'
	}
	// Liquibase ends a statement at `go` or `/` only at the start of a line.
	if lineStartDelimiter(text[at:]) {
		return true
	}
	return !separatesTokens(before) && !separatesTokens(after)
}

// lineStartDelimiter reports whether text starts with `go` as a word, in any
// case, or with `/`: the delimiters Liquibase's statement splitter honors only
// at the start of a line.
func lineStartDelimiter(text string) bool {
	if strings.HasPrefix(text, "/") && !strings.HasPrefix(text, "/*") {
		return true
	}
	rest, ok := strings.CutPrefix(asciiLower(text), "go")
	return ok && (rest == "" || !identifierByte(rest[0]))
}

// sqlBlanks are the bytes every SQL dialect reads as a blank between tokens.
const sqlBlanks = " \t\n\v\f\r"

// separatesTokens reports whether b ends one SQL token and begins the next
// whatever stands beside it: a blank, or `;`, `,`, `(` or `)`.
func separatesTokens(b byte) bool {
	return strings.IndexByte(sqlBlanks+";,()", b) >= 0
}

// identifierByte reports whether b can continue an unquoted SQL name.
func identifierByte(b byte) bool {
	return b == '_' || b == '$' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

// sqlContext is where an offset of SQL text stands.
type sqlContext int

const (
	atTopLevel sqlContext = iota
	inLineComment
	inBlockComment
	// inQuoted is a quoted string or name, or a dollar-quoted body.
	inQuoted
)

// sqlContextAt reads text up to offset at and reports where the offset stands.
// It knows the quoting of every dialect Ptah reads -- '...', "...", `...`,
// [...] and PostgreSQL's $tag$...$tag$ -- with a doubled quote as an escape,
// and a backslash as one too when backslashEscapes is set. Block comments do
// not nest.
func sqlContextAt(text string, at int, backslashEscapes bool) sqlContext {
	context := atTopLevel
	var closer string
	for index := 0; index < at; index++ {
		switch context {
		case inLineComment:
			if text[index] == '\n' {
				context = atTopLevel
			}
		case inBlockComment:
			if strings.HasPrefix(text[index:], "*/") {
				context, index = atTopLevel, index+1
			}
		case inQuoted:
			switch {
			case backslashEscapes && text[index] == '\\' && len(closer) == 1:
				index++
			case strings.HasPrefix(text[index:], closer+closer) && len(closer) == 1:
				index++
			case strings.HasPrefix(text[index:], closer):
				context, index = atTopLevel, index+len(closer)-1
			}
		default:
			context, closer, index = sqlOpening(text, index)
		}
	}
	return context
}

// sqlOpening reads what opens at text[index] when it stands at the top level:
// a comment, a quoted string or name, a dollar-quoted body, or nothing. It
// returns the context from there on, the text that closes a quoted one, and the
// index of the opening's last byte.
func sqlOpening(text string, index int) (sqlContext, string, int) {
	rest := text[index:]
	switch {
	case strings.HasPrefix(rest, "--"):
		return inLineComment, "", index + 1
	case strings.HasPrefix(rest, "/*"):
		return inBlockComment, "", index + 1
	case rest[0] == '\'' || rest[0] == '"' || rest[0] == '`':
		return inQuoted, rest[:1], index
	case rest[0] == '[':
		return inQuoted, "]", index
	case rest[0] == '$' && (index == 0 || !identifierByte(text[index-1])):
		if tag, ok := dollarTag(rest); ok {
			return inQuoted, tag, index + len(tag) - 1
		}
	}
	return atTopLevel, "", index
}

// dollarTag returns the `$tag$` or `$$` that opens a dollar-quoted body at the
// start of text.
func dollarTag(text string) (string, bool) {
	for index := 1; index < len(text); index++ {
		switch b := text[index]; {
		case b == '$':
			return text[:index+1], true
		case b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || index > 1 && b >= '0' && b <= '9':
		default:
			return "", false
		}
	}
	return "", false
}
