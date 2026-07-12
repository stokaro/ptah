package lint

import "strings"

// scanMode selects dialect-specific lexing behavior for the lint scanner.
// SQL comment and string syntax differ between the supported dialects in
// ways that change where statements begin and end, so the scanner cannot be
// dialect-blind: under PostgreSQL's standard_conforming_strings a backslash
// is a literal character (treating it as an escape lets 'C:\' swallow the
// rest of the file), while MySQL treats backslash as an escape and adds the
// # line-comment and /*!...*/ executable-comment forms.
type scanMode struct {
	// hashComments makes # start a line comment (MySQL/MariaDB).
	hashComments bool
	// backslashEscapes makes a backslash escape the next character inside
	// quoted strings (MySQL/MariaDB default; wrong for PostgreSQL).
	backslashEscapes bool
	// execComments treats MySQL executable comments /*!...*/ as real SQL:
	// the server executes their content, so the linter must scan it.
	execComments bool
	// dollarQuotes recognizes $tag$...$tag$ string bodies (PostgreSQL).
	dollarQuotes bool
	// nestedComments lets block comments nest (PostgreSQL).
	nestedComments bool
}

// modeForDialect maps a lint target dialect to its lexing behavior. An empty
// or unknown dialect gets a hybrid that keeps hazards visible for every
// supported dialect: the MySQL comment forms are honored, but backslash
// escapes stay off because a trailing backslash in a standard-conforming
// literal would otherwise hide every statement after it.
func modeForDialect(dialect string) scanMode {
	switch dialect {
	case "mysql", "mariadb":
		return scanMode{hashComments: true, backslashEscapes: true, execComments: true}
	case "postgres":
		return scanMode{dollarQuotes: true, nestedComments: true}
	default:
		return scanMode{hashComments: true, execComments: true, dollarQuotes: true, nestedComments: true}
	}
}

// lintTokenKind classifies scanner tokens.
type lintTokenKind int

const (
	tokWhitespace lintTokenKind = iota
	tokComment
	// tokString is a single-quoted or dollar-quoted literal, kept verbatim.
	tokString
	// tokQuotedIdent is a double-quoted or backtick-quoted identifier (or a
	// MySQL double-quoted string), kept verbatim including the quotes.
	tokQuotedIdent
	tokWord
	tokSemicolon
	tokOp
)

// lintToken is one lexical token of a migration file.
type lintToken struct {
	kind lintTokenKind
	text string
	// start/end are byte offsets into the scanned input.
	start int
	end   int
	// line is the 1-based line the token starts on.
	line int
}

// scanSQL tokenizes SQL under the given dialect mode. It never fails: an
// unterminated string or comment consumes the rest of the input.
func scanSQL(input string, mode scanMode) []lintToken {
	var toks []lintToken
	n := len(input)
	i := 0
	if strings.HasPrefix(input, "\uFEFF") {
		i = len("\uFEFF") // a UTF-8 BOM must not become part of the first word
	}
	line := 1
	execDepth := 0
	for i < n {
		start := i
		startLine := line
		var kind lintTokenKind
		kind, i, execDepth = nextLintToken(input, i, mode, execDepth)
		toks = append(toks, lintToken{kind: kind, text: input[start:i], start: start, end: i, line: startLine})
		line += strings.Count(input[start:i], "\n")
	}
	return toks
}

// nextLintToken scans one token starting at input[i] and returns its kind,
// the offset past its end, and the updated executable-comment depth.
func nextLintToken(input string, i int, mode scanMode, execDepth int) (kind lintTokenKind, end int, depth int) {
	c := input[i]
	switch {
	case isSpaceByte(c):
		return tokWhitespace, whitespaceEnd(input, i), execDepth
	case isWordByte(c):
		return tokWord, wordEnd(input, i), execDepth
	case c == '\'':
		return tokString, quotedEnd(input, i+1, '\'', mode.backslashEscapes), execDepth
	case c == '"':
		return tokQuotedIdent, quotedEnd(input, i+1, '"', mode.backslashEscapes), execDepth
	case c == '`':
		return tokQuotedIdent, quotedEnd(input, i+1, '`', false), execDepth
	case c == ';':
		return tokSemicolon, i + 1, execDepth
	default:
		return nextPunctToken(input, i, mode, execDepth)
	}
}

// nextPunctToken handles the punctuation-led tokens: comment forms, dollar
// quotes and bare operator characters.
func nextPunctToken(input string, i int, mode scanMode, execDepth int) (kind lintTokenKind, end int, depth int) {
	n := len(input)
	c := input[i]
	switch {
	case c == '-' && i+1 < n && input[i+1] == '-':
		return tokComment, lineCommentEnd(input, i+2), execDepth
	case c == '#' && mode.hashComments:
		return tokComment, lineCommentEnd(input, i+1), execDepth
	case c == '/' && i+1 < n && input[i+1] == '*':
		if mode.execComments && i+2 < n && input[i+2] == '!' {
			// The /*!NNNNN marker is comment syntax, but its content is SQL
			// the MySQL family executes; scan it as code until the */.
			return tokComment, execCommentMarkerEnd(input, i+3), execDepth + 1
		}
		return tokComment, blockCommentEnd(input, i+2, mode.nestedComments), execDepth
	case c == '*' && execDepth > 0 && i+1 < n && input[i+1] == '/':
		return tokComment, i + 2, execDepth - 1
	case c == '$' && mode.dollarQuotes:
		if end, ok := dollarQuoteEnd(input, i); ok {
			return tokString, end, execDepth
		}
		return tokOp, i + 1, execDepth
	default:
		return tokOp, i + 1, execDepth
	}
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// isWordByte reports whether a byte continues a bare word. Bytes >= 0x80 are
// UTF-8 continuation or lead bytes, so non-ASCII identifiers stay one word.
func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func whitespaceEnd(input string, i int) int {
	for i < len(input) && isSpaceByte(input[i]) {
		i++
	}
	return i
}

func wordEnd(input string, i int) int {
	for i < len(input) && isWordByte(input[i]) {
		i++
	}
	return i
}

func lineCommentEnd(input string, i int) int {
	for i < len(input) && input[i] != '\n' {
		i++
	}
	return i
}

// execCommentMarkerEnd consumes the optional version digits of a /*!NNNNN
// executable-comment marker.
func execCommentMarkerEnd(input string, i int) int {
	for i < len(input) && input[i] >= '0' && input[i] <= '9' {
		i++
	}
	return i
}

func blockCommentEnd(input string, i int, nested bool) int {
	depth := 1
	n := len(input)
	for i < n {
		switch {
		case nested && input[i] == '/' && i+1 < n && input[i+1] == '*':
			depth++
			i += 2
		case input[i] == '*' && i+1 < n && input[i+1] == '/':
			i += 2
			depth--
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return i
}

// quotedEnd scans a quoted region opened before input[i] until its closing
// quote, honoring doubled-quote escapes (two consecutive quote characters
// stay inside the region) and, when enabled, backslash escapes.
func quotedEnd(input string, i int, quote byte, backslashEscapes bool) int {
	n := len(input)
	for i < n {
		switch {
		case backslashEscapes && input[i] == '\\' && i+1 < n:
			i += 2
		case input[i] == quote:
			if i+1 < n && input[i+1] == quote {
				i += 2 // doubled quote stays inside the region
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return i
}

// dollarQuoteEnd matches a PostgreSQL dollar-quoted body $tag$...$tag$
// starting at input[i] == '$' and returns the offset past its end. The tag
// follows identifier rules (it must not start with a digit: $1 is a
// positional parameter, not a delimiter).
func dollarQuoteEnd(input string, i int) (int, bool) {
	n := len(input)
	j := i + 1
	if j < n && input[j] >= '0' && input[j] <= '9' {
		return 0, false
	}
	for j < n && isWordByte(input[j]) {
		j++
	}
	if j >= n || input[j] != '$' {
		return 0, false
	}
	delim := input[i : j+1]
	rest := strings.Index(input[j+1:], delim)
	if rest < 0 {
		return n, true // unterminated: consume the rest of the input
	}
	return j + 1 + rest + len(delim), true
}
