package lint

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// scanMode selects dialect-specific lexing behavior for the lint scanner.
// SQL comment and string syntax differ between the supported dialects in
// ways that change where statements begin and end, so the scanner cannot be
// dialect-blind: under PostgreSQL's standard_conforming_strings a backslash
// is a literal character (treating it as an escape lets 'C:\' swallow the
// rest of the file), while MySQL treats backslash as an escape and adds the
// # line-comment and /*!...*/ executable-comment forms.
type scanMode struct {
	// dialect is the normalized target, carried so the statement splitter can
	// read a compound routine body the way the dialect writes one. Empty is a
	// target nobody named, which recognizes only the dialect-blind form.
	dialect string
	// hashComments makes # start a line comment (MySQL/MariaDB).
	hashComments bool
	// backslashEscapes makes a backslash escape the next character inside
	// quoted strings (MySQL/MariaDB default; wrong for PostgreSQL).
	backslashEscapes bool
	// execComments treats MySQL executable comments /*!...*/ and MariaDB's own
	// /*M!...*/ marker as real SQL: the server executes their content, so the
	// linter must scan it. MySQL does not understand /*M!, so on a MySQL
	// server that block is dead, inert comment text -- but scanning it anyway
	// costs nothing and stays on the same side of the "never hide a hazard"
	// rule execComments already follows for /*!NNNNN, which is scanned on
	// every dialect without checking whether the server's own version would
	// actually run it.
	execComments bool
	// dollarQuotes recognizes $tag$...$tag$ string bodies (PostgreSQL).
	dollarQuotes bool
	// nestedComments lets block comments nest (PostgreSQL).
	nestedComments bool
	// yql reads the text with the YQL lexer of internal/lexer instead of
	// this file's scanner (YDB). YQL's strings, names and comments share
	// little with the other dialects -- a double-quoted "x" is a string,
	// backslashes escape in both quote styles, @@...@@ is a string, $x is a
	// name -- and the lexer that splits a YDB migration for the migrator
	// already reads them, so the linter reads what the migrator runs rather
	// than keeping a second YQL scanner.
	yql bool
}

// modeForDialect maps a lint target dialect to its lexing behavior. An empty
// or unknown dialect gets a hybrid whose overriding goal is to never HIDE a
// hazard: every lexing rule it omits is one that could swallow a statement in
// one of the supported dialects. It recognizes strings and PostgreSQL
// dollar-quoted bodies (so literal / function-body content is not mis-scanned
// as code), and it scans the content of MySQL executable comments (which only
// makes more visible), but it deliberately omits:
//
//   - backslash escapes — a trailing backslash in a standard-conforming
//     PostgreSQL literal would hide everything after it;
//   - MySQL '#' line comments — '#' is a PostgreSQL operator (bitwise XOR,
//     and the ubiquitous jsonb '#>' / '#>>'), so treating it as a comment
//     would eat the rest of the line including its ';' and hide a following
//     DROP TABLE;
//   - nested block comments — PostgreSQL nests them but MySQL/MariaDB do
//     not, so a '*/' must always close a comment; otherwise
//     "/* a /* b */ DROP TABLE t;" would keep scanning for a second '*/'
//     and swallow the DROP that MySQL would actually execute.
//
// The dialect-specific behaviors (MySQL '#' comments, PostgreSQL nested
// comments) require an explicit --dialect.
func modeForDialect(dialect string) scanMode {
	switch dialect {
	case "mysql", "mariadb":
		return scanMode{dialect: dialect, hashComments: true, backslashEscapes: true, execComments: true}
	case "postgres":
		return scanMode{dialect: dialect, dollarQuotes: true, nestedComments: true}
	case platform.YDB:
		return scanMode{dialect: dialect, yql: true}
	default:
		return scanMode{dialect: dialect, execComments: true, dollarQuotes: true}
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
	if mode.yql {
		return scanYQL(input)
	}
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
func nextLintToken(input string, i int, mode scanMode, execDepth int) (kind lintTokenKind, end, depth int) {
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
func nextPunctToken(input string, i int, mode scanMode, execDepth int) (kind lintTokenKind, end, depth int) {
	n := len(input)
	c := input[i]
	switch {
	case c == '-' && i+1 < n && input[i+1] == '-':
		return tokComment, lineCommentEnd(input, i+2), execDepth
	case c == '#' && mode.hashComments:
		return tokComment, lineCommentEnd(input, i+1), execDepth
	case c == '/' && i+1 < n && input[i+1] == '*':
		end, opensExec := blockCommentOpen(input, i, mode)
		if opensExec {
			return tokComment, end, execDepth + 1
		}
		return tokComment, end, execDepth
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

// blockCommentOpen scans a block comment opened at input[i] == '/' (with
// input[i+1] == '*' already confirmed by the caller) and reports where it
// ends and whether it opened as an executable-comment marker rather than an
// ordinary comment: MariaDB's /*M!NNNNN or the /*!NNNNN form MySQL and
// MariaDB share. Neither marker is recognized unless mode.execComments is set.
func blockCommentOpen(input string, i int, mode scanMode) (end int, opensExec bool) {
	if mode.execComments && isMariaDBExecMarker(input, i) {
		// The /*M!NNNNN marker is comment syntax, but MariaDB executes its
		// content; scan it as code until the */.
		return execCommentMarkerEnd(input, i+4), true
	}
	if mode.execComments && i+2 < len(input) && input[i+2] == '!' {
		// The /*!NNNNN marker is comment syntax, but its content is SQL the
		// MySQL family executes; scan it as code until the */.
		return execCommentMarkerEnd(input, i+3), true
	}
	return blockCommentEnd(input, i+2, mode.nestedComments), false
}

// isMariaDBExecMarker reports whether input[i:] opens a MariaDB executable
// comment, /*M!NNNNN with M matched case-insensitively, exactly as MariaDB
// itself matches it.
func isMariaDBExecMarker(input string, i int) bool {
	return i+3 < len(input) &&
		(input[i+2] == 'M' || input[i+2] == 'm') &&
		input[i+3] == '!'
}

// execCommentMarkerEnd consumes the optional version digits of a /*!NNNNN or
// /*M!NNNNN executable-comment marker.
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

// scanYQL tokenizes YQL with internal/lexer in its YQL mode, which is how the
// migrator reads the same text, and maps each token onto the kinds the rules
// read. A backticked name is a quoted identifier; every other identifier --
// a keyword, a name, a $name, a number with its suffix -- is a word. A
// translation setting such as --!syntax_v1 at the head of the text is a
// comment here: it starts no statement.
func scanYQL(input string) []lintToken {
	start := 0
	if strings.HasPrefix(input, "\uFEFF") {
		start = len("\uFEFF") // a UTF-8 BOM must not become part of the first word
	}
	lexr := lexer.NewLexerWithOptions(input[start:], dialectlexer.Options(platform.YDB))
	var toks []lintToken
	line := 1
	for {
		token := lexr.NextToken()
		if token.Type == lexer.TokenEOF {
			return toks
		}
		begin, end := start+token.Start, start+token.End
		toks = append(toks, lintToken{kind: yqlTokenKind(token), text: input[begin:end], start: begin, end: end, line: line})
		line += strings.Count(input[begin:end], "\n")
	}
}

// yqlTokenKind is the scanner kind of one YQL lexer token.
func yqlTokenKind(token lexer.Token) lintTokenKind {
	switch token.Type {
	case lexer.TokenWhitespace:
		return tokWhitespace
	case lexer.TokenComment, lexer.TokenUnknown:
		return tokComment
	case lexer.TokenString:
		return tokString
	case lexer.TokenSemicolon:
		return tokSemicolon
	case lexer.TokenIdentifier:
		if strings.HasPrefix(token.Value, "`") {
			return tokQuotedIdent
		}
		return tokWord
	default:
		return tokOp
	}
}
