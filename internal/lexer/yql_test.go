package lexer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// yqlToken is the part of a token a YQL row pins: its type and its text. The
// offsets follow from the text, and TestLexer_YQL_TokensCoverTheInput holds
// them.
type yqlToken struct {
	Type  lexer.TokenType
	Value string
}

func yqlTokens(input string) []yqlToken {
	return lexedTokens(input, lexer.Options{YQL: true})
}

// lexedTokens lexes input under options and keeps each token's type and
// text, without the EOF token.
func lexedTokens(input string, options lexer.Options) []yqlToken {
	var tokens []yqlToken
	for _, token := range everyToken(input, options) {
		if token.Type != lexer.TokenEOF {
			tokens = append(tokens, yqlToken{Type: token.Type, Value: token.Value})
		}
	}
	return tokens
}

// everyToken lexes input under options through its EOF token.
func everyToken(input string, options lexer.Options) []lexer.Token {
	l := lexer.NewLexerWithOptions(input, options)
	var tokens []lexer.Token
	for {
		token := l.NextToken()
		tokens = append(tokens, token)
		if token.Type == lexer.TokenEOF {
			return tokens
		}
	}
}

// Each row is measured on YDB 26.2.1.14 through the ydb CLI, or read from the
// YQL grammar's STRING_VALUE, ID_QUOTED and COMMENT rules where the row says
// so. A row whose tokens differ from the server's reading is a statement
// boundary the splitter would draw somewhere the server does not.
func TestLexer_YQL_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []yqlToken
	}{
		{
			// SELECT 'c\'d' answered c'd.
			name:  "a backslash escapes a single quote",
			input: `'c\'d';`,
			want:  []yqlToken{{lexer.TokenString, `'c\'d'`}, {lexer.TokenSemicolon, ";"}},
		},
		{
			// SELECT "a\"b" answered a"b: a double-quoted text is a string.
			name:  "a double-quoted text is a string with backslash escapes",
			input: `"a\"b;"`,
			want:  []yqlToken{{lexer.TokenString, `"a\"b;"`}},
		},
		{
			// SELECT 'a\'' answered a': the escaped quote does not pair with
			// the one after it, which closes the literal.
			name:  "an escaped quote followed by a quote closes the literal",
			input: `'a\''; DELETE`,
			want: []yqlToken{
				{lexer.TokenString, `'a\''`}, {lexer.TokenSemicolon, ";"},
				{lexer.TokenWhitespace, " "}, {lexer.TokenIdentifier, "DELETE"},
			},
		},
		{
			// SELECT 'a''b' AS x failed at AS: the server read two literals,
			// the second an alias of the first.
			name:  "a doubled quote is two literals",
			input: `'a''b'`,
			want:  []yqlToken{{lexer.TokenString, `'a'`}, {lexer.TokenString, `'b'`}},
		},
		{
			name:  "a backslash escapes a backslash",
			input: `'a\\';`,
			want:  []yqlToken{{lexer.TokenString, `'a\\'`}, {lexer.TokenSemicolon, ";"}},
		},
		{
			// "x"u, "x"s, "[1]"y, "{}"j and "x"U each answered one value.
			name:  "a one-letter type suffix belongs to the literal",
			input: `"x"u '[1]'Y`,
			want: []yqlToken{
				{lexer.TokenString, `"x"u`}, {lexer.TokenWhitespace, " "}, {lexer.TokenString, `'[1]'Y`},
			},
		},
		{
			// "x"pt, "x"pb, "x"pv and "x"p each answered one value.
			name:  "a two-letter type suffix belongs to the literal",
			input: `"x"pt "x"PB "x"p`,
			want: []yqlToken{
				{lexer.TokenString, `"x"pt`}, {lexer.TokenWhitespace, " "},
				{lexer.TokenString, `"x"PB`}, {lexer.TokenWhitespace, " "},
				{lexer.TokenString, `"x"p`},
			},
		},
		{
			// The grammar matches the longest suffix and no more.
			name:  "letters after the suffix start the next token",
			input: `"x"ux`,
			want:  []yqlToken{{lexer.TokenString, `"x"u`}, {lexer.TokenIdentifier, "x"}},
		},
		{
			// SELECT @@a;b@@ answered a;b.
			name:  "a multiline literal holds a semicolon",
			input: "@@a;\nb@@;",
			want:  []yqlToken{{lexer.TokenString, "@@a;\nb@@"}, {lexer.TokenSemicolon, ";"}},
		},
		{
			// SELECT @@a@@@@b@@ answered a@@b.
			name:  "four at signs continue a multiline literal",
			input: "@@a@@@@;b@@",
			want:  []yqlToken{{lexer.TokenString, "@@a@@@@;b@@"}},
		},
		{
			// SELECT @@c@@@ answered c@.
			name:  "a single at sign after the close belongs to the literal",
			input: "@@c@@@;",
			want:  []yqlToken{{lexer.TokenString, "@@c@@@"}, {lexer.TokenSemicolon, ";"}},
		},
		{
			name:  "a multiline literal takes a type suffix",
			input: `@@{"a":1}@@j`,
			want:  []yqlToken{{lexer.TokenString, `@@{"a":1}@@j`}},
		},
		{
			name:  "an unterminated multiline literal runs to the end",
			input: "@@a; DROP",
			want:  []yqlToken{{lexer.TokenString, "@@a; DROP"}},
		},
		{
			// CREATE TABLE `tick\`name` created tick`name, and SELECT 1 AS
			// `a``b` answered the column a`b.
			name:  "a backtick identifier takes both escapes",
			input: "`tick\\`;name` `a``b;`",
			want: []yqlToken{
				{lexer.TokenIdentifier, "`tick\\`;name`"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "`a``b;`"},
			},
		},
		{
			name:  "a path is one identifier",
			input: "`dir/sub/t`",
			want:  []yqlToken{{lexer.TokenIdentifier, "`dir/sub/t`"}},
		},
		{
			// SELECT 1 AS a /* outer /* inner */ */ failed at the second */.
			name:  "a block comment does not nest",
			input: "/* a /* b */ ; */",
			want: []yqlToken{
				{lexer.TokenComment, "/* a /* b */"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenSemicolon, ";"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenOperator, "*"}, {lexer.TokenOperator, "/"},
			},
		},
		{
			// SELECT 1 # not a comment failed with "token recognition error
			// at: '#'".
			name:  "a hash is not a comment",
			input: "# x;",
			want: []yqlToken{
				{lexer.TokenOperator, "#"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "x"}, {lexer.TokenSemicolon, ";"},
			},
		},
		{
			name:  "a line comment holds a semicolon",
			input: "-- a; b\n;",
			want: []yqlToken{
				{lexer.TokenComment, "-- a; b"}, {lexer.TokenWhitespace, "\n"}, {lexer.TokenSemicolon, ";"},
			},
		},
		{
			// $ starts a named expression, so PostgreSQL's dollar quote has
			// no meaning here and must not hide the semicolon.
			name:  "a dollar sign never opens a dollar-quoted string",
			input: "$a$;$a$",
			want: []yqlToken{
				{lexer.TokenIdentifier, "$a$"}, {lexer.TokenSemicolon, ";"}, {lexer.TokenIdentifier, "$a$"},
			},
		},
		{
			name:  "a lone dollar sign is an operator",
			input: "$ ",
			want:  []yqlToken{{lexer.TokenOperator, "$"}, {lexer.TokenWhitespace, " "}},
		},
		{
			// SELECT 1uAS a, 0o7AS b, 0x1fAS c, 0b1uAS d, 1e0AS e, 1.5fAS f,
			// 2.5pf8AS g, 3pnAS h answered 1, 7, 506, 1, 1, 1.5, "2.5" and
			// "3": a number takes its suffix, and the word after it starts
			// where the suffix ends. 0x1fAS is the hex digits 1fA and the
			// suffix S.
			name:  "a number carries its type suffix and stops there",
			input: "1uFROM 0o7AS 0x1fAS 0b1uAS 1e0INTO 1.5fAS 2.5pf8AS 3pnAS",
			want: []yqlToken{
				{lexer.TokenIdentifier, "1u"}, {lexer.TokenIdentifier, "FROM"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "0o7"}, {lexer.TokenIdentifier, "AS"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "0x1fAS"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "0b1u"}, {lexer.TokenIdentifier, "AS"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "1e0"}, {lexer.TokenIdentifier, "INTO"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "1.5f"}, {lexer.TokenIdentifier, "AS"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "2.5pf8"}, {lexer.TokenIdentifier, "AS"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "3pn"}, {lexer.TokenIdentifier, "AS"},
			},
		},
		{
			// Each answered a parse error at the token after the one shown:
			// a prefix with no digit of its base, an exponent with no digit,
			// and an f directly after a decimal point end the number early.
			name:  "a number stops where the grammar stops it",
			input: "0x 1e 1.foo 0b2 0o9 1e+",
			want: []yqlToken{
				{lexer.TokenIdentifier, "0"}, {lexer.TokenIdentifier, "x"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "1"}, {lexer.TokenIdentifier, "e"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "1.f"}, {lexer.TokenIdentifier, "oo"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "0b"}, {lexer.TokenIdentifier, "2"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "0"}, {lexer.TokenIdentifier, "o9"}, {lexer.TokenWhitespace, " "},
				{lexer.TokenIdentifier, "1"}, {lexer.TokenIdentifier, "e"}, {lexer.TokenOperator, "+"},
			},
		},
		{
			name:  "a named expression is one identifier",
			input: "$rows = 1;",
			want: []yqlToken{
				{lexer.TokenIdentifier, "$rows"}, {lexer.TokenWhitespace, " "}, {lexer.TokenOperator, "="},
				{lexer.TokenWhitespace, " "}, {lexer.TokenIdentifier, "1"}, {lexer.TokenSemicolon, ";"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlTokens(test.input), qt.DeepEquals, test.want)
		})
	}
}

// The server reads translation settings from the head of the text only:
// measured, --!ansi_lexer changed how SELECT 'a\' read after leading
// whitespace, after a blank line and after another setting, and did not after
// a plain comment, after a block comment, or after a statement. A setting is
// TokenUnknown so that stripping comments keeps it; anywhere else the same
// text is an ordinary comment.
func TestLexer_YQL_TranslationSettings(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []yqlToken
	}{
		{
			name:  "a setting at the head",
			input: "--!ansi_lexer\nSELECT",
			want: []yqlToken{
				{lexer.TokenUnknown, "--!ansi_lexer"}, {lexer.TokenWhitespace, "\n"},
				{lexer.TokenIdentifier, "SELECT"},
			},
		},
		{
			name:  "settings after whitespace and a blank line",
			input: "  --!syntax_v1\n\n--!ansi_lexer\n",
			want: []yqlToken{
				{lexer.TokenWhitespace, "  "}, {lexer.TokenUnknown, "--!syntax_v1"},
				{lexer.TokenWhitespace, "\n\n"}, {lexer.TokenUnknown, "--!ansi_lexer"},
				{lexer.TokenWhitespace, "\n"},
			},
		},
		{
			name:  "a plain comment ends the head",
			input: "-- c\n--!ansi_lexer",
			want: []yqlToken{
				{lexer.TokenComment, "-- c"}, {lexer.TokenWhitespace, "\n"}, {lexer.TokenComment, "--!ansi_lexer"},
			},
		},
		{
			name:  "a block comment ends the head",
			input: "--!syntax_v1\n/* c */\n--!ansi_lexer",
			want: []yqlToken{
				{lexer.TokenUnknown, "--!syntax_v1"}, {lexer.TokenWhitespace, "\n"},
				{lexer.TokenComment, "/* c */"}, {lexer.TokenWhitespace, "\n"},
				{lexer.TokenComment, "--!ansi_lexer"},
			},
		},
		{
			name:  "a statement ends the head",
			input: "SELECT 1;\n--!ansi_lexer",
			want: []yqlToken{
				{lexer.TokenIdentifier, "SELECT"}, {lexer.TokenWhitespace, " "}, {lexer.TokenIdentifier, "1"},
				{lexer.TokenSemicolon, ";"}, {lexer.TokenWhitespace, "\n"}, {lexer.TokenComment, "--!ansi_lexer"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(yqlTokens(test.input), qt.DeepEquals, test.want)
		})
	}
}

// The YQL tokens tile the input, so a caller that writes token values back
// out, as the splitter does, reproduces the text byte for byte.
func TestLexer_YQL_TokensCoverTheInput(t *testing.T) {
	inputs := []string{
		"--!syntax_v1\nDEFINE ACTION $a() AS SELECT 'x\\'y'u; END DEFINE;\nDO $a();",
		"SELECT @@a@@@@b@@j, \"q\"pt, `t\\`x`, $f(($x) -> { RETURN $x; }) # x",
		"'unterminated \\'",
		"@@unterminated",
		"/* unterminated",
	}
	for _, input := range inputs {
		c := qt.New(t)
		tokens := everyToken(input, lexer.Options{YQL: true})
		var rebuilt strings.Builder
		position := 0
		for _, token := range tokens {
			c.Assert(token.Start, qt.Equals, position, qt.Commentf("input %q", input))
			c.Assert(token.Value, qt.Equals, input[token.Start:token.End], qt.Commentf("input %q", input))
			position = token.End
			rebuilt.WriteString(token.Value)
		}
		c.Assert(rebuilt.String(), qt.Equals, input)
		c.Assert(tokens[len(tokens)-1], qt.Equals, lexer.Token{Type: lexer.TokenEOF, Start: len(input), End: len(input)})
	}
}

// The YQL field leaves every other reading where it was. Each row is a text
// whose YQL reading differs, lexed under the options a dialect that is not
// YDB uses, with the tokens that dialect produced before YQL existed.
func TestLexer_YQL_OtherDialectsKeepTheirReading(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		options lexer.Options
		want    []yqlToken
	}{
		{
			name:    "MySQL system variables keep their at signs apart",
			input:   "@@session.x;",
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want: []yqlToken{
				{lexer.TokenOperator, "@"}, {lexer.TokenOperator, "@"}, {lexer.TokenIdentifier, "session"},
				{lexer.TokenOperator, "."}, {lexer.TokenIdentifier, "x"}, {lexer.TokenSemicolon, ";"},
			},
		},
		{
			name:    "PostgreSQL keeps its dollar quote",
			input:   "$a$;$a$",
			options: lexer.Options{StandardStrings: true, PostgreSQLEscapeStrings: true},
			want:    []yqlToken{{lexer.TokenString, "$a$;$a$"}},
		},
		{
			name:    "MySQL keeps its doubled quote",
			input:   `'a\''';'`,
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want:    []yqlToken{{lexer.TokenString, `'a\''';'`}},
		},
		{
			name:    "MySQL keeps its hash comment",
			input:   "# c;",
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want:    []yqlToken{{lexer.TokenComment, "# c;"}},
		},
		{
			name:    "a suffix stays a separate identifier",
			input:   `'x'u`,
			options: lexer.Options{StandardStrings: true},
			want:    []yqlToken{{lexer.TokenString, `'x'`}, {lexer.TokenIdentifier, "u"}},
		},
		{
			name:    "a leading bang comment stays a comment",
			input:   "--!x",
			options: lexer.Options{StandardStrings: true},
			want:    []yqlToken{{lexer.TokenComment, "--!x"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lexedTokens(test.input, test.options), qt.DeepEquals, test.want)
		})
	}
}

// Each row is a YQL literal and the text YDB 26.2.1.14 answered for it.
func TestStringValue_YQL_HappyPath(t *testing.T) {
	yql := lexer.Options{YQL: true}
	tests := []struct {
		name    string
		literal string
		want    string
	}{
		{name: "an escaped single quote", literal: `'c\'d'`, want: `c'd`},
		{name: "an escaped double quote", literal: `"a\"b"u`, want: `a"b`},
		{name: "an unknown escape stands for its character", literal: `"a\qb"u`, want: `aqb`},
		{name: "hex, octal and unicode escapes", literal: `"\x41\101A\U00000041"u`, want: `AAAA`},
		{name: "control escapes", literal: `"\n\t\r\\\a\b\f\v"`, want: "\n\t\r\\\a\b\f\v"},
		{name: "an octal zero", literal: `"\000x"`, want: "\x00x"},
		{name: "a digit above three stands for itself", literal: `"\400"`, want: `400`},
		{name: "two hex digits and no more", literal: `"\x414"u`, want: `A4`},
		{name: "a question mark", literal: `"\?"u`, want: `?`},
		{name: "a two-letter suffix", literal: `'x'Pt`, want: `x`},
		{name: "a multiline literal", literal: "@@a;\nb@@", want: "a;\nb"},
		{name: "four at signs stand for two", literal: `@@a@@@@b@@`, want: `a@@b`},
		{name: "a trailing at sign", literal: `@@c@@@`, want: `c@`},
		{name: "a multiline literal keeps its backslashes", literal: `@@a\nb@@j`, want: `a\nb`},
		{name: "an empty literal", literal: `""`, want: ``},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.StringValue(test.literal, yql)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A literal the server refuses, or a token that is not one whole literal,
// has no text.
func TestStringValue_YQL_FailurePath(t *testing.T) {
	yql := lexer.Options{YQL: true}
	tests := []struct {
		name    string
		literal string
	}{
		// Each of these four answered "Failed to parse string literal".
		{name: "a short octal escape", literal: `"\01x"`},
		{name: "a short hex escape", literal: `"\x4"`},
		{name: "a surrogate", literal: `"\uD800"`},
		{name: "a code point past the last", literal: `"\U00110000"`},
		{name: "an unterminated literal", literal: `'a\'`},
		{name: "an unterminated multiline literal", literal: `@@a`},
		{name: "an unknown suffix", literal: `"x"q`},
		{name: "a doubled quote is not one literal", literal: `'a''b'`},
		{name: "an identifier", literal: "`a`"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.StringValue(test.literal, yql)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestYQLIdentifierValue_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{name: "a bare name keeps its case", token: "Users", want: "Users"},
		{name: "a backticked path", token: "`dir/sub/t`", want: "dir/sub/t"},
		{name: "an escaped backtick", token: "`tick\\`name`", want: "tick`name"},
		{name: "a doubled backtick", token: "`tick``name`", want: "tick`name"},
		{name: "an escaped backslash", token: "`back\\\\slash`", want: "back\\slash"},
		{name: "an empty backticked name", token: "``", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := lexer.YQLIdentifierValue(test.token)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestYQLIdentifierValue_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		token string
	}{
		{name: "no token", token: ""},
		{name: "a missing closing backtick", token: "`dir/t"},
		{name: "text after the closing backtick", token: "`a`b"},
		{name: "a trailing backslash", token: "`a\\"},
		{name: "an escape short of its digits", token: "`a\\x1`"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := lexer.YQLIdentifierValue(test.token)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
