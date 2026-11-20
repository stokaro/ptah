package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// stringsAndWords lists the strings and identifiers of text, in order, as
// "type:text", the rest left out.
func stringsAndWords(text string, options lexer.Options) []string {
	scan := lexer.NewLexerWithOptions(text, options)
	var found []string
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		switch token.Type {
		case lexer.TokenString:
			found = append(found, "string:"+token.Value)
		case lexer.TokenIdentifier:
			found = append(found, "word:"+token.Value)
		}
	}
	return found
}

// With AlternativeQuoting an Oracle q-quoted literal is one string whatever its
// delimiter and whatever it holds, an apostrophe included, and the text after it
// is text again.
func TestLexer_AlternativeQuoting_HappyPath(t *testing.T) {
	options := lexer.Options{StandardStrings: true, AlternativeQuoting: true}
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "square brackets and an apostrophe", text: `q'[it's]' || a`, want: []string{`string:q'[it's]'`, "word:a"}},
		{name: "braces", text: `q'{it's}' || a`, want: []string{`string:q'{it's}'`, "word:a"}},
		{name: "angle brackets", text: `q'<it's>' || a`, want: []string{`string:q'<it's>'`, "word:a"}},
		{name: "parentheses", text: `q'(it's)' || a`, want: []string{`string:q'(it's)'`, "word:a"}},
		{name: "an exclamation mark", text: `q'!it's!' || a`, want: []string{`string:q'!it's!'`, "word:a"}},
		{name: "a hash", text: `q'#it's#' || a`, want: []string{`string:q'#it's#'`, "word:a"}},
		{name: "a letter as the delimiter", text: `q'xit'sx' || a`, want: []string{`string:q'xit'sx'`, "word:a"}},
		{name: "upper case", text: `Q'[it's]' || a`, want: []string{`string:Q'[it's]'`, "word:a"}},
		{name: "national", text: `nq'[it's]' || a`, want: []string{`string:nq'[it's]'`, "word:a"}},
		{name: "national in upper case", text: `NQ'[it's]' || a`, want: []string{`string:NQ'[it's]'`, "word:a"}},
		{name: "a mixed-case national", text: `Nq'[it's]' || a`, want: []string{`string:Nq'[it's]'`, "word:a"}},
		{name: "a closing bracket not followed by a quote", text: `q'[a]b]' || c`, want: []string{`string:q'[a]b]'`, "word:c"}},
		{name: "the closing delimiter inside", text: `q'[a[b]' || c`, want: []string{`string:q'[a[b]'`, "word:c"}},
		{name: "an empty literal", text: `q'[]' || c`, want: []string{`string:q'[]'`, "word:c"}},
		{name: "a double quote inside", text: `q'["x"]' || c`, want: []string{`string:q'["x"]'`, "word:c"}},
		{name: "a multi-byte delimiter", text: `q'«it's«' || c`, want: []string{`string:q'«it's«'`, "word:c"}},
		{name: "an ordinary literal after", text: `q'[a]' || 'b' || c`, want: []string{`string:q'[a]'`, `string:'b'`, "word:c"}},
		{name: "a name that ends in q", text: `aq'x' || c`, want: []string{"word:aq", `string:'x'`, "word:c"}},
		{name: "q alone", text: `q || c`, want: []string{"word:q", "word:c"}},
		{name: "q before a space", text: `q 'x' || c`, want: []string{"word:q", `string:'x'`, "word:c"}},
		{name: "a space as the delimiter", text: `q' x' || c`, want: []string{"word:q", `string:' x'`, "word:c"}},
		{name: "q before a quote at the end", text: `q'`, want: []string{"word:q", `string:'`}},
		{name: "an unterminated literal runs to the end", text: `q'[a || c`, want: []string{`string:q'[a || c`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(stringsAndWords(tc.text, options), qt.DeepEquals, tc.want)
		})
	}
}

// Without the option the lexer reads the literal as it always did, which is what
// internal/sqlreach relies on to refuse the spelling: q is a word and the string
// ends at the apostrophe inside.
func TestLexer_AlternativeQuoting_OffReadsAWordThenAString(t *testing.T) {
	c := qt.New(t)

	got := stringsAndWords(`q'[it's]' || a`, lexer.Options{StandardStrings: true})

	c.Assert(got, qt.DeepEquals, []string{"word:q", `string:'[it'`, "word:s", `string:' || a`})
}
