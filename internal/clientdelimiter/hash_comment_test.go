package clientdelimiter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clientdelimiter"
)

// Rules that name a dialect's markers read each one as a comment whatever
// follows it, so a banner such as `#= don't` hides its apostrophe and the
// directive below it acts.
func TestNormalizeFor_DeclaredHashComment_HappyPath(t *testing.T) {
	declared := clientdelimiter.Rules{BackslashEscapes: true, LineComments: []string{"#"}, LineCommentsDeclared: true}
	tests := []struct {
		name   string
		script string
		want   string
	}{
		{"banner with an equals sign", "#= don't\nDELIMITER $$\nSELECT 1; SELECT 2$$\n", "#= don't\nSELECT 1; SELECT 2;\n"},
		{"banner with a greater-than sign", "#> don't\nDELIMITER $$\nSELECT 1; SELECT 2$$\n", "#> don't\nSELECT 1; SELECT 2;\n"},
		{"banner with a space", "# don't\nDELIMITER $$\nSELECT 1; SELECT 2$$\n", "# don't\nSELECT 1; SELECT 2;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _ := clientdelimiter.NormalizeFor(test.script, declared)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Rules that name no markers read # as an ordinary character, as PostgreSQL
// does, so the apostrophe after it opens a literal and the directive inside
// that literal is text.
func TestNormalizeFor_UndeclaredHash_HappyPath(t *testing.T) {
	postgres := clientdelimiter.Rules{EscapeStrings: true, DollarQuotes: true, LineCommentsDeclared: true}
	tests := []struct {
		name   string
		script string
	}{
		{"xor beside a literal", "SELECT 5 # 3 AS x, 'abc\nDELIMITER $$\ndef';\nSELECT 2;\n"},
		{"json path operator beside a literal", "SELECT a #> 'abc\nDELIMITER $$\ndef';\nSELECT 2;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _ := clientdelimiter.NormalizeFor(test.script, postgres)

			c.Assert(got, qt.Equals, test.script)
		})
	}
}

// Rules with no dialect keep the heuristic: a # starts a comment unless an
// operator character follows it.
func TestNormalizeFor_NoDialectHashHeuristic_HappyPath(t *testing.T) {
	legacy := clientdelimiter.Rules{BackslashEscapes: true, LineComments: []string{"#"}}
	tests := []struct {
		name   string
		script string
		want   string
	}{
		{"hash then space is a comment", "# don't\nDELIMITER $$\nSELECT 1; SELECT 2$$\n", "# don't\nSELECT 1; SELECT 2;\n"},
		{"hash then operator character is not", "#= don't\nDELIMITER $$\nSELECT 1$$\n", "#= don't\nDELIMITER $$\nSELECT 1$$\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _ := clientdelimiter.NormalizeFor(test.script, legacy)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
