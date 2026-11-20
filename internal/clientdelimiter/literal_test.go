package clientdelimiter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clientdelimiter"
)

// A line that reads like a directive inside a literal or a block comment is
// the text of that literal or comment. The rows are scripts with no directive
// at all, so the rewrite has to hand each back byte for byte; the dialect rules
// are what decide where a literal ends.
func TestNormalizeFor_DirectiveLookingTextInsideALiteral_HappyPath(t *testing.T) {
	mysql := clientdelimiter.Rules{BackslashEscapes: true}
	postgres := clientdelimiter.Rules{EscapeStrings: true}
	tests := []struct {
		name   string
		rules  clientdelimiter.Rules
		script string
	}{
		{
			name:   "Atlas form in a single-quoted literal",
			script: "COMMENT ON TABLE t IS 'c\n-- atlas:delimiter $$\nd';\nSELECT 1;\n",
		},
		{
			name:   "client form in a single-quoted literal",
			script: "SELECT 'a\nDELIMITER $$\nb';\nSELECT 2;\n",
		},
		{
			name:   "Atlas form in a double-quoted string",
			script: "SELECT \"a\n-- atlas:delimiter $$\nb\";\nSELECT 2;\n",
		},
		{
			name:   "client form in a backtick-quoted name",
			script: "SELECT 1 AS `a\nDELIMITER $$\nb`;\nSELECT 2;\n",
		},
		{
			name:   "client form after a doubled quote",
			script: "SELECT 'it''s\nDELIMITER $$\nx';\nSELECT 2;\n",
		},
		{
			name:   "client form in a block comment",
			script: "/* note\nDELIMITER $$\n*/ SELECT 1;\nSELECT 2;\n",
		},
		{
			name:   "Atlas form in a dollar-quoted body",
			script: "SELECT $q$\n-- atlas:delimiter $$\n$q$;\nSELECT 2;\n",
		},
		{
			name:   "MySQL backslash keeps the literal open",
			rules:  mysql,
			script: "SELECT 'a\\'\nDELIMITER $$\nb';\nSELECT 2;\n",
		},
		{
			name:   "PostgreSQL E string keeps the literal open",
			rules:  postgres,
			script: "SELECT E'a\\'\nDELIMITER $$\nb';\nSELECT 2;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _ := clientdelimiter.NormalizeFor(test.script, test.rules)

			c.Assert(got, qt.Equals, test.script)
		})
	}
}

// Under a custom delimiter the literal still reads as text, and the delimiter
// at the end of the statement is rewritten as usual.
func TestNormalizeFor_DirectiveLookingTextUnderACustomDelimiter_HappyPath(t *testing.T) {
	c := qt.New(t)
	script := "DELIMITER $$\nCREATE PROCEDURE p() BEGIN SELECT 'a\n-- atlas:delimiter ;\nb'; END$$\nCREATE PROCEDURE q() BEGIN SELECT 1; END$$\n"

	got, _ := clientdelimiter.NormalizeFor(script, clientdelimiter.Rules{BackslashEscapes: true})

	c.Assert(got, qt.Equals, "CREATE PROCEDURE p() BEGIN SELECT 'a\n-- atlas:delimiter ;\nb'; END;\nCREATE PROCEDURE q() BEGIN SELECT 1; END;\n")
}

// The control for the rows above: a directive outside a literal is still
// honored, and so is one that follows a literal the dialect closes early (a
// PostgreSQL backslash is an ordinary character, so `'a\'` is a whole literal).
func TestNormalizeFor_DirectiveOutsideALiteral_StillHonored(t *testing.T) {
	tests := []struct {
		name   string
		rules  clientdelimiter.Rules
		script string
		want   string
	}{
		{
			name:   "client form after a closed literal",
			script: "SELECT 'a';\nDELIMITER $$\nSELECT 2$$\n",
			want:   "SELECT 'a';\nSELECT 2;\n",
		},
		{
			name:   "Atlas form after a closed literal",
			script: "SELECT 'a';\n-- atlas:delimiter $$\nSELECT 2$$\n",
			want:   "SELECT 'a';\nSELECT 2;\n",
		},
		{
			name:   "client form after a closed block comment",
			script: "/* note */\nDELIMITER $$\nSELECT 2$$\n",
			want:   "/* note */\nSELECT 2;\n",
		},
		{
			name:   "PostgreSQL literal ending in a backslash",
			script: "SELECT 'a\\';\nDELIMITER $$\nSELECT 2$$\n",
			want:   "SELECT 'a\\';\nSELECT 2;\n",
		},
		{
			name:   "client form after a doubled-quote literal",
			script: "SELECT 'it''s';\nDELIMITER $$\nSELECT 2$$\n",
			want:   "SELECT 'it''s';\nSELECT 2;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _ := clientdelimiter.NormalizeFor(test.script, test.rules)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
