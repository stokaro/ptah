package lexerext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext"
)

// namedScanner is a scanner the tests can tell apart by its name. It reads no
// text.
type namedScanner struct{ name string }

func (*namedScanner) Scan(text string, start int, _ bool) lexer.TokenReader {
	return lexer.NewLexer(text[start:])
}
func (*namedScanner) StringValue(string) (string, bool)     { return "", false }
func (*namedScanner) IdentifierValue(string) (string, bool) { return "", false }

// A registration answers under every name it was given, without regard to
// case or surrounding space, until it is released, and releasing twice is
// harmless.
func TestProvide_HappyPath(t *testing.T) {
	c := qt.New(t)
	scanner := &namedScanner{name: "engine"}
	release, err := lexerext.Provide(scanner, "provided-engine", "provided-alias")
	c.Assert(err, qt.IsNil)
	c.Assert(lexerext.Provided("provided-engine"), qt.Equals, lexer.Scanner(scanner))
	c.Assert(lexerext.Provided(" PROVIDED-ALIAS "), qt.Equals, lexer.Scanner(scanner))
	c.Assert(lexerext.Provided("provided-other"), qt.IsNil)
	release()
	release()
	c.Assert(lexerext.Provided("provided-engine"), qt.IsNil)
	c.Assert(lexerext.Provided("provided-alias"), qt.IsNil)
}

// A release removes only its own entries: a later registration of the same
// name survives the release of an earlier one.
func TestProvide_ReleaseKeepsALaterRegistration(t *testing.T) {
	c := qt.New(t)
	first, err := lexerext.Provide(&namedScanner{name: "first"}, "provided-reused")
	c.Assert(err, qt.IsNil)
	first()
	second := &namedScanner{name: "second"}
	release, err := lexerext.Provide(second, "provided-reused")
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)
	first()
	c.Assert(lexerext.Provided("provided-reused"), qt.Equals, lexer.Scanner(second))
}

func TestProvide_FailurePath(t *testing.T) {
	held, err := lexerext.Provide(&namedScanner{name: "held"}, "provided-held")
	qt.New(t).Assert(err, qt.IsNil)
	t.Cleanup(held)
	for _, test := range []struct {
		name    string
		scanner lexer.Scanner
		names   []string
		want    string
	}{
		{"a name another registration holds", &namedScanner{}, []string{"provided-free", "provided-held"}, `lexerext: another scanner is registered for provided-held`},
		{"a dialect Ptah describes", &namedScanner{}, []string{"provided-free", "PostgreSQL"}, `lexerext: postgresql is a dialect Ptah describes, .*`},
		{"an empty name", &namedScanner{}, []string{" "}, `lexerext: a scanner needs an engine name`},
		{"no scanner", nil, []string{"provided-free"}, `lexerext: no scanner to register`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			release, err := lexerext.Provide(test.scanner, test.names...)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(release, qt.IsNil)
			c.Assert(lexerext.Provided("provided-free"), qt.IsNil)
		})
	}
}
