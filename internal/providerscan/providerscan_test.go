package providerscan_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/lexer"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
)

// counted is scantest.Scan that counts its calls and records the last head it
// was given.
func counted(calls *atomic.Int32, heads *atomic.Bool) providerscan.Scan {
	return func(ctx context.Context, text string, head bool) (*protocol.SQLScan, error) {
		calls.Add(1)
		heads.Store(head)
		return scantest.Scan(ctx, text, head)
	}
}

// tokens reads every token of a lexer over text with scanner, up to the end.
func tokens(scanner lexer.Scanner, text string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(text, lexer.Options{Scanner: scanner})
	var read []lexer.Token
	for {
		token := lex.NextToken()
		read = append(read, token)
		if token.Type == lexer.TokenEOF {
			return read
		}
	}
}

// The provider's tokens reach the lexer with their text and offsets, the
// strings and names read back to what they stand for, and a text read twice
// is scanned once.
func TestScanner_HappyPath(t *testing.T) {
	c := qt.New(t)
	var calls atomic.Int32
	var heads atomic.Bool
	scanner := providerscan.New(counted(&calls, &heads))
	text := "SELECT @@it's@@ FROM `shop/items`;"

	got := tokens(scanner, text)

	c.Assert(got, qt.DeepEquals, []lexer.Token{
		{Type: lexer.TokenIdentifier, Value: "SELECT", Start: 0, End: 6},
		{Type: lexer.TokenWhitespace, Value: " ", Start: 6, End: 7},
		{Type: lexer.TokenString, Value: "@@it's@@", Start: 7, End: 15},
		{Type: lexer.TokenWhitespace, Value: " ", Start: 15, End: 16},
		{Type: lexer.TokenIdentifier, Value: "FROM", Start: 16, End: 20},
		{Type: lexer.TokenWhitespace, Value: " ", Start: 20, End: 21},
		{Type: lexer.TokenIdentifier, Value: "`shop/items`", Start: 21, End: 33},
		{Type: lexer.TokenSemicolon, Value: ";", Start: 33, End: 34},
		{Type: lexer.TokenEOF, Start: 34, End: 34},
	})
	c.Assert(heads.Load(), qt.IsTrue)
	value, ok := scanner.StringValue("@@it's@@")
	c.Assert(ok, qt.IsTrue)
	c.Assert(value, qt.Equals, "it's")
	name, ok := scanner.IdentifierValue("`shop/items`")
	c.Assert(ok, qt.IsTrue)
	c.Assert(name, qt.Equals, "shop/items")
	c.Assert(tokens(scanner, text), qt.DeepEquals, got)
	c.Assert(calls.Load(), qt.Equals, int32(1))
}

// A range is scanned from its start as text after the head, with offsets in
// the whole text.
func TestScanner_Range(t *testing.T) {
	c := qt.New(t)
	var calls atomic.Int32
	var heads atomic.Bool
	scanner := providerscan.New(counted(&calls, &heads))
	text := "SELECT 'a;b'"

	ranged := lexer.NewLexerWithOptions(text, lexer.Options{Scanner: scanner}).Range(7, len(text))

	c.Assert(ranged.NextToken(), qt.DeepEquals, lexer.Token{Type: lexer.TokenString, Value: "'a;b'", Start: 7, End: 12})
	c.Assert(ranged.NextToken(), qt.DeepEquals, lexer.Token{Type: lexer.TokenEOF, Start: 12, End: 12})
	c.Assert(heads.Load(), qt.IsFalse)
}

// A value the scans so far have not read is asked for by scanning the token
// alone. Text that is not one token of the kind asked for stands for no
// value.
func TestScanner_ValueOfAnUnscannedToken(t *testing.T) {
	for _, test := range []struct {
		name, token string
		identifier  bool
		want        string
		read        bool
	}{
		{name: "a string", token: `"fixed"`, want: "fixed", read: true},
		{name: "a quoted name", token: "`a``b`", identifier: true, want: "a`b", read: true},
		{name: "a name taken for a string", token: "`a`", read: false},
		{name: "two tokens", token: "a b", identifier: true, read: false},
		{name: "an empty text", token: "", identifier: true, read: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scanner := providerscan.New(scantest.Scan)
			valueOf := map[bool]func(string) (string, bool){false: scanner.StringValue, true: scanner.IdentifierValue}[test.identifier]
			value, read := valueOf(test.token)
			c.Assert(read, qt.Equals, test.read)
			c.Assert(value, qt.Equals, test.want)
		})
	}
}

// A scan that fails, or that the provider answers with tokens that do not
// cover the text, is one unknown token over the rest of the text, and it is
// asked again the next time rather than kept.
func TestScanner_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply *protocol.SQLScan
		err   error
	}{
		{name: "an error", err: errors.New("provider scan SQL failed (Unavailable)")},
		{name: "a token past the text", reply: &protocol.SQLScan{Tokens: []*protocol.SQLToken{{Kind: protocol.SQLToken_STRING, End: 99}}}},
		{name: "tokens short of the text", reply: &protocol.SQLScan{Tokens: []*protocol.SQLToken{{Kind: protocol.SQLToken_STRING, End: 2}}}},
		{name: "a kind the host does not know", reply: &protocol.SQLScan{Tokens: []*protocol.SQLToken{{Kind: protocol.SQLToken_Kind(99), End: 5}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var calls atomic.Int32
			scanner := providerscan.New(func(context.Context, string, bool) (*protocol.SQLScan, error) {
				calls.Add(1)
				return test.reply, test.err
			})
			c.Assert(tokens(scanner, "'a;b'"), qt.DeepEquals, []lexer.Token{
				{Type: lexer.TokenUnknown, Value: "'a;b'", Start: 0, End: 5},
				{Type: lexer.TokenEOF, Start: 5, End: 5},
			})
			value, read := scanner.StringValue("'a;b'")
			c.Assert(read, qt.IsFalse)
			c.Assert(value, qt.Equals, "")
			tokens(scanner, "'a;b'")
			c.Assert(calls.Load(), qt.Equals, int32(3))
		})
	}
}
