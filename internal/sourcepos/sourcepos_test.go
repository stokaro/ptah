package sourcepos_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/sourcepos"
)

func TestLocate(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		offset     int
		wantLine   int
		wantColumn int
	}{
		{name: "an empty document", text: "", offset: 0, wantLine: 1, wantColumn: 1},
		{name: "the first byte", text: "ab", offset: 0, wantLine: 1, wantColumn: 1},
		{name: "on the first line", text: "ab", offset: 1, wantLine: 1, wantColumn: 2},
		{name: "after LF", text: "ab\ncd", offset: 4, wantLine: 2, wantColumn: 2},
		{name: "after CRLF, which is one line ending", text: "ab\r\ncd", offset: 5, wantLine: 2, wantColumn: 2},
		{name: "after a lone CR", text: "ab\rcd", offset: 4, wantLine: 2, wantColumn: 2},
		{name: "every ending in one document", text: "a\nb\r\nc\rd", offset: 7, wantLine: 4, wantColumn: 1},
		{name: "an empty line", text: "a\n\nb", offset: 3, wantLine: 3, wantColumn: 1},
		{name: "on the CR of a CRLF", text: "ab\r\ncd", offset: 2, wantLine: 1, wantColumn: 3},
		{name: "on the LF of a CRLF, still the line it ends", text: "ab\r\ncd", offset: 3, wantLine: 1, wantColumn: 4},
		{name: "a two-byte character is one column", text: "заказ x", offset: 11, wantLine: 1, wantColumn: 7},
		{name: "a four-byte character is one column", text: "😀 x", offset: 5, wantLine: 1, wantColumn: 3},
		{name: "a tab is one column", text: "\t\tx", offset: 2, wantLine: 1, wantColumn: 3},
		{name: "a byte that is not UTF-8 is one column", text: "a\xff\xfex", offset: 3, wantLine: 1, wantColumn: 4},
		{name: "the end, with no final line ending", text: "ab\ncd", offset: 5, wantLine: 2, wantColumn: 3},
		{name: "the end, after a final line ending", text: "ab\n", offset: 3, wantLine: 2, wantColumn: 1},
		{name: "the end, after a final CRLF", text: "ab\r\n", offset: 4, wantLine: 2, wantColumn: 1},
		{name: "the end, after a final CR", text: "ab\r", offset: 3, wantLine: 2, wantColumn: 1},
		{name: "past the end is the end", text: "ab\ncd", offset: 99, wantLine: 2, wantColumn: 3},
		{name: "before the start is the start", text: "ab", offset: -1, wantLine: 1, wantColumn: 1},
		{name: "a newline inside a string counts", text: "'a\nb' x", offset: 6, wantLine: 2, wantColumn: 4},
		{name: "a newline inside a comment counts", text: "/* a\n b */ x", offset: 11, wantLine: 2, wantColumn: 7},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			line, column := sourcepos.Locate(test.text, test.offset)

			c.Assert(line, qt.Equals, test.wantLine)
			c.Assert(column, qt.Equals, test.wantColumn)
		})
	}
}

func identity(offset int) int { return offset }

// A chain stating no offset comes back as it went in: a timeout, or a refusal
// an owner reported about nothing in particular, has no place to report.
func TestResolve_AChainStatingNoOffset(t *testing.T) {
	c := qt.New(t)
	cause := errors.New("no place")

	err := sourcepos.Resolve(cause, "ab", identity)

	c.Assert(err, qt.Equals, cause)
}

// The result reads as the chain did, with the position in front of it, and the
// chain stays reachable.
func TestResolve_KeepsTheChain(t *testing.T) {
	c := qt.New(t)
	refusal := ptaherr.At(3, fmt.Errorf("feature: %w", ptaherr.ErrUnsupportedFeature))

	err := sourcepos.Resolve(fmt.Errorf("expected a name: %w", refusal), "a\nbcd", identity)

	c.Assert(err, qt.ErrorMatches, `line 2, column 2: expected a name: feature: unsupported feature`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	at, ok := errors.AsType[*ptaherr.ParseError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(at.Offset, qt.Equals, 3)
	c.Assert(at.Line, qt.Equals, 2)
	c.Assert(at.Column, qt.Equals, 2)
}

// A statement's offset around a clause's offset: the clause is the more exact.
func TestResolve_TheInnermostOffsetWins(t *testing.T) {
	c := qt.New(t)
	clause := ptaherr.At(4, errors.New("expected a word"))

	err := sourcepos.Resolve(ptaherr.At(1, fmt.Errorf("ALTER: %w", clause)), "abcdefg", identity)

	c.Assert(err, qt.ErrorMatches, `line 1, column 5: ALTER: expected a word`)
}

// An offset in rewritten text is reported against the text as written.
func TestResolve_MapsTheOffsetThroughOrigin(t *testing.T) {
	c := qt.New(t)

	err := sourcepos.Resolve(ptaherr.At(0, errors.New("stop")), "a\nb", func(offset int) int { return offset + 2 })

	c.Assert(err, qt.ErrorMatches, `line 2, column 1: stop`)
	at, ok := errors.AsType[*ptaherr.ParseError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(at.Offset, qt.Equals, 2)
}

// A refusal of a piece counts from the piece's start until its reader moves it,
// and every offset the chain states moves together.
func TestShift_MovesEveryOffsetInTheChain(t *testing.T) {
	c := qt.New(t)
	clause := ptaherr.At(2, errors.New("expected a word"))
	outer := ptaherr.At(0, fmt.Errorf("statement: %w", clause))

	err := sourcepos.Shift(outer, 10)

	c.Assert(err, qt.Equals, outer)
	at, ok := errors.AsType[*ptaherr.ParseError](clause)
	c.Assert(ok, qt.IsTrue)
	c.Assert(at.Offset, qt.Equals, 12)
	at, ok = errors.AsType[*ptaherr.ParseError](outer)
	c.Assert(ok, qt.IsTrue)
	c.Assert(at.Offset, qt.Equals, 10)
}

func TestShift_AChainStatingNoOffset(t *testing.T) {
	c := qt.New(t)
	cause := errors.New("no place")

	c.Assert(sourcepos.Shift(cause, 10), qt.Equals, cause)
	c.Assert(sourcepos.Shift(nil, 10), qt.IsNil)
}

func TestLinesLocateBytes(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		offset     int
		wantLine   int
		wantColumn int
	}{
		{name: "an empty document", text: "", offset: 0, wantLine: 1, wantColumn: 1},
		{name: "on the first line", text: "ab", offset: 1, wantLine: 1, wantColumn: 2},
		{name: "after LF", text: "ab\ncd", offset: 4, wantLine: 2, wantColumn: 2},
		{name: "after CRLF, which is one line ending", text: "ab\r\ncd", offset: 5, wantLine: 2, wantColumn: 2},
		{name: "after a lone CR", text: "ab\rcd", offset: 4, wantLine: 2, wantColumn: 2},
		{name: "on the LF of a CRLF, still the line it ends", text: "ab\r\ncd", offset: 3, wantLine: 1, wantColumn: 4},
		{name: "a two-byte character is two columns", text: "заказ x", offset: 11, wantLine: 1, wantColumn: 12},
		{name: "a four-byte character is four columns", text: "😀 x", offset: 5, wantLine: 1, wantColumn: 6},
		{name: "the end, after a final line ending", text: "ab\n", offset: 3, wantLine: 2, wantColumn: 1},
		{name: "past the end is the end", text: "ab\ncd", offset: 99, wantLine: 2, wantColumn: 3},
		{name: "before the start is the start", text: "ab", offset: -1, wantLine: 1, wantColumn: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			line, column := sourcepos.NewLines(test.text).LocateBytes(test.offset)

			c.Assert(line, qt.Equals, test.wantLine)
			c.Assert(column, qt.Equals, test.wantColumn)
		})
	}
}
