package migrator

// White-box testing required: the length is the one the ClickHouse driver
// writes, and the public API observes it only as a live server's refusal of
// a whole statement. The live test pins the default limit; this test pins
// each escaped byte class and the exact boundary, which a live run cannot
// separate from the rest of the statement.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestInlinedStatementLength_HappyPath(t *testing.T) {
	// The query is one byte, and a literal adds its two quotes.
	tests := []struct {
		name string
		arg  any
		want int
	}{
		{name: "a plain string is its bytes and two quotes", arg: "ab", want: 1 + 2 + 2},
		{name: "a single quote counts twice", arg: "a'b", want: 1 + 4 + 2},
		{name: "a backslash counts twice", arg: `a\b`, want: 1 + 4 + 2},
		{name: "a NUL byte counts twice", arg: "a\x00b", want: 1 + 4 + 2},
		{name: "a byte slice escapes each class", arg: []byte("'\\\x00"), want: 1 + 6 + 2},
		{name: "a multibyte character counts its bytes", arg: "é", want: 1 + 2 + 2},
		{name: "a timestamp counts 64 bytes", arg: time.Unix(0, 0).UTC(), want: 1 + 64},
		{name: "another value counts its printed form", arg: int64(12345), want: 1 + 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(inlinedStatementLength("q", []any{tt.arg}), qt.Equals, tt.want)
		})
	}
}

func TestRefuseStatementOverLimit_HappyPath(t *testing.T) {
	c := qt.New(t)
	// "q" and the literal 'ab' are five bytes.
	c.Assert(refuseStatementOverLimit("R__widgets.sql", "q", []any{"ab"}, 5, "max_query_size"), qt.IsNil)
}

func TestRefuseStatementOverLimit_FailurePath(t *testing.T) {
	c := qt.New(t)
	// "q" and the literal 'abc' are six bytes, one over the limit.
	err := refuseStatementOverLimit("R__widgets.sql", "q", []any{"abc"}, 5, "max_query_size")

	var tooLong *RepeatableStatementTooLongError
	c.Assert(err, qt.ErrorAs, &tooLong)
	c.Assert(*tooLong, qt.DeepEquals, RepeatableStatementTooLongError{
		Path: "R__widgets.sql", Length: 6, Limit: 5, Setting: "max_query_size",
	})
}
