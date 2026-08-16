package processcapture

// White-box testing required: cappedBuffer and tailBuffer are unexported
// memory-safety guards whose exact truncation state is not observable through
// the public API without producing very large subprocess fixtures.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCappedBuffer_KeepsOutputWithinLimit(t *testing.T) {
	c := qt.New(t)

	b := &cappedBuffer{limit: 8}
	n, err := b.Write([]byte("abcd"))

	c.Assert(err, qt.IsNil)
	c.Assert(n, qt.Equals, 4)
	c.Assert(b.String(), qt.Equals, "abcd")
	c.Assert(b.truncated, qt.IsFalse)
}

func TestCappedBuffer_TruncatesPastLimit(t *testing.T) {
	c := qt.New(t)

	b := &cappedBuffer{limit: 4}
	n, err := b.Write([]byte("abcdef"))

	c.Assert(err, qt.IsNil)
	c.Assert(n, qt.Equals, 6) // reports a full write so the child is not killed
	c.Assert(b.String(), qt.Equals, "abcd")
	c.Assert(b.truncated, qt.IsTrue)
}

func TestCappedBuffer_TruncatesOnLaterWrite(t *testing.T) {
	c := qt.New(t)

	b := &cappedBuffer{limit: 4}
	_, _ = b.Write([]byte("abcd"))
	_, err := b.Write([]byte("ef"))

	c.Assert(err, qt.IsNil)
	c.Assert(b.String(), qt.Equals, "abcd")
	c.Assert(b.truncated, qt.IsTrue)
}

func TestTailBuffer_KeepsLatestBytesAcrossWrites(t *testing.T) {
	c := qt.New(t)

	b := &tailBuffer{limit: 8}
	_, _ = b.Write([]byte("abcdef"))
	n, err := b.Write([]byte("ghijkl"))

	c.Assert(err, qt.IsNil)
	c.Assert(n, qt.Equals, 6)
	c.Assert(b.String(), qt.Equals, "efghijkl")
	c.Assert(b.truncated, qt.IsTrue)
}

func TestTailBuffer_KeepsTailOfOversizedWrite(t *testing.T) {
	c := qt.New(t)

	b := &tailBuffer{limit: 4}
	n, err := b.Write([]byte("abcdef"))

	c.Assert(err, qt.IsNil)
	c.Assert(n, qt.Equals, 6)
	c.Assert(b.String(), qt.Equals, "cdef")
	c.Assert(b.truncated, qt.IsTrue)
}
