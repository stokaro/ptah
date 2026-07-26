package schemasource

// White-box testing required: cappedBuffer is an unexported guard against
// runaway command output, and the surviving-child timeout test needs to shorten
// the unexported waitDelay so it runs fast.

import (
	"context"
	"os"
	"testing"
	"time"

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

// TestRun_TimeoutBoundedDespiteSurvivingChild proves the WaitDelay guard: a
// program that leaves a grandchild holding the stdout pipe open must not block
// Run past the timeout (the grandchild sleeps far longer than any wait here).
func TestRun_TimeoutBoundedDespiteSurvivingChild(t *testing.T) {
	c := qt.New(t)

	restore := waitDelay
	waitDelay = 200 * time.Millisecond
	defer func() { waitDelay = restore }()

	start := time.Now()
	_, err := Run(context.Background(), Command{
		Args:    []string{os.Args[0], "-test.run=TestHelperProcess"},
		Env:     []string{"GO_WANT_HELPER_PROCESS=1", "SCHEMASOURCE_HELPER_MODE=orphan"},
		Timeout: 200 * time.Millisecond,
	})
	elapsed := time.Since(start)

	c.Assert(err, qt.IsNotNil)
	c.Assert(elapsed < 10*time.Second, qt.IsTrue)
}
