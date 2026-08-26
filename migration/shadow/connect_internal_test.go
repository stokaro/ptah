package shadow

// White-box testing required: the connect budget is a derivation, not an
// outcome. Whether the returned context carries a deadline is decided when it
// is built, and asserting that a small budget fires instead would depend on
// timer granularity rather than on the rule under test.

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestConnectContextAppliesOnlyConfiguredTimeout(t *testing.T) {
	c := qt.New(t)

	ctx, cancel := connectContext(context.Background(), time.Second)
	defer cancel()
	deadline, ok := ctx.Deadline()
	c.Assert(ok, qt.IsTrue)
	c.Assert(time.Until(deadline) > 0, qt.IsTrue)

	ctx, cancel = connectContext(context.Background(), 0)
	defer cancel()
	_, ok = ctx.Deadline()
	c.Assert(ok, qt.IsFalse)
}
