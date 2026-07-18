package generator

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestBaselineShadowConnectContextAppliesOnlyConfiguredTimeout(t *testing.T) {
	c := qt.New(t)

	ctx, cancel := baselineShadowConnectContext(context.Background(), time.Second)
	defer cancel()
	deadline, ok := ctx.Deadline()
	c.Assert(ok, qt.IsTrue)
	c.Assert(time.Until(deadline) > 0, qt.IsTrue)

	ctx, cancel = baselineShadowConnectContext(context.Background(), 0)
	defer cancel()
	_, ok = ctx.Deadline()
	c.Assert(ok, qt.IsFalse)
}
