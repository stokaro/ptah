package schemaserve_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/cli/internal/schemaserve"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestHandlerRequiresRuntimeBeforeServing(t *testing.T) {
	c := qt.New(t)
	handler, err := schemaserve.Handler(t.Context(), schemaserve.Options{DatabaseURL: "sqlite://unused.db"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(handler, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	handler, err = schemaserve.Handler(ctx, schemaserve.Options{Runtime: selectedRuntime(c), DatabaseURL: "sqlite://unused.db"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(handler, qt.IsNil)
}
