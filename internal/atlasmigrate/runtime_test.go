package atlasmigrate_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasmigrate"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestGenerateDiffRequiresRuntimeBeforeDatabaseAccess(t *testing.T) {
	c := qt.New(t)
	result, err := atlasmigrate.GenerateDiff(t.Context(), nil, atlasmigrate.DiffOptions{})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, atlasmigrate.DiffResult{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = atlasmigrate.GenerateDiff(ctx, nil, atlasmigrate.DiffOptions{Runtime: selectedRuntime(c)})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, atlasmigrate.DiffResult{})
}
