package atlasmigrate_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/builtintest"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return builtintest.Runtime()
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
