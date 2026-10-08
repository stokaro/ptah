package atlasmigrateimport_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

func TestImportRequiresContextBeforeCapture(t *testing.T) {
	c := qt.New(t)
	var missingContext context.Context
	missing, err := atlasmigrateimport.Import(missingContext, atlasmigrateimport.Options{})
	c.Assert(err, qt.ErrorMatches, "migration import requires a context")
	c.Assert(missing, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled, err := atlasmigrateimport.Import(ctx, atlasmigrateimport.Options{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(canceled, qt.IsNil)
}

func TestCapturedImportRequiresContextBeforeConversion(t *testing.T) {
	c := qt.New(t)
	captured := &atlasmigrateimport.CapturedImport{}
	var missingContext context.Context
	missing, err := captured.Write(missingContext)
	c.Assert(err, qt.ErrorMatches, "migration import requires a context")
	c.Assert(missing, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	canceled, err := captured.Write(ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(canceled, qt.IsNil)
}
