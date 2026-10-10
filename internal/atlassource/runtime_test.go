package atlassource_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
)

func sourceRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return builtintest.Runtime()
}

func TestResolveRequiresRuntimeBeforeConnection(t *testing.T) {
	c := qt.New(t)
	set := classifySingle(t, "--from", "postgres://localhost:1/not-contacted")
	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
		Dialect: "postgres",
	})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(state, qt.DeepEquals, atlassource.State{})
}

func TestResolveRequiresRuntimeBeforeSourceValidation(t *testing.T) {
	c := qt.New(t)
	path := filepath.ToSlash(filepath.Join(t.TempDir(), "missing.yaml"))
	set := classifySingle(t, "--to", "file://"+path)
	called := false
	wantErr := errors.New("source validation")
	opts := atlassource.ResolveOptions{
		Dialect:                   "sqlite",
		ValidateLocalSchemaSource: func(string) error { called = true; return wantErr },
	}
	_, err := set.Resolve(t.Context(), opts)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(called, qt.IsFalse)
	// With the runtime supplied, the same path must actually reach validation.
	opts.Runtime = sourceRuntime(c)
	_, err = set.Resolve(t.Context(), opts)
	c.Assert(err, qt.ErrorIs, wantErr)
	c.Assert(called, qt.IsTrue)
}

func TestStartingPointConversionFailureReturnsNoPartialState(t *testing.T) {
	c := qt.New(t)
	wantErr := errors.New("conversion unavailable")
	runtime := failedSourceConversion{Runtime: sourceRuntime(c), err: wantErr}
	state := replayedOnStartingPoint()
	got, err := state.WithoutStartingPoint(t.Context(), atlassource.State{}, "postgres", runtime)
	c.Assert(err, qt.ErrorIs, wantErr)
	c.Assert(got, qt.DeepEquals, atlassource.State{})
	c.Assert(state.DB.Tables, qt.HasLen, 2)
	c.Assert(state.Schema.Tables, qt.HasLen, 0)
}

type failedSourceConversion struct {
	goschematodb.Runtime
	err error
}

func (r failedSourceConversion) ConvertFeatures(context.Context, schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return nil, r.err
}
