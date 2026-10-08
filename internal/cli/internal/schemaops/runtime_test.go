package schemaops_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/schemaops"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestCompareRequiresRuntimeBeforeDatabaseAccess(t *testing.T) {
	c := qt.New(t)
	result, err := schemaops.Compare(t.Context(), schemaops.CompareOptions{DatabaseURL: "invalid://unreachable"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}

type comparisonRuntime struct {
	*engine.Runtime
	received context.Context
	request  schemaext.ComparisonRequest
	failure  error
}

func (r *comparisonRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	r.received = ctx
	r.request = request
	return schemaext.ComparisonResult{}, r.failure
}

func TestCompareUsesSelectedRuntimeAndReturnsNoPartialResult(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	failure := errors.New("selected comparison failed")
	runtime := &comparisonRuntime{Runtime: selectedRuntime(c), failure: failure}
	result, err := schemaops.Compare(t.Context(), schemaops.CompareOptions{
		Runtime: runtime, DatabaseURL: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "live.db")), SchemaFiles: []string{schemaFile},
	})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
	c.Assert(runtime.received, qt.Equals, t.Context())
	c.Assert(runtime.request.Target, qt.Equals, "sqlite")
}
