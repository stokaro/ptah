package dbtest_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/dbtest"
)

func selectedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestRunners_RequireRuntimeBeforeDatabaseAccess(t *testing.T) {
	c := qt.New(t)
	report, err := dbtest.RunSchemaTest(t.Context(), dbtest.SchemaOptions{DBURL: "invalid://unreachable"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
	report, err = dbtest.RunMigrationTest(t.Context(), dbtest.Options{DBURL: "invalid://unreachable"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err = dbtest.RunSchemaTest(ctx, dbtest.SchemaOptions{Runtime: selectedRuntime(c), DBURL: "invalid://unreachable"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.IsNil)
}
