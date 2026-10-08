//go:build integration

package integration_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
)

func lintFeatureRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	return runtime
}
