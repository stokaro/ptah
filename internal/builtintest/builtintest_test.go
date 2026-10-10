package builtintest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
)

// TestRuntime_IsSharedAndComplete pins both halves of the contract: every
// caller gets the same runtime, and it is the bundled one rather than an
// empty selection.
func TestRuntime_IsSharedAndComplete(t *testing.T) {
	c := qt.New(t)

	first := builtintest.Runtime()
	second := builtintest.Runtime()

	c.Assert(second, qt.Equals, first)
	c.Assert(first.Targets(), qt.Not(qt.HasLen), 0)
	c.Assert(first.Codecs().Definitions(), qt.Not(qt.HasLen), 0)
}
