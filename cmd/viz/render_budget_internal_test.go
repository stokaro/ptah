package viz

// White-box testing required: the property is which of two deadlines governs a
// `dot` invocation, and the only way to ask it through the exported command is
// to outlast the real ten-second budget. renderDOTToSVG takes the budget as an
// argument so a test can answer in milliseconds; that argument is unexported,
// and so is the function.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/testutils"
)

// TestRenderDOTToSVG_TheCallersDeadlineWins is the fix.
//
// The budget used to be applied unconditionally, so a caller that had allowed
// five seconds got fifty milliseconds. Here `dot` takes longer than the budget
// and less than the caller's deadline, and the render has to succeed: under the
// unconditional version it fails with a deadline the caller never set.
func TestRenderDOTToSVG_TheCallersDeadlineWins(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	installSleepingDot(c, t, "0.4")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := renderDOTToSVG(ctx, []byte("digraph{}"), 50*time.Millisecond)

	c.Assert(err, qt.IsNil)
	c.Assert(string(out), qt.Equals, "rendered\n")
}

// TestRenderDOTToSVG_TheBudgetAppliesWhenNobodyNamedOne is the other half, and
// the control for the test above.
//
// Without it, deleting the budget entirely would leave that test passing: a
// caller's deadline governs trivially when nothing competes with it.
func TestRenderDOTToSVG_TheBudgetAppliesWhenNobodyNamedOne(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	installSleepingDot(c, t, "0.4")

	_, err := renderDOTToSVG(context.Background(), []byte("digraph{}"), 50*time.Millisecond)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

// installSleepingDot puts a `dot` on PATH that takes seconds to answer.
func installSleepingDot(c *qt.C, t *testing.T, seconds string) {
	c.Helper()
	binDir := t.TempDir()
	script := "#!/bin/sh\nsleep " + seconds + "\necho rendered\n"
	path := filepath.Join(binDir, "dot")
	c.Assert(os.WriteFile(path, []byte(script), 0o600), qt.IsNil)
	c.Assert(os.Chmod(path, 0o700), qt.IsNil)
	t.Setenv("PATH", binDir)
}
