package exitcode_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestCodeReturnsWrappedExitCode(t *testing.T) {
	c := qt.New(t)

	err := fmt.Errorf("wrapped: %w", exitcode.New(2, fmt.Errorf("boom")))

	c.Assert(exitcode.Code(err, 1), qt.Equals, 2)
}

func TestCodeReturnsFallback(t *testing.T) {
	c := qt.New(t)

	c.Assert(exitcode.Code(fmt.Errorf("boom"), 1), qt.Equals, 1)
}
