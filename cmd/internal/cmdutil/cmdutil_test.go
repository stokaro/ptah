package cmdutil_test

import (
	"bytes"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func TestWrapRunEMapsOrdinaryErrorsToExit2(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "boom"}
	cmd.SetErr(&stderr)
	run := cmdutil.WrapRunE(func(_ *cobra.Command, _ []string) error {
		return errors.New("boom")
	})

	err := run(cmd, nil)

	c.Assert(err, qt.ErrorMatches, "boom")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Equals, "error: boom\n")
}

func TestWrapRunEPreservesExplicitExitCodes(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "diff"}
	cmd.SetErr(&stderr)
	run := cmdutil.WrapRunE(func(_ *cobra.Command, _ []string) error {
		return exitcode.New(1, errors.New("diff found"))
	})

	err := run(cmd, nil)

	c.Assert(err, qt.ErrorMatches, "diff found")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "")
}
