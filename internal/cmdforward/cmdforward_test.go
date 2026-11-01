package cmdforward_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cmdforward"
)

func TestTarget_HappyPath(t *testing.T) {
	c := qt.New(t)
	forwarder := &cobra.Command{Use: "down", Annotations: map[string]string{cmdforward.Annotation: "migrations down"}}
	c.Assert(cmdforward.Target(forwarder), qt.Equals, "migrations down")
}

func TestTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(cmdforward.Target(&cobra.Command{Use: "plain"}), qt.Equals, "")
}
