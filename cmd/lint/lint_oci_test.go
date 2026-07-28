package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	cmdlint "github.com/stokaro/ptah/cmd/lint"
)

func TestNewLintCommand_OCIFlags(t *testing.T) {
	c := qt.New(t)

	cmd := cmdlint.NewLintCommand()

	c.Assert(cmd.Flags().Lookup("attach"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("plain-http"), qt.IsNotNil)
}

func TestRunLint_AttachRejectsLocalInput(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute("--dir", "testdata/clean", "--attach", "--format", "json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `"error": "--attach requires an OCI migration source"`)
}

func TestRunLint_OCIInputValidation(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute("--dir", "oci://", "--plain-http", "--format", "json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "invalid OCI reference")
}

func TestRunLint_OCIInputRejectsGitBase(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute(
		"--dir",
		"oci://registry.example/acme/migrations:latest",
		"--git-base",
		"main",
		"--format",
		"json",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `"error": "--git-base is not supported with OCI migration sources"`)
}
