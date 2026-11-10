package root_test

import (
	"bytes"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/root"
)

// A later statement error can quote an address that connected successfully.
// The command records the address during ConnectToDatabase and hides its secret
// before Fail writes the driver's error to stderr.
func TestRunContextRedactsLaterStatementError(t *testing.T) {
	c := qt.New(t)
	const address = "sqlite://:memory:?password=Pa55wordX9"
	var stderr bytes.Buffer
	command := &cobra.Command{Use: "statement", RunE: func(cmd *cobra.Command, _ []string) error {
		conn, err := dbschema.ConnectToDatabase(cmd.Context(), address)
		c.Assert(err, qt.IsNil)
		defer func() { c.Check(conn.Close(), qt.IsNil) }()
		_, err = conn.ExecContext(cmd.Context(), "CREATE TABLE records (id integer)")
		c.Assert(err, qt.IsNil)
		return cmdutil.Fail(cmd, errors.New("statement failed at Pa55wordX9"))
	}}
	cmdutil.ConfigureCommand(command)
	parent := &cobra.Command{Use: "ptah"}
	parent.AddCommand(command)
	parent.SetErr(&stderr)

	code := root.RunContext(t.Context(), parent, "statement")

	c.Assert(code, qt.Equals, 2)
	c.Assert(stderr.String(), qt.Equals, "error: statement failed at ***\n")
}
