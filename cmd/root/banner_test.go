package root_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
)

// wordmark is one line of the shared banner, enough to find it in a stream.
//
// Written out rather than imported from cmd/internal/banner: this asserts what
// a consumer of the command's output sees, and asking the banner package what
// it emits would agree with whatever it emitted.
const wordmark = `|  ___/| |_ | (_| | | | |`

// TestRootCommand_KeepsTheBannerOutOfCapturedOutput is the guarantee every
// caller that reads this command's output depends on.
//
// A bare `ptah` is the entry screen, and it is also what a script runs to get
// the command list. The banner belongs to the first reader and not the second,
// so the writer decides: an in-process buffer is never a terminal, and this is
// the shape every test in this package and every consumer piping stdout has.
//
// Each row's control is the help itself. Without it the assertion passes for a
// command that produced nothing at all, which is the failure a "does not
// contain" test cannot otherwise tell from success.
func TestRootCommand_KeepsTheBannerOutOfCapturedOutput(t *testing.T) {
	// An empty non-nil slice, never nil: cobra reads os.Args[1:] when SetArgs
	// is given nil, so a nil row would run this test binary's own flags
	// through the command and assert against whatever `go test` was invoked
	// with.
	tests := []struct {
		name string
		args []string
	}{
		{name: "the entry screen", args: make([]string, 0)},
		{name: "an explicit help flag", args: []string{"--help"}},
		{name: "the help subcommand", args: []string{"help"}},
		{name: "a namespace's own entry screen", args: []string{"schema"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := root.NewRootCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(test.args)

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, "Usage:")
			c.Assert(out.String(), qt.Not(qt.Contains), wordmark)
		})
	}
}
