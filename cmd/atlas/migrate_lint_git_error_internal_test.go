package atlas

// White-box testing required: atlasMigrateLintGitError is the unexported
// compatibility-boundary adapter for one measured git failure. Reaching its
// non-matching branches through the exported command needs a real repository
// per row plus a dev database, which belongs to the tagged integration contour;
// the exact bytes for the matching row are asserted there as well.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationlintreport"
)

// exitStatus128 is the failure a git invocation against an unresolvable
// revision produces. exec.ExitError cannot be constructed with a chosen status
// without running something, so the rows use a stand-in whose Error() text is
// what the adapter prints.
type exitStatus128 struct{}

func (exitStatus128) Error() string { return "exit status 128" }

func TestAtlasMigrateLintGitError(t *testing.T) {
	diffErr := &migrationlintreport.GitCommandError{
		Subcommand: "diff",
		Args: []string{
			"diff", "--name-only", "--diff-filter=ACMR",
			"--end-of-options", "nosuchbranch...HEAD", "--", "migrations",
		},
		Output: "fatal: bad revision 'nosuchbranch...HEAD'",
		Err:    exitStatus128{},
	}

	t.Run("a failed git diff prints the verb and the status alone", func(t *testing.T) {
		c := qt.New(t)
		source := fmt.Errorf("detect git changeset against %q: %w", "nosuchbranch", diffErr)

		got := atlasMigrateLintGitError(source)

		c.Assert(got.Error(), qt.Equals, "git diff: exit status 128")
		// The complete invocation, git's own output and the process failure stay
		// reachable; only the printed bytes change.
		c.Assert(errors.Unwrap(got), qt.Equals, source)
		c.Assert(got, qt.ErrorIs, diffErr.Err)
		var recovered *migrationlintreport.GitCommandError
		c.Assert(got, qt.ErrorAs, &recovered)
		c.Assert(recovered.Output, qt.Equals, "fatal: bad revision 'nosuchbranch...HEAD'")
		c.Assert(recovered.Args, qt.HasLen, 7)
	})

	t.Run("everything else keeps its own diagnostic", func(t *testing.T) {
		revParseErr := &migrationlintreport.GitCommandError{
			Subcommand: "rev-parse",
			Args:       []string{"rev-parse", "--show-toplevel"},
			Output:     "fatal: not a git repository",
			Err:        exitStatus128{},
		}

		tests := []struct {
			name string
			err  error
			want string
		}{
			{
				// The pinned binary reaches its own `git diff` here and reports
				// status 129; this preflight reports 128. Rendering one as the
				// other would print a status no process returned.
				name: "a failed rev-parse preflight is a different event",
				err:  fmt.Errorf("find git repository root: %w", revParseErr),
				want: "find git repository root: git rev-parse --show-toplevel: exit status 128: fatal: not a git repository",
			},
			{
				name: "an error carrying no git failure",
				err:  errors.New("replaying the migration directory: no such table"),
				want: "replaying the migration directory: no such table",
			},
			{
				name: "text that merely looks like the rendered form",
				err:  errors.New("git diff: exit status 128"),
				want: "git diff: exit status 128",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)

				got := atlasMigrateLintGitError(test.err)

				c.Check(got.Error(), qt.Equals, test.want)
				// Non-matching input is returned as-is, not re-wrapped.
				c.Check(got, qt.Equals, test.err)
			})
		}
	})
}
