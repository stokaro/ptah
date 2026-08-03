package cmdutil_test

import (
	"bytes"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
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

func TestNormalizeCommandError_MapsCobraFlagGroupErrors(t *testing.T) {
	c := qt.New(t)

	cmd := &cobra.Command{
		Use:  "root",
		RunE: func(_ *cobra.Command, _ []string) error { return nil },
	}
	cmd.Flags().Bool("first", false, "First choice")
	cmd.Flags().Bool("second", false, "Second choice")
	cmd.MarkFlagsMutuallyExclusive("first", "second")
	cmdutil.SetErrorCodePolicy(cmd, 1)
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--first", "--second"})

	executed, err := cmd.ExecuteC()
	err = cmdutil.NormalizeCommandError(executed, err, 2)

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[first second\] are set none of the others can be; \[first second\] were all set`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: if any flags in the group [first second] are set none of the others can be; [first second] were all set\n")
}

func TestNormalizeCommandError_MapsLateDescendantErrors(t *testing.T) {
	c := qt.New(t)

	root := &cobra.Command{Use: "root"}
	cmdutil.SetErrorCodePolicy(root, 1)
	root.AddCommand(&cobra.Command{
		Use:  "late",
		RunE: commandError("late failure"),
	})
	var stderr bytes.Buffer
	root.SetErr(&stderr)
	root.SetArgs([]string{"late"})

	executed, err := root.ExecuteC()
	err = cmdutil.NormalizeCommandError(executed, err, 2)

	c.Assert(err, qt.ErrorMatches, "late failure")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: late failure\n")
}

func TestNormalizeCommandError_PreservesNativeExplicitExitCode(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "native"}
	cmd.SetErr(&stderr)
	err := cmdutil.NormalizeCommandError(cmd, exitcode.New(1, errors.New("drift")), 2)

	c.Assert(err, qt.ErrorMatches, "drift")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestNormalizeCommandError_RemapExplicitErrorWithoutDuplicateOutput(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "atlas"}
	cmd.SetErr(&stderr)
	cmdutil.SetErrorCodePolicy(cmd, 1)
	_, err := stderr.WriteString("error: boom\n")
	c.Assert(err, qt.IsNil)

	err = cmdutil.NormalizeCommandError(cmd, exitcode.New(2, errors.New("boom")), 2)

	c.Assert(err, qt.ErrorMatches, "boom")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "error: boom\n")
}

func TestExactArgs_HappyPath(t *testing.T) {
	c := qt.New(t)

	var stderr bytes.Buffer
	cmd := &cobra.Command{Use: "push"}
	cmd.SetErr(&stderr)

	err := cmdutil.ExactArgs(1)(cmd, []string{"oci://registry.example/acme/migrations"})

	c.Assert(err, qt.IsNil)
	c.Assert(stderr.String(), qt.Equals, "")
}

func TestExactArgs_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing argument",
			args:    nil,
			wantErr: "expected exactly 1 positional argument\\(s\\), got 0",
		},
		{
			name:    "extra argument",
			args:    []string{"first", "second"},
			wantErr: "expected exactly 1 positional argument\\(s\\), got 2",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			var stderr bytes.Buffer
			cmd := &cobra.Command{Use: "push"}
			cmd.SetErr(&stderr)

			err := cmdutil.ExactArgs(1)(cmd, tt.args)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(stderr.String(), qt.Matches, "error: "+tt.wantErr+"\n")
		})
	}
}

// TestErrorPrefixPolicyGovernsEveryPrinter pins that the diagnostic prefix is
// resolved from the command tree rather than hardcoded at each print site
// (stokaro/ptah#1019). Every printer in this package is covered, because the
// defect being prevented is a surface whose prefix depends on which route a
// given failure happens to take: before the policy existed, one command
// overriding one printer was enough to give a single binary two prefixes for
// the same class of failure.
//
// The "inherited by a descendant" rows are the load-bearing ones: a surface
// declares the prefix once on its root, and hundreds of call sites below it
// answer with it without naming it.
func TestErrorPrefixPolicyGovernsEveryPrinter(t *testing.T) {
	tests := []struct {
		name string
		// emit exercises one printer against a command tree whose root has
		// already had the policy under test applied.
		emit func(root, leaf *cobra.Command) error
	}{
		{
			name: "Fail",
			emit: func(_, leaf *cobra.Command) error {
				return cmdutil.Fail(leaf, errors.New("boom"))
			},
		},
		{
			name: "FlagErrorFunc",
			emit: func(_, leaf *cobra.Command) error {
				return cmdutil.FlagErrorFunc(leaf, errors.New("boom"))
			},
		},
		{
			name: "WrapRunE",
			emit: func(_, leaf *cobra.Command) error {
				return cmdutil.WrapRunE(commandError("boom"))(leaf, nil)
			},
		},
		{
			// NormalizeCommandError's configured-policy branch.
			name: "NormalizeCommandError under an exit-code policy",
			emit: func(root, leaf *cobra.Command) error {
				cmdutil.SetErrorCodePolicy(root, 1)
				return cmdutil.NormalizeCommandError(leaf, errors.New("boom"), 2)
			},
		},
		{
			// NormalizeCommandError's fallback branch, reached when no
			// ancestor declares an exit code.
			name: "NormalizeCommandError without an exit-code policy",
			emit: func(_, leaf *cobra.Command) error {
				return cmdutil.NormalizeCommandError(leaf, errors.New("boom"), 2)
			},
		},
	}

	policies := []struct {
		name  string
		apply func(*cobra.Command)
		want  string
	}{
		{
			name:  "no declared policy prints the native prefix",
			apply: func(*cobra.Command) {},
			want:  "error: boom\n",
		},
		{
			name:  "a declared policy replaces the native prefix",
			apply: func(root *cobra.Command) { cmdutil.SetErrorPrefixPolicy(root, "Error") },
			want:  "Error: boom\n",
		},
	}

	for _, tt := range tests {
		for _, policy := range policies {
			t.Run(tt.name+"/"+policy.name, func(t *testing.T) {
				c := qt.New(t)

				var stderr bytes.Buffer
				root := &cobra.Command{Use: "root"}
				leaf := &cobra.Command{Use: "leaf"}
				root.AddCommand(leaf)
				root.SetErr(&stderr)
				policy.apply(root)

				err := tt.emit(root, leaf)

				c.Assert(err, qt.ErrorMatches, "boom")
				c.Assert(stderr.String(), qt.Equals, policy.want)
			})
		}
	}
}

// TestAdoptErrorPrefixPolicyRestoresTheTargetPolicy pins the restore discipline
// forwarding adapters depend on. Command factories hand back package-level
// singletons, so a target that adopts the compat prefix and never gives it back
// leaks "Error: " into the next native execution in the same process.
func TestAdoptErrorPrefixPolicyRestoresTheTargetPolicy(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(*cobra.Command)
		wantRestored  string
		wantWhileHeld string
	}{
		{
			name:          "target without a policy of its own",
			configure:     func(*cobra.Command) {},
			wantWhileHeld: "Error: boom\n",
			wantRestored:  "error: boom\n",
		},
		{
			name:          "target with a policy of its own",
			configure:     func(target *cobra.Command) { cmdutil.SetErrorPrefixPolicy(target, "target") },
			wantWhileHeld: "Error: boom\n",
			wantRestored:  "target: boom\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			var stderr bytes.Buffer
			source := &cobra.Command{Use: "source"}
			cmdutil.SetErrorPrefixPolicy(source, "Error")
			target := &cobra.Command{Use: "target"}
			target.SetErr(&stderr)
			tt.configure(target)

			restore := cmdutil.AdoptErrorPrefixPolicy(target, source)
			cmdutil.Fail(target, errors.New("boom"))
			held := stderr.String()
			stderr.Reset()
			restore()
			cmdutil.Fail(target, errors.New("boom"))

			c.Assert(held, qt.Equals, tt.wantWhileHeld)
			c.Assert(stderr.String(), qt.Equals, tt.wantRestored)
		})
	}
}

// TestAdoptErrorPrefixPolicyRestoresTheAnnotationMap pins that the restore is
// a restore of the annotations, not only of the one key it wrote.
// [cmdutil.SetErrorPrefixPolicy] allocates the map when a command has none, so
// a restore that only deletes its key hands back a command whose Annotations
// changed from nil to an allocated empty map. Forwarded targets are
// package-level singletons that survive the execution, so that difference
// outlives the call and is visible to anything that reads Annotations rather
// than a single key.
//
// The second row is the control: the restore must not reach beyond its own key
// and clear annotations the target already carried.
func TestAdoptErrorPrefixPolicyRestoresTheAnnotationMap(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*cobra.Command)
		want      map[string]string
	}{
		{
			name:      "target that carried no annotations at all",
			configure: func(*cobra.Command) {},
			want:      nil,
		},
		{
			name: "target that carried unrelated annotations",
			configure: func(target *cobra.Command) {
				target.Annotations = map[string]string{"ptah.unrelated": "kept"}
			},
			want: map[string]string{"ptah.unrelated": "kept"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			source := &cobra.Command{Use: "source"}
			cmdutil.SetErrorPrefixPolicy(source, "Error")
			target := &cobra.Command{Use: "target"}
			tt.configure(target)

			restore := cmdutil.AdoptErrorPrefixPolicy(target, source)
			held := cmdutil.ErrorPrefix(target)
			restore()

			c.Assert(held, qt.Equals, "Error")
			c.Assert(target.Annotations, qt.DeepEquals, tt.want)
		})
	}
}

// TestSetErrorPrefixPolicyRejectsAnEmptyPrefix pins that a surface cannot
// declare "no prefix". Every printer in this package writes
// "<prefix>: <message>", so an empty prefix has no printable meaning; before
// this was rejected the call was silently ignored and the surface resolved to
// whatever an ancestor declared, or to the native prefix — the exact class of
// accident stokaro/ptah#1019 removed from the compat surface.
func TestSetErrorPrefixPolicyRejectsAnEmptyPrefix(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*cobra.Command)
	}{
		{
			name:      "command with no annotations",
			configure: func(*cobra.Command) {},
		},
		{
			name: "command that already declares a prefix",
			configure: func(cmd *cobra.Command) {
				cmdutil.SetErrorPrefixPolicy(cmd, "Error")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cmd := &cobra.Command{Use: "surface"}
			tt.configure(cmd)

			c.Assert(
				func() { cmdutil.SetErrorPrefixPolicy(cmd, "") },
				qt.PanicMatches,
				`cmdutil: empty error prefix policy for command surface`,
			)
		})
	}
}

func commandError(message string) func(*cobra.Command, []string) error {
	return func(_ *cobra.Command, _ []string) error {
		return errors.New(message)
	}
}
