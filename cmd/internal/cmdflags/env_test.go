package cmdflags_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

func TestEnvNameNormalizesFlagName(t *testing.T) {
	c := qt.New(t)

	c.Assert(cmdflags.EnvName("PTAH", "db-url"), qt.Equals, "PTAH_DB_URL")
	c.Assert(cmdflags.EnvName("PTAH", "migration.lock-timeout"), qt.Equals, "PTAH_MIGRATION_LOCK_TIMEOUT")
}

func TestEnvBindingNameSkipsExplicitOnlyFlags(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("url", "", "Database URL")
	flags.Bool("auto-approve", false, "Skip approval")
	qt.Assert(t, cmdflags.DisableEnvBinding(flags, "auto-approve"), qt.IsNil)

	name, ok := cmdflags.EnvBindingName("PTAH", flags.Lookup("url"))
	qt.Assert(t, ok, qt.IsTrue)
	qt.Assert(t, name, qt.Equals, "PTAH_URL")
	name, ok = cmdflags.EnvBindingName("PTAH", flags.Lookup("auto-approve"))
	qt.Assert(t, ok, qt.IsFalse)
	qt.Assert(t, name, qt.Equals, "")
}

func TestInitializeEnvAppliesEnvironmentDefaults(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "postgres://example")
	t.Setenv("PTAH_VERBOSE", "true")

	var dbURL string
	var verbose bool
	root := &cobra.Command{Use: "ptah"}
	child := &cobra.Command{Use: "up"}
	child.Flags().StringVar(&dbURL, "db-url", "", "Database URL")
	child.Flags().BoolVar(&verbose, "verbose", false, "Verbose output")
	root.AddCommand(child)

	err := cmdflags.InitializeEnv("PTAH", child)

	c.Assert(err, qt.IsNil)
	c.Assert(dbURL, qt.Equals, "postgres://example")
	c.Assert(verbose, qt.IsTrue)
	c.Assert(child.Flags().Lookup("db-url").Changed, qt.IsTrue)
	c.Assert(child.Flags().Lookup("verbose").Changed, qt.IsTrue)
	c.Assert(child.Flags().Lookup("db-url").Usage, qt.Contains, "[env: PTAH_DB_URL]")
	c.Assert(child.Flags().Lookup("verbose").Usage, qt.Contains, "[env: PTAH_VERBOSE]")
}

func TestInitializeEnvDoesNotOverrideExplicitFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "postgres://env")

	var dbURL string
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Database URL")
	c.Assert(cmd.Flags().Set("db-url", "postgres://cli"), qt.IsNil)

	err := cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(dbURL, qt.Equals, "postgres://cli")
}

func TestInitializeEnvIgnoresEmptyEnvironmentValues(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "")

	var dbURL string
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(&dbURL, "db-url", "postgres://default", "Database URL")

	err := cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(dbURL, qt.Equals, "postgres://default")
}

// TestInitializeEnvSplitsEmptyByFlagType is where the boolean rule departs from
// the general one, and the two halves are asserted together so neither can be
// changed without meeting the other.
//
// An empty value keeps meaning "unset" for a string or a uint: those are types
// where "no value" is a thing an operator can plausibly want to spell. For a
// boolean it is a configuration error (stokaro/ptah#1334) -- `PTAH_DRY_RUN=` is
// a boolean with nothing in it, and reading it as `false` is how a broken shell
// expansion turned into a silent default.
func TestInitializeEnvSplitsEmptyByFlagType(t *testing.T) {
	tests := []struct {
		name        string
		envName     string
		register    func(*cobra.Command)
		wantMessage string
	}{
		{
			name:    "a string flag keeps ignoring an empty value",
			envName: "PTAH_DB_URL",
			register: func(cmd *cobra.Command) {
				cmd.Flags().String("db-url", "", "Database URL")
			},
		},
		{
			name:    "a uint flag keeps ignoring an empty value",
			envName: "PTAH_LATEST",
			register: func(cmd *cobra.Command) {
				cmd.Flags().Uint("latest", 0, "Latest versions")
			},
		},
		{
			name:    "a bool flag refuses an empty value",
			envName: "PTAH_DRY_RUN",
			register: func(cmd *cobra.Command) {
				cmd.Flags().Bool("dry-run", false, "Preview changes")
			},
			wantMessage: `invalid boolean value "" for PTAH_DRY_RUN`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(test.envName, "")
			cmd := &cobra.Command{Use: "ptah"}
			test.register(cmd)

			err := cmdflags.InitializeEnv("PTAH", cmd)

			c.Assert(errMessage(err), qt.Equals, test.wantMessage)
		})
	}
}

// errMessage renders an error for comparison against a table row without a
// branch in the test body.
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestInitializeEnvSkipsDisabledEnvironmentBinding(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	var autoApprove bool
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Skip confirmation")
	c.Assert(cmdflags.DisableEnvBinding(cmd.Flags(), "auto-approve"), qt.IsNil)

	err := cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(autoApprove, qt.IsFalse)
	c.Assert(cmd.Flags().Lookup("auto-approve").Changed, qt.IsFalse)
	c.Assert(cmd.Flags().Lookup("auto-approve").Usage, qt.Not(qt.Contains), "[env: PTAH_AUTO_APPROVE]")
}

func TestInitializeEnvAnnotatesUsageOnce(t *testing.T) {
	c := qt.New(t)

	var dbURL string
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Database URL")

	err := cmdflags.InitializeEnv("PTAH", cmd)
	c.Assert(err, qt.IsNil)
	err = cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(cmd.Flags().Lookup("db-url").Usage, qt.Equals, "Database URL [env: PTAH_DB_URL]")
}

func TestInitializeEnvRejectsMalformedBoolean(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	var dryRun bool
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes")

	err := cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "notabool" for PTAH_DRY_RUN`)
	c.Assert(dryRun, qt.IsFalse)
	c.Assert(cmd.Flags().Lookup("dry-run").Changed, qt.IsFalse)
}

func TestInitializeEnvRejectsMalformedUnsignedIntegerWithoutMutation(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_LIMIT", "many")

	var limit uint64
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().Uint64Var(&limit, "limit", 7, "Migration limit")

	err := cmdflags.InitializeEnv("PTAH", cmd)

	c.Assert(err, qt.ErrorMatches, `invalid unsigned integer value "many" for PTAH_LIMIT`)
	c.Assert(limit, qt.Equals, uint64(7))
	c.Assert(cmd.Flags().Lookup("limit").Changed, qt.IsFalse)
}

func TestInstallEnvBindingRejectsMalformedValueBeforeCommandHooks(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	var calls []string
	cmd := &cobra.Command{
		Use: "run",
		Args: func(_ *cobra.Command, _ []string) error {
			calls = append(calls, "args")
			return nil
		},
		PreRun: func(_ *cobra.Command, _ []string) {
			calls = append(calls, "pre-run")
		},
		Run: func(_ *cobra.Command, _ []string) {
			calls = append(calls, "run")
		},
	}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	cmdflags.InstallEnvBinding("PTAH", cmd)

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "notabool" for PTAH_DRY_RUN`)
	c.Assert(calls, qt.HasLen, 0)
}

func TestInstallEnvBindingExplicitCLIWinsOverMalformedEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	var dryRun bool
	var ran bool
	cmd := &cobra.Command{
		Use: "run",
		Run: func(_ *cobra.Command, _ []string) {
			ran = true
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes")
	cmdflags.InstallEnvBinding("PTAH", cmd)
	cmd.SetArgs([]string{"--dry-run"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(dryRun, qt.IsTrue)
	c.Assert(ran, qt.IsTrue)
}

func TestInstallEnvBindingAppliesInheritedPersistentFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_VERBOSE", "true")

	var verbose bool
	var ran bool
	root := &cobra.Command{Use: "ptah"}
	root.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	child := &cobra.Command{
		Use: "run",
		Run: func(_ *cobra.Command, _ []string) {
			ran = true
		},
	}
	root.AddCommand(child)
	cmdflags.InstallEnvBinding("PTAH", root)
	root.SetArgs([]string{"run"})

	err := root.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(verbose, qt.IsTrue)
	c.Assert(ran, qt.IsTrue)
}

func TestInstallEnvBindingIgnoresMalformedValueForUnselectedCommand(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	root := &cobra.Command{Use: "ptah"}
	preview := &cobra.Command{Use: "preview", Run: func(_ *cobra.Command, _ []string) {}}
	preview.Flags().Bool("dry-run", false, "Preview changes")
	var ran bool
	run := &cobra.Command{
		Use: "run",
		Run: func(_ *cobra.Command, _ []string) {
			ran = true
		},
	}
	root.AddCommand(preview, run)
	cmdflags.InstallEnvBinding("PTAH", root)
	root.SetArgs([]string{"run"})

	err := root.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
}

func TestInstallEnvBindingHelpDoesNotParseEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "notabool")

	cmd := &cobra.Command{Use: "run", Run: func(_ *cobra.Command, _ []string) {}}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	cmdflags.InstallEnvBinding("PTAH", cmd)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "[env: PTAH_DRY_RUN]")
}

// TestSetOnCommandLineSeparatesArgvFromEnvironment pins the distinction pflag's
// Changed bit cannot make. InitializeEnv applies a PTAH_* value through
// FlagSet.Set, which marks the flag Changed exactly as an argv occurrence does,
// so a rule about what the operator wrote has to ask a different question.
func TestSetOnCommandLineSeparatesArgvFromEnvironment(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "1")

	cmd := &cobra.Command{Use: "apply"}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	cmd.Flags().Bool("auto-approve", false, "Skip approval")
	c.Assert(cmd.Flags().Set("auto-approve", "true"), qt.IsNil)

	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

	c.Assert(cmd.Flags().Lookup("dry-run").Changed, qt.IsTrue)
	c.Assert(cmdflags.SetOnCommandLine(cmd.Flags(), "dry-run"), qt.IsFalse)
	c.Assert(cmdflags.SetOnCommandLine(cmd.Flags(), "auto-approve"), qt.IsTrue)
	c.Assert(cmdflags.SetOnCommandLine(cmd.Flags(), "absent"), qt.IsFalse)
}

// TestSetOnCommandLineForgetsAPreviousExecution covers a command tree reused for
// a second run, which is how the compatibility CLI is driven under test. The
// marker recorded when the environment supplied a value must not outlive the
// execution that recorded it, or the next run reads a typed flag as an
// environment default.
func TestSetOnCommandLineForgetsAPreviousExecution(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "1")

	cmd := &cobra.Command{Use: "apply"}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)
	c.Assert(cmdflags.SetOnCommandLine(cmd.Flags(), "dry-run"), qt.IsFalse)

	envbooltest.Unset("PTAH_DRY_RUN")(t)
	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

	c.Assert(cmdflags.SetOnCommandLine(cmd.Flags(), "dry-run"), qt.IsTrue)
}

// TestMutuallyExclusiveOnCommandLineMatchesCobrasSentence keeps the replacement
// for MarkFlagsMutuallyExclusive byte-identical to what cobra emits, including
// the declaration-order group list and the sorted list of flags that were set.
func TestMutuallyExclusiveOnCommandLineMatchesCobrasSentence(t *testing.T) {
	c := qt.New(t)

	cmd := &cobra.Command{Use: "apply"}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	cmd.Flags().Bool("auto-approve", false, "Skip approval")
	c.Assert(cmd.Flags().Set("dry-run", "true"), qt.IsNil)
	c.Assert(cmd.Flags().Set("auto-approve", "true"), qt.IsNil)

	err := cmdflags.MutuallyExclusiveOnCommandLine(cmd.Flags(), "dry-run", "auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals,
		"if any flags in the group [dry-run auto-approve] are set none of the others can be;"+
			" [auto-approve dry-run] were all set")
}

// TestMutuallyExclusiveOnCommandLineIgnoresEnvironmentMembers is the control
// that makes the test above about the command line rather than about Changed.
func TestMutuallyExclusiveOnCommandLineIgnoresEnvironmentMembers(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DRY_RUN", "1")

	cmd := &cobra.Command{Use: "apply"}
	cmd.Flags().Bool("dry-run", false, "Preview changes")
	cmd.Flags().Bool("auto-approve", false, "Skip approval")
	c.Assert(cmd.Flags().Set("auto-approve", "true"), qt.IsNil)
	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)

	c.Assert(cmdflags.MutuallyExclusiveOnCommandLine(cmd.Flags(), "dry-run", "auto-approve"), qt.IsNil)
}
