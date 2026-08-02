package cmdflags_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
)

func TestEnvNameNormalizesFlagName(t *testing.T) {
	c := qt.New(t)

	c.Assert(cmdflags.EnvName("PTAH", "db-url"), qt.Equals, "PTAH_DB_URL")
	c.Assert(cmdflags.EnvName("PTAH", "migration.lock-timeout"), qt.Equals, "PTAH_MIGRATION_LOCK_TIMEOUT")
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
