package cmdflags

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
)

func TestEnvNameNormalizesFlagName(t *testing.T) {
	c := qt.New(t)

	c.Assert(EnvName("PTAH", "db-url"), qt.Equals, "PTAH_DB_URL")
	c.Assert(EnvName("PTAH", "migration.lock-timeout"), qt.Equals, "PTAH_MIGRATION_LOCK_TIMEOUT")
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

	InitializeEnv("PTAH", root)

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

	InitializeEnv("PTAH", cmd)

	c.Assert(dbURL, qt.Equals, "postgres://cli")
}

func TestInitializeEnvIgnoresEmptyEnvironmentValues(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_DB_URL", "")

	var dbURL string
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(&dbURL, "db-url", "postgres://default", "Database URL")

	InitializeEnv("PTAH", cmd)

	c.Assert(dbURL, qt.Equals, "postgres://default")
}

func TestInitializeEnvSkipsDisabledEnvironmentBinding(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	var autoApprove bool
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().BoolVar(&autoApprove, "auto-approve", false, "Skip confirmation")
	c.Assert(DisableEnvBinding(cmd.Flags(), "auto-approve"), qt.IsNil)

	InitializeEnv("PTAH", cmd)

	c.Assert(autoApprove, qt.IsFalse)
	c.Assert(cmd.Flags().Lookup("auto-approve").Changed, qt.IsFalse)
	c.Assert(cmd.Flags().Lookup("auto-approve").Usage, qt.Not(qt.Contains), "[env: PTAH_AUTO_APPROVE]")
}

func TestInitializeEnvAnnotatesUsageOnce(t *testing.T) {
	c := qt.New(t)

	var dbURL string
	cmd := &cobra.Command{Use: "ptah"}
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Database URL")

	InitializeEnv("PTAH", cmd)
	InitializeEnv("PTAH", cmd)

	c.Assert(cmd.Flags().Lookup("db-url").Usage, qt.Equals, "Database URL [env: PTAH_DB_URL]")
}
