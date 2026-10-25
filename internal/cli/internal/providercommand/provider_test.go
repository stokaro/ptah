package providercommand_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
	"ptah.run/internal/providerselect"
)

func TestProviderSelectionRequiresAnExplicitFlag(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_PROVIDER_CONFIG", "missing.json")
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.SetArgs(nil)
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	cmd.SetArgs([]string{"--provider-config="})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNotNil)
	c.Assert(calls, qt.Equals, 1)
}

func dockerDevURLManifest(c *qt.C) string {
	c.Helper()
	executable := filepath.Join(c.TempDir(), "missing-provider")
	manifest := `{"format_version":2,"providers":[{"executable":` + strconv.Quote(executable) + `,"sha256":"` + strings.Repeat("a", 64) +
		`","provider_id":"example.org/fixture","engines":[{"id":"fixture"}]}]}`
	path := filepath.Join(c.TempDir(), "providers.json")
	c.Assert(os.WriteFile(path, []byte(manifest), 0o600), qt.IsNil)
	return path
}

func TestDockerDevURLSelectsTheProviderOfItsEngine(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("dev-url", "", "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--dev-url", "docker://fixture/1/dev"})
	// The provider is selected, so its missing executable stops the command.
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `The configured provider executable was not found.*`)
	c.Assert(calls, qt.Equals, 0)
}

func TestDockerDevURLOfAnotherEngineIsNotOwnedBySelectedProvider(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("dev-url", "", "")
	cmd.Flags().String("db-url", "", "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--provider-engine", "fixture", "--dev-url", "docker://other/1"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*not owned by the selected provider.*`)
	c.Assert(calls, qt.Equals, 0)
}

// startWith runs a command that declares only the provider flags and starts
// the selection for targets, and returns whether a provider was started.
func startWith(c *qt.C, targets []providerselect.Target, args ...string) (bool, error) {
	c.Helper()
	var started bool
	cmd := &cobra.Command{Use: "test"}
	selection := providercommand.Register(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop, err := selection.Start(cmd, targets)
		defer stop()
		started = providerselect.StartedFrom(ctx) != nil
		return err
	}
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(c.Context())
	return started, err
}

// A command that names its database under a flag of its own passes it as the
// target. A manifest that claims another engine then leaves a PostgreSQL
// database to the built-in engine and starts nothing.
func TestStart_HappyPath_ATargetOfAnotherEngineKeepsTheBuiltInEngine(t *testing.T) {
	c := qt.New(t)
	started, err := startWith(c,
		[]providerselect.Target{{Flag: "database-url", Value: "postgres://u@127.0.0.1:1/app"}},
		"--provider-config", dockerDevURLManifest(c))
	c.Assert(err, qt.IsNil)
	c.Assert(started, qt.IsFalse)
}

// The same target with the engine named explicitly is refused before anything
// starts, and the refusal names the flag the command carried it under.
func TestStart_FailurePath_AnExplicitEngineRefusesAnotherEnginesTarget(t *testing.T) {
	c := qt.New(t)
	started, err := startWith(c,
		[]providerselect.Target{{Flag: "database-url", Value: "postgres://u@127.0.0.1:1/app"}},
		"--provider-config", dockerDevURLManifest(c), "--provider-engine", "fixture")
	c.Assert(err, qt.ErrorMatches, `(?s).*The --database-url target is not owned by the selected provider engine.*`)
	c.Assert(started, qt.IsFalse)
}

// isolateStore points the user cache at an empty directory, so the user store
// holds nothing the developer installed.
func isolateStore(c *qt.C) {
	c.Helper()
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
}

// corruptStoreCopy installs a record for the YDB provider in the user store and
// an executable whose bytes do not match it.
func corruptStoreCopy(c *qt.C) string {
	c.Helper()
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	root, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	platform := providerrelease.Running()
	c.Assert(providerinstall.WriteRecord(root, platform, providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: entry.ProviderID, Version: "0.2.0",
		MinContract: providerrelease.HostContract(), Engines: []string{"ydb"}, SHA256: strings.Repeat("a", 64),
		Executable: "0.2.0/" + platform.OS + "_" + platform.Arch + "/ptah-plugin-ydb",
	}), qt.IsNil)
	path := filepath.Join(providerinstall.Dir(root, entry.ProviderID, "0.2.0", platform), "ptah-plugin-ydb")
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte("not the recorded provider"), 0o600), qt.IsNil)
	return path
}

func newProbe(calls *int) *cobra.Command {
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { *calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("db-url", "", "")
	return cmd
}

// The YDB provider is selectable only by asking for it. A URL alone keeps the
// built-in engine and never reads the store.
func TestOfficialEngineWithABuiltInImplementationIgnoresTheStoreWithoutAnExplicitRequest(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptStoreCopy(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--db-url", "ydb://localhost:2136/local"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestExplicitProviderEngineWithNothingInstalledReportsTheInstallInstruction(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--provider-engine", "ydb"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*installed separately, and no installation was found.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}

func TestExplicitProviderEngineRefusesACorruptStoreCopy(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	corruptStoreCopy(c)
	calls := 0
	cmd := newProbe(&calls)
	cmd.SetArgs([]string{"--provider-engine", "ydb"})
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `(?s).*installed provider executable does not match the SHA-256 digest its install record holds.*ptah providers install ydb.*`)
	c.Assert(calls, qt.Equals, 0)
}

// With no flag naming a provider engine, the command runs under a lazy
// session over the user store: an engine a provider serves by default is
// served, nothing has started, and stop closes the session.
func TestStart_HappyPath_NoSelectionRunsUnderALazySession(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	var session *providerselect.Lazy
	var started bool
	cmd := &cobra.Command{Use: "test"}
	selection := providercommand.Register(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop, err := selection.Start(cmd, []providerselect.Target{{Flag: "db-url", Value: "sqlite:///x.db"}})
		session = providerselect.FromContext(ctx)
		started = providerselect.StartedFrom(ctx) != nil
		stop()
		return err
	}
	cmd.SetArgs(nil)

	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)

	c.Assert(started, qt.IsFalse)
	c.Assert(session.Serves("duckdb"), qt.IsTrue)
	c.Assert(session.Serves("sqlite"), qt.IsFalse)
	_, _, err := session.NativeRegistration(c.Context(), "duckdb")
	c.Assert(err, qt.ErrorIs, providerselect.ErrClosed)
}

// sessionUnder runs a command with args under a context carrying enclosing,
// and returns the session the command's selection hands its verb.
func sessionUnder(c *qt.C, enclosing *providerselect.Lazy, args ...string) *providerselect.Lazy {
	c.Helper()
	var session *providerselect.Lazy
	cmd := &cobra.Command{Use: "test"}
	selection := providercommand.Register(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop, err := selection.Start(cmd, []providerselect.Target{{Flag: "db-url", Value: "sqlite:///x.db"}})
		defer stop()
		session = providerselect.FromContext(ctx)
		return err
	}
	cmd.SetArgs(args)
	c.Assert(cmd.ExecuteContext(enclosing.With(c.Context())), qt.IsNil)
	return session
}

// A command that runs under a session an enclosing surface installed, as
// ptah-compat does before it forwards to a native command, keeps that session:
// its own would start a second provider process for the same engine.
func TestStart_HappyPath_AnEnclosingSessionIsKept(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	enclosing := providerselect.NewLazy(providerconfig.Configuration{}, false, nil)
	c.Cleanup(enclosing.Close)

	session := sessionUnder(c, enclosing)

	c.Assert(session, qt.Equals, enclosing)
}

// A manifest the command names replaces the enclosing session with one over
// that manifest.
func TestStart_HappyPath_AManifestReplacesAnEnclosingSession(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	enclosing := providerselect.NewLazy(providerconfig.Configuration{}, false, nil)
	c.Cleanup(enclosing.Close)

	session := sessionUnder(c, enclosing, "--provider-config", dockerDevURLManifest(c))

	c.Assert(session, qt.Not(qt.Equals), enclosing)
	c.Assert(session.Serves("fixture"), qt.IsTrue)
	c.Assert(enclosing.Serves("fixture"), qt.IsFalse)
}

// A manifest whose engine no flag names leaves the session to serve it when the
// command names it later, from a project file or a flag of its own.
func TestStart_HappyPath_AManifestNoFlagNamesIsServedLater(t *testing.T) {
	c := qt.New(t)
	var session *providerselect.Lazy
	cmd := &cobra.Command{Use: "test"}
	selection := providercommand.Register(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		ctx, stop, err := selection.Start(cmd, []providerselect.Target{{Flag: "db-url", Value: "postgres://u@127.0.0.1:1/app"}})
		defer stop()
		session = providerselect.FromContext(ctx)
		return err
	}
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c)})

	c.Assert(cmd.ExecuteContext(c.Context()), qt.IsNil)

	c.Assert(session.Serves("fixture"), qt.IsTrue)
}

// A connection flag nobody set names no engine. `schema lineage` defaults
// --dialect to postgres and ignores it for a live read, so the default must not
// be read as the operator naming another engine than the database's.
func TestConfigure_ADefaultFlagValueNamesNoEngine(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().String("db-url", "", "")
	cmd.Flags().String("dialect", "postgres", "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--db-url", "fixture:///tmp/x.db"})

	// The provider of the database's engine is selected, so its missing
	// executable stops the command; the default dialect is not a mismatch.
	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `The configured provider executable was not found.*`)
	c.Assert(calls, qt.Equals, 0)
}

// A repeatable connection flag names an engine with each value it holds.
func TestConfigure_ARepeatableFlagNamesItsValues(t *testing.T) {
	c := qt.New(t)
	calls := 0
	cmd := providercommand.Configure(&cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { calls++; return nil }}, func(_ *cobra.Command, err error) error { return err })
	cmd.Flags().StringArray("dialect", nil, "")
	cmd.SetArgs([]string{"--provider-config", dockerDevURLManifest(c), "--dialect", "fixture"})

	c.Assert(cmd.ExecuteContext(c.Context()), qt.ErrorMatches, `The configured provider executable was not found.*`)
	c.Assert(calls, qt.Equals, 0)
}
