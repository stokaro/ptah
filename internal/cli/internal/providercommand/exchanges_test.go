package providercommand_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerexchange"
	"ptah.run/internal/providerselect"
)

// recorderUnder runs a command with args under ctx and returns the recorder
// the context its selection hands the verb carries, and the error Start
// returned.
func recorderUnder(c *qt.C, ctx context.Context, args ...string) (*providerexchange.Recorder, error) {
	c.Helper()
	var recorder *providerexchange.Recorder
	var startErr error
	cmd := &cobra.Command{Use: "test"}
	selection := providercommand.Register(cmd)
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		started, stop, err := selection.Start(cmd, []providerselect.Target{{Flag: "db-url", Value: "sqlite:///x.db"}})
		defer stop()
		startErr = err
		// Start returns no context when it refuses.
		if started != nil {
			recorder = providerexchange.FromContext(started)
		}
		return nil
	}
	cmd.SetArgs(args)
	c.Assert(cmd.ExecuteContext(ctx), qt.IsNil)
	return recorder, startErr
}

// --provider-exchanges puts a recorder into the context of a command that runs
// under a session of its own, and creates the directory before any provider
// starts.
func TestStart_HappyPath_TheExchangesFlagRecords(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	dir := filepath.Join(c.TempDir(), "exchanges")

	recorder, err := recorderUnder(c, c.Context(), "--provider-exchanges", dir)

	c.Assert(err, qt.IsNil)
	c.Assert(recorder, qt.IsNotNil)
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// A command that keeps the session an enclosing surface installed records
// too: the recorder goes into that context, not only into a new session's.
func TestStart_HappyPath_TheExchangesFlagRecordsUnderAnEnclosingSession(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)
	enclosing := providerselect.NewLazy(providerconfig.Configuration{}, false, nil)
	c.Cleanup(enclosing.Close)

	recorder, err := recorderUnder(c, enclosing.With(c.Context()), "--provider-exchanges", filepath.Join(c.TempDir(), "exchanges"))

	c.Assert(err, qt.IsNil)
	c.Assert(recorder, qt.IsNotNil)
}

// Without the flag nothing is recorded.
func TestStart_HappyPath_NoExchangesFlagRecordsNothing(t *testing.T) {
	c := qt.New(t)
	isolateStore(c)

	recorder, err := recorderUnder(c, c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder, qt.IsNil)
}

// A directory that holds anything is refused before a provider starts, and so
// is an empty value.
func TestStart_FailurePath_TheExchangesFlag(t *testing.T) {
	t.Run("a directory that holds a file", func(t *testing.T) {
		c := qt.New(t)
		isolateStore(c)
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "notes.txt"), nil, 0o600), qt.IsNil)

		recorder, err := recorderUnder(c, c.Context(), "--provider-exchanges", dir)

		c.Assert(err, qt.ErrorIs, providerexchange.ErrDirectoryNotEmpty)
		c.Assert(err, qt.ErrorMatches, `(?s)--provider-exchanges: the provider exchange directory is not empty: .*`)
		c.Assert(recorder, qt.IsNil)
	})
	t.Run("an empty value", func(t *testing.T) {
		c := qt.New(t)
		isolateStore(c)

		recorder, err := recorderUnder(c, c.Context(), "--provider-exchanges=")

		c.Assert(err, qt.ErrorMatches, `--provider-exchanges requires a directory`)
		c.Assert(recorder, qt.IsNil)
	})
}

// The flag reads no environment variable: a variable exported once would
// record every later run without the operator asking for it.
func TestRegister_HappyPath_TheExchangesFlagReadsNoEnvironmentVariable(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
	providercommand.Register(cmd)
	cmdflags.InstallEnvBinding("PTAH", cmd)

	_, bound := cmdflags.EnvBindingName("PTAH", cmd.Flags().Lookup(providercommand.ExchangesFlag))

	c.Assert(bound, qt.IsFalse)
}
