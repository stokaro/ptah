package atlas

// White-box testing required: the command runner seam observes the exact
// DiffOptions assembled at the Cobra boundary without mutating package globals.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
)

func TestMigrateDiffCommand_RoutesDiagnosticsToCobraStderr(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	desiredPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(
		desiredPath,
		[]byte("CREATE TABLE desired (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	called := 0
	cmd := newAtlasMigrateDiffCommandWithRunner(func(
		_ context.Context,
		_ *dbschema.DatabaseConnection,
		opts atlasmigrate.DiffOptions,
	) (atlasmigrate.DiffResult, error) {
		called++
		_, err := fmt.Fprint(opts.Diagnostics, "migrate diff diagnostic\n")
		return atlasmigrate.DiffResult{Synced: true}, err
	})
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + desiredPath,
		"diagnostics",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(called, qt.Equals, 1)
	c.Assert(stderr.String(), qt.Equals, "migrate diff diagnostic\n")
	c.Assert(stdout.String(), qt.Equals,
		"The migration directory is synced with the desired state, no changes to be made\n",
	)
}
