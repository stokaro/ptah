package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

const (
	gooseImportFixture = `-- +goose Up
CREATE TABLE users (id int);

-- +goose Down
DROP TABLE users;
`
	dbmateImportFixture = `-- migrate:up
CREATE TABLE users (id int);

-- migrate:down
DROP TABLE users;
`
)

func writeMigrateImportFixture(tb testing.TB, dir, name, body string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
}

func runMigrateImport(tb testing.TB, args []string) (*bytes.Buffer, error) {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs(append([]string{"migrate", "import"}, args...))

	return out, cmd.Execute()
}

// TestCompatMigrateImportSuccessIsSilent pins the whole of what a successful
// compatibility import reports: the destination directory and its atlas.sum,
// and not a single byte on the command's writer. Both documented spellings of
// the source format are covered, because the format is resolved from the
// --from query parameter and from --dir-format on separate code paths.
func TestCompatMigrateImportSuccessIsSilent(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		// prepare writes the source fixture and returns the flags to import it
		// with, so each spelling carries its own wiring instead of branching in
		// the loop body.
		prepare func(c *qt.C, source, target string) []string
	}{
		{
			name: "goose via format query parameter",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + target}
			},
		},
		{
			name: "goose via dir-format flag",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source, "--to", "file://" + target, "--dir-format", "goose"}
			},
		},
		{
			name: "golang-migrate via format query parameter",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "1_init.up.sql", "CREATE TABLE users (id int);\n")
				writeMigrateImportFixture(c.TB, source, "1_init.down.sql", "DROP TABLE users;\n")
				return []string{"--from", "file://" + source + "?format=golang-migrate", "--to", "file://" + target}
			},
		},
		{
			name: "golang-migrate via dir-format flag",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "1_init.up.sql", "CREATE TABLE users (id int);\n")
				writeMigrateImportFixture(c.TB, source, "1_init.down.sql", "DROP TABLE users;\n")
				return []string{"--from", "file://" + source, "--to", "file://" + target, "--dir-format", "golang-migrate"}
			},
		},
		{
			name: "dbmate via format query parameter",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "20240101010101_init.sql", dbmateImportFixture)
				return []string{"--from", "file://" + source + "?format=dbmate", "--to", "file://" + target}
			},
		},
		{
			name: "dbmate via dir-format flag",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "20240101010101_init.sql", dbmateImportFixture)
				return []string{"--from", "file://" + source, "--to", "file://" + target, "--dir-format", "dbmate"}
			},
		},
		{
			name: "flyway via format query parameter",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "V1__init.sql", "CREATE TABLE users (id int);\n")
				return []string{"--from", "file://" + source + "?format=flyway", "--to", "file://" + target}
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			root := c.TempDir()
			source := filepath.Join(root, "source")
			target := filepath.Join(root, "target")

			out, err := runMigrateImport(c.TB, tt.prepare(c, source, target))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Equals, "")
			_, statErr := os.Stat(filepath.Join(target, "atlas.sum"))
			c.Assert(statErr, qt.IsNil)
		})
	}
}

// TestCompatMigrateImportHelpUsesUpdatedRootWriter is the counterpart of
// TestCompatMigrateHashHelpUsesUpdatedRootWriter. `migrate import` is
// registered directly on the compatibility tree rather than through the
// adapter, so its command object is reused across executions: silencing it by
// assigning io.Discard to its own writer would survive the root's later
// SetOut and send this help text nowhere.
func TestCompatMigrateImportHelpUsesUpdatedRootWriter(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)

	cmd := atlas.NewCompatCommand("atlas")
	var firstOut bytes.Buffer
	cmd.SetOut(&firstOut)
	cmd.SetErr(&firstOut)
	cmd.SetArgs([]string{"migrate", "import", "--from", "file://" + source + "?format=goose", "--to", "file://" + target})

	firstErr := cmd.Execute()

	c.Assert(firstErr, qt.IsNil, qt.Commentf("%s", firstOut.String()))
	c.Assert(firstOut.String(), qt.Equals, "")
	var secondOut bytes.Buffer
	cmd.SetOut(&secondOut)
	cmd.SetErr(&secondOut)
	cmd.SetArgs([]string{"migrate", "import", "--help"})

	secondErr := cmd.Execute()

	c.Assert(secondErr, qt.IsNil, qt.Commentf("%s", secondOut.String()))
	c.Assert(firstOut.String(), qt.Equals, "")
	c.Assert(secondOut.String(), qt.Contains, "Usage:\n  atlas migrate import [flags]")
}

// TestCompatMigrateImportFailuresStayLoud keeps the silence scoped to success.
// Every rejection this verb can produce still reports a diagnostic and a
// non-nil error, so a later reader cannot widen the deletion into a blanket
// discard without turning this table red.
func TestCompatMigrateImportFailuresStayLoud(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		// prepare writes whatever the rejection needs to be reachable and
		// returns the flags that trigger it.
		prepare func(c *qt.C, source, target string) []string
	}{
		{
			name: "missing source directory",
			prepare: func(_ *qt.C, source, target string) []string {
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + target}
			},
		},
		{
			name: "source and destination are the same directory",
			prepare: func(c *qt.C, source, _ string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + source}
			},
		},
		{
			name: "destination already holds migrations",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				writeMigrateImportFixture(c.TB, target, "1_existing.sql", "CREATE TABLE existing (id int);\n")
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + target}
			},
		},
		{
			name: "unknown dir-format flag value",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source, "--to", "file://" + target, "--dir-format", "nope"}
			},
		},
		{
			name: "unknown format query parameter",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source + "?format=nope", "--to", "file://" + target}
			},
		},
		{
			name: "remote source URL",
			prepare: func(_ *qt.C, _, target string) []string {
				return []string{"--from", "atlas://repo/migrations?format=flyway", "--to", "file://" + target}
			},
		},
		{
			name: "positional argument",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + target, "extra"}
			},
		},
		{
			name: "unknown flag",
			prepare: func(c *qt.C, source, target string) []string {
				writeMigrateImportFixture(c.TB, source, "00001_init.sql", gooseImportFixture)
				return []string{"--from", "file://" + source + "?format=goose", "--to", "file://" + target, "--nope"}
			},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			root := c.TempDir()
			source := filepath.Join(root, "source")
			target := filepath.Join(root, "target")

			out, err := runMigrateImport(c.TB, tt.prepare(c, source, target))

			c.Assert(err, qt.IsNotNil)
			c.Assert(out.String(), qt.Contains, "Error: ")
		})
	}
}

// TestCompatMigrateImportRefusesADirectoryAlreadyInTargetFormat covers the
// spelling that copies without converting anything.
//
// Importing a directory that is already in the target format is a no-op
// dressed as work: it copies the files and writes a fresh sum over a directory
// whose previous contents nothing verified. The pinned community binary refuses
// it, and this surface exited 0 and copied — the direction that lets a mistake
// pass silently.
//
// All three spellings that resolve to atlas are covered, because the format
// arrives on three separate paths: omitted entirely, in the --from query, and
// on --dir-format. Only checking the explicit ones would leave the default
// invocation — the likeliest way to hit this — untested.
func TestCompatMigrateImportRefusesADirectoryAlreadyInTargetFormat(t *testing.T) {
	tests := []struct {
		name string
		args func(from, to string) []string
	}{
		{
			name: "format omitted",
			args: func(from, to string) []string {
				return []string{"--from", "file://" + from, "--to", "file://" + to}
			},
		},
		{
			name: "format in the source query",
			args: func(from, to string) []string {
				return []string{"--from", "file://" + from + "?format=atlas", "--to", "file://" + to}
			},
		},
		{
			name: "format on the flag",
			args: func(from, to string) []string {
				return []string{"--from", "file://" + from, "--to", "file://" + to, "--dir-format", "atlas"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			from := filepath.Join(t.TempDir(), "src")
			to := filepath.Join(t.TempDir(), "dst")
			writeMigrateImportFixture(c.TB, from, "20240101010101_init.sql", "CREATE TABLE users (id int);\n")

			_, err := runMigrateImport(c.TB, tt.args(from, to))

			c.Assert(err, qt.ErrorMatches, `cannot import a migration directory already in "atlas" format`)
			_, statErr := os.Stat(to)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue,
				qt.Commentf("the refusal must precede writing anything to the destination"))
		})
	}
}
