//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestMigrateUnknownDirectoryFormatDiagnosticsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)
	buildPtah(c, ctx, repoRoot, nativeBinary)

	// One row per VERB and per SPELLING, because that is the granularity at
	// which this can regress. `unknown dir format "bogus"` is the pinned
	// community binary v1.3.0's answer on every path where it reaches directory-
	// layout resolution; the adaptation used to live inside the `hash` /
	// `validate` wrapper, so those two matched and the other seven printed a
	// longer diagnostic of their own (stokaro/ptah#1235 cell 9.8).
	//
	// `migrate apply` carries only the query row: neither binary registers
	// `--dir-format` there, and both answer `unknown flag: --dir-format` when
	// it is passed.
	//
	// The refusal precedes every read, so `file://migrations` need not exist
	// and the `--url` / `--dev-url` values are never opened. They are present
	// because cobra validates required flags before the command body runs.
	const devURL = "sqlite://dev?mode=memory&_fk=1"
	const dbURL = "sqlite://e2e.db?_fk=1"
	compatCases := []struct {
		name string
		args []string
	}{
		{
			name: "compat new query",
			args: []string{"migrate", "new", "demo", "--dir", "file://migrations?format=bogus"},
		},
		{
			name: "compat new flag",
			args: []string{"migrate", "new", "demo", "--dir", "file://migrations", "--dir-format", "bogus"},
		},
		{
			name: "compat hash query",
			args: []string{"migrate", "hash", "--dir", "file://migrations?format=bogus"},
		},
		{
			name: "compat hash flag",
			args: []string{"migrate", "hash", "--dir", "file://migrations", "--dir-format", "bogus"},
		},
		{
			name: "compat validate query",
			args: []string{"migrate", "validate", "--dir", "file://migrations?format=bogus"},
		},
		{
			name: "compat validate flag",
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--dir-format", "bogus"},
		},
		{
			name: "compat lint query",
			args: []string{
				"migrate", "lint", "--dir", "file://migrations?format=bogus",
				"--dev-url", devURL, "--latest", "1",
			},
		},
		{
			name: "compat lint flag",
			args: []string{
				"migrate", "lint", "--dir", "file://migrations", "--dir-format", "bogus",
				"--dev-url", devURL, "--latest", "1",
			},
		},
		{
			name: "compat status query",
			args: []string{"migrate", "status", "--dir", "file://migrations?format=bogus", "--url", dbURL},
		},
		{
			name: "compat status flag",
			args: []string{
				"migrate", "status", "--dir", "file://migrations", "--dir-format", "bogus", "--url", dbURL,
			},
		},
		{
			name: "compat set query",
			args: []string{
				"migrate", "set", "20240101000000",
				"--dir", "file://migrations?format=bogus", "--url", dbURL,
			},
		},
		{
			name: "compat set flag",
			args: []string{
				"migrate", "set", "20240101000000",
				"--dir", "file://migrations", "--dir-format", "bogus", "--url", dbURL,
			},
		},
		{
			name: "compat diff query",
			args: []string{
				"migrate", "diff", "demo", "--dir", "file://migrations?format=bogus",
				"--dev-url", devURL, "--to", "file://schema.sql",
			},
		},
		{
			name: "compat diff flag",
			args: []string{
				"migrate", "diff", "demo", "--dir", "file://migrations", "--dir-format", "bogus",
				"--dev-url", devURL, "--to", "file://schema.sql",
			},
		},
		{
			name: "compat apply query",
			args: []string{"migrate", "apply", "--dir", "file://migrations?format=bogus", "--url", dbURL},
		},
		{
			name: "compat import query",
			args: []string{"migrate", "import", "--from", "file://src?format=bogus", "--to", "file://dst"},
		},
		{
			name: "compat import flag",
			args: []string{
				"migrate", "import", "--from", "file://src", "--to", "file://dst", "--dir-format", "bogus",
			},
		},
	}

	for _, test := range compatCases {
		c.Run(test.name, func(c *qt.C) {
			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), compatBinary, test.args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: unknown dir format \"bogus\"\n")
			c.Assert(stderr, qt.HasLen, 34)
		})
	}

	c.Run("native validate keeps native diagnostic", func(c *qt.C) {
		dir := c.TempDir()

		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
			"migrations", "validate",
			"--dir", dir,
			"--dir-format", "bogus",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"error: unknown migration directory format \"bogus\": expected auto, ptah, or atlas\n")
	})

	// The compat `migrate import` source resolution was rewritten alongside the
	// rows above. Native `migrations import` reads a plain `--source-dir` path
	// through a different command and keeps its own diagnostic and its own exit
	// code; without this row, "native is unchanged" would be an argument rather
	// than a measurement.
	c.Run("native import keeps native diagnostic", func(c *qt.C) {
		stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), nativeBinary,
			"migrations", "import",
			"--source-dir", "nope",
			"--from", "flyway",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"error: migrations directory nope: stat nope: no such file or directory\n")
	})

	// The compatibility import's own refusal ordering, at the process level:
	// a source directory that is not there is reported as missing, not as a
	// layout conflict. Measured on the pinned community binary v1.3.0, which
	// answers `sql/migrate: stat nope: no such file or directory` for the same
	// input. Both layouts are rows because the defect was specific to the
	// default one, where the already-in-target-format refusal ran first.
	importMissingCases := []struct {
		name string
		args []string
	}{
		{
			name: "default layout",
			args: []string{"migrate", "import", "--from", "file://nope", "--to", "file://dst"},
		},
		{
			name: "explicit layout",
			args: []string{
				"migrate", "import", "--from", "file://nope", "--to", "file://dst",
				"--dir-format", "flyway",
			},
		},
	}

	for _, test := range importMissingCases {
		c.Run("compat import missing source "+test.name, func(c *qt.C) {
			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), compatBinary, test.args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: sql/migrate: stat nope: no such file or directory\n")
			c.Assert(stderr, qt.HasLen, 57)
		})
	}
}
