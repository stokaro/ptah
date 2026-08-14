package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The Atlas-compatible binary is quiet: the pinned Atlas CE v1.3.0 binary
// writes nothing to stderr for a dry run, and `--format` consumers redirect
// stderr into stdout (`2>&1 | jq`), which the conformance harness's
// atlas-cli-report-format probe does. The dry-run revision-state fix
// (stokaro/ptah#963) started routing dry runs through the dialect writers,
// whose per-statement narration goes to the package-level slog logger and
// therefore to stderr, so the probe stopped seeing a single JSON document
// (stokaro/ptah#967).
//
// These pins hold both halves of the contract: nothing extra on the combined
// stream, and failures still reported — quiet must not mean silent.

// dryRunQuietDir writes an Atlas-format migration directory with a matching
// down migration and seals it with atlas.sum.
func dryRunQuietDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"1_init.sql":      "CREATE TABLE quiet_users (id INTEGER PRIMARY KEY);\n",
		"1_init.down.sql": "DROP TABLE quiet_users;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// applyForReal runs a real apply so the revision table exists for a later
// dry-run rollback.
func applyForReal(c *qt.C, binPath, dbPath, migrationsDir string) {
	c.Helper()
	run := newCompatProcess(binPath,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	out, err := run.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

func TestCompatBinaryMigrateDownRunsWithEOFStdin(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)
	dbPath := filepath.Join(c.TempDir(), "down-eof.db")
	applyForReal(c, binPath, dbPath, migrationsDir)
	t.Setenv("PTAH_CONFIRM", "not-a-boolean")

	run := newCompatProcess(binPath,
		"migrate", "down",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--to-version", "0",
	)
	var stdout, stderr bytes.Buffer
	run.Stdin = bytes.NewReader(nil)
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", stdout.String(), stderr.String()))
	c.Assert(stdout.String(), qt.Not(qt.Contains), "Type 'YES' to confirm")
	// The rollback-succeeded proof is read from stdout, the command's own
	// report. It used to be read from the run log on stderr, which only ever
	// appeared there because the forwarded verb reinstalled an INFO logger over
	// the binary's quiet default (stokaro/ptah#969). This test is about EOF
	// stdin, not about the log, so it must not pin the noise.
	c.Assert(stdout.String(), qt.Contains, "✅ Migration rollback completed successfully!")
	c.Assert(stderr.String(), qt.Not(qt.Contains), "Type 'YES' to confirm")
	c.Assert(stderr.String(), qt.Not(qt.Contains), "read rollback confirmation")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'quiet_users'`)
	var tableCount int
	c.Assert(row.Scan(&tableCount), qt.IsNil)
	c.Assert(tableCount, qt.Equals, 0)
	row = conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM atlas_schema_revisions`)
	var revisionCount int
	c.Assert(row.Scan(&revisionCount), qt.IsNil)
	c.Assert(revisionCount, qt.Equals, 0)
}

// assertSingleJSONDocument asserts that combined holds exactly one JSON
// document and nothing else — the shape `... 2>&1 | jq` requires.
func assertSingleJSONDocument(c *qt.C, combined []byte) map[string]any {
	c.Helper()
	decoder := json.NewDecoder(bytes.NewReader(combined))
	var document map[string]any
	c.Assert(decoder.Decode(&document), qt.IsNil, qt.Commentf("combined output:\n%s", combined))

	var trailing any
	c.Assert(decoder.Decode(&trailing), qt.Equals, io.EOF,
		qt.Commentf("combined output carries more than one document:\n%s", combined))
	return document
}

func TestCompatBinaryDryRunFormatCombinedOutputIsOneJSONDocument(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)

	c.Run("migrate apply", func(c *qt.C) {
		dbPath := filepath.Join(c.TempDir(), "apply.db")
		run := newCompatProcess(binPath,
			"migrate", "apply",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+migrationsDir,
			"--dry-run",
			"--format", "{{ json . }}",
		)
		combined, err := run.CombinedOutput()

		c.Assert(err, qt.IsNil, qt.Commentf("%s", combined))
		document := assertSingleJSONDocument(c, combined)
		c.Assert(document["Target"], qt.Equals, "1")
	})

	c.Run("migrate down", func(c *qt.C) {
		dbPath := filepath.Join(c.TempDir(), "down.db")
		applyForReal(c, binPath, dbPath, migrationsDir)

		run := newCompatProcess(binPath,
			"migrate", "down",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+migrationsDir,
			"--to-version", "0",
			"--dry-run",
			"--format", "{{ json . }}",
		)
		run.Stdin = strings.NewReader("YES\n")
		combined, err := run.CombinedOutput()

		c.Assert(err, qt.IsNil, qt.Commentf("%s", combined))
		document := assertSingleJSONDocument(c, combined)
		c.Assert(document["Current"], qt.Equals, "1")
	})
}

func TestCompatBinaryDryRunDefaultFormatKeepsStderrEmpty(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)

	tests := []struct {
		name       string
		args       func(c *qt.C, dbPath string) []string
		wantStdout string
	}{
		{
			name: "migrate apply",
			args: func(_ *qt.C, dbPath string) []string {
				return []string{
					"migrate", "apply",
					"--url", "sqlite://" + dbPath,
					"--dir", "file://" + migrationsDir,
					"--dry-run",
				}
			},
			wantStdout: "Dry run mode: no changes will be made.\n" +
				"Migrating to version 1 from 1 pending migrations.\n" +
				"Would have applied 1 migrations.\n",
		},
		{
			name: "schema apply",
			args: func(c *qt.C, dbPath string) []string {
				c.Helper()
				schemaPath := filepath.Join(filepath.Dir(dbPath), "schema.sql")
				c.Assert(os.WriteFile(schemaPath,
					[]byte("CREATE TABLE quiet_schema (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
				return []string{
					"schema", "apply",
					"--url", "sqlite://" + dbPath,
					"--to", "file://" + schemaPath,
					"--dev-url", "sqlite://" + filepath.Join(filepath.Dir(dbPath), "dev.db"),
					"--dry-run",
				}
			},
			wantStdout: "Planned schema changes:\n" +
				"CREATE TABLE \"quiet_schema\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dbPath := filepath.Join(c.TempDir(), "quiet.db")
			run := newCompatProcess(binPath, tt.args(c, dbPath)...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", stderr.String()))
			c.Assert(stderr.String(), qt.Equals, "")
			c.Assert(stdout.String(), qt.Equals, tt.wantStdout)
		})
	}
}

// TestCompatBinaryMigrateDownDefaultFormatKeepsStderrEmpty is the pin for
// stokaro/ptah#969. `migrate down` without --format is the one Atlas verb that
// forwards into a native command which starts its own observability runtime,
// and that runtime installed a fresh INFO logger over the compat binary's quiet
// default — eight lines of narration on a dry run, four on a rollback.
//
// TestCompatBinaryDryRunDefaultFormatKeepsStderrEmpty above cannot cover this:
// it asserts stdout EQUALS its expectation, and a down report embeds the
// database and migrations paths. The stdout marker each row asserts is what
// stops the stderr assertion from holding vacuously — without it, a run that
// never reached the rollback would satisfy an empty stderr too.
func TestCompatBinaryMigrateDownDefaultFormatKeepsStderrEmpty(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)

	tests := []struct {
		name string
		args func(dbPath string) []string
		// wantStdout proves the run reached the rollback the row is about.
		wantStdout string
	}{
		{
			name: "dry run",
			args: func(dbPath string) []string {
				return []string{
					"migrate", "down",
					"--url", "sqlite://" + dbPath,
					"--dir", "file://" + migrationsDir,
					"--to-version", "0",
					"--dry-run",
				}
			},
			wantStdout: "Would have rolled back to version: 0",
		},
		{
			name: "rollback",
			args: func(dbPath string) []string {
				return []string{
					"migrate", "down",
					"--url", "sqlite://" + dbPath,
					"--dir", "file://" + migrationsDir,
					"--to-version", "0",
				}
			},
			wantStdout: "Database is now at version: 0",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			// Each row needs its own revision to roll back. Sharing one
			// database would make the second row a no-op, and an empty stderr
			// would then prove nothing.
			dbPath := filepath.Join(c.TempDir(), "down-quiet.db")
			applyForReal(c, binPath, dbPath, migrationsDir)
			run := newCompatProcess(binPath, tt.args(dbPath)...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", stderr.String()))
			c.Assert(stdout.String(), qt.Contains, tt.wantStdout)
			c.Assert(stderr.String(), qt.Equals, "")
		})
	}
}

// TestCompatBinaryMigrateDownJSONKeepsItsReport covers what the quieting must
// not take with it.
//
// The narration is silenced by lowering the native log threshold. Under a
// machine-readable format that is the wrong knob: the emitter turns the whole
// report into Info-level records, so the threshold and the output are the same
// thing, and lowering it produced a completely silent rollback -- exit 0, zero
// bytes, database changed.
//
// Asserting bytes rather than exit status is the point. A silent success has
// the same exit code as a reported one, so an exit-only fixture passes on the
// broken binary.
func TestCompatBinaryMigrateDownJSONKeepsItsReport(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)

	tests := []struct {
		name  string
		extra []string
	}{
		{
			name:  "format selected on the command line",
			extra: []string{"--log-format", "json"},
		},
		{
			name:  "format and an explicit level",
			extra: []string{"--log-format", "json", "--log-level", "info"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dbPath := filepath.Join(c.TempDir(), "down-json.db")
			applyForReal(c, binPath, dbPath, migrationsDir)
			args := append([]string{
				"migrate", "down",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
				"--to-version", "0",
			}, tt.extra...)
			run := newCompatProcess(binPath, args...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", stderr.String()))
			c.Assert(stdout.String(), qt.Contains, "Database is now at version")
		})
	}
}

// TestCompatBinaryDryRunPinNoWriterNarration is the direct pin for
// stokaro/ptah#963: the dialect writers' "[DRY RUN] Would ..." narration must
// never reach the Atlas-compatible binary's streams, whatever mechanism keeps
// it away.
func TestCompatBinaryMalformedPTAHDryRunAppliesNothing(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)
	dbPath := filepath.Join(c.TempDir(), "malformed-env.db")
	t.Setenv("PTAH_DRY_RUN", "notabool")
	run := newCompatProcess(binPath,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	combined, err := run.CombinedOutput()

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", combined))
	c.Assert(string(combined), qt.Contains, `invalid boolean value "notabool" for PTAH_DRY_RUN`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestCompatBinaryDryRunPinNoWriterNarration(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	migrationsDir := dryRunQuietDir(c)

	// The apply cases need a target with pending work and the down case needs
	// one with a revision to roll back. Sharing a database between them would
	// leave the apply dry runs with nothing pending, so the writers would never
	// be entered and the pin would hold vacuously.
	applyDB := filepath.Join(c.TempDir(), "pin-apply.db")
	downDB := filepath.Join(c.TempDir(), "pin-down.db")
	applyForReal(c, binPath, downDB, migrationsDir)

	tests := []struct {
		name string
		args []string
		// wantPlanned proves the run actually reached the writers, so the
		// narration assertions below can never pass on an empty plan.
		wantPlanned string
	}{
		{
			name: "apply default format",
			args: []string{
				"migrate", "apply",
				"--url", "sqlite://" + applyDB,
				"--dir", "file://" + migrationsDir,
				"--dry-run",
			},
			wantPlanned: "Would have applied 1 migrations.",
		},
		{
			name: "apply go template",
			args: []string{
				"migrate", "apply",
				"--url", "sqlite://" + applyDB,
				"--dir", "file://" + migrationsDir,
				"--dry-run", "--format", "{{ json . }}",
			},
			wantPlanned: `"Target":"1"`,
		},
		{
			name: "down go template",
			args: []string{
				"migrate", "down",
				"--url", "sqlite://" + downDB,
				"--dir", "file://" + migrationsDir,
				"--to-version", "0",
				"--dry-run", "--format", "{{ json . }}",
			},
			wantPlanned: `"Planned":[{"Name":"1_init.sql"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCompatProcess(binPath, tt.args...)
			run.Stdin = strings.NewReader("YES\n")
			combined, err := run.CombinedOutput()

			c.Assert(err, qt.IsNil, qt.Commentf("%s", combined))
			c.Assert(string(combined), qt.Contains, tt.wantPlanned)
			c.Assert(string(combined), qt.Not(qt.Contains), "[DRY RUN]")
			c.Assert(string(combined), qt.Not(qt.Contains), "level=INFO")
		})
	}
}

// TestCompatBinaryValidCircularForeignKeysDoNotWarn verifies that two-phase
// foreign-key rendering treats valid cycles as ordinary schema relationships.
func TestCompatBinaryValidCircularForeignKeysDoNotWarn(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := c.TempDir()
	schemaPath := filepath.Join(dir, "circular.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE authors (\n"+
			"  id INTEGER PRIMARY KEY,\n"+
			"  favorite_book_id INTEGER REFERENCES books(id)\n"+
			");\n"+
			"CREATE TABLE books (\n"+
			"  id INTEGER PRIMARY KEY,\n"+
			"  author_id INTEGER REFERENCES authors(id)\n"+
			");\n"), 0o600), qt.IsNil)

	tests := []struct {
		name string
		mode string
	}{
		{name: "dry run", mode: "--dry-run"},
		{name: "real apply", mode: "--auto-approve"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			runDir := c.TempDir()
			run := newCompatProcess(binPath,
				"schema", "apply",
				"--url", "sqlite://"+filepath.Join(runDir, "target.db"),
				"--to", "file://"+schemaPath,
				"--dev-url", "sqlite://"+filepath.Join(runDir, "dev.db"),
				tt.mode,
			)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()

			c.Assert(err, qt.IsNil, qt.Commentf("stderr=%s", stderr.String()))
			c.Assert(stderr.String(), qt.Equals, "")
		})
	}
}

// TestCompatBinaryDryRunFailuresStillReportOnStderr keeps the quiet default
// from turning into silence: a dry run that fails still explains itself.
func TestCompatBinaryDryRunFailuresStillReportOnStderr(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	tests := []struct {
		name       string
		args       func(c *qt.C) []string
		wantStderr string
	}{
		{
			name: "missing migration directory",
			args: func(c *qt.C) []string {
				return []string{
					"migrate", "apply",
					"--url", "sqlite://" + filepath.Join(c.TempDir(), "missing.db"),
					"--dir", "file://" + filepath.Join(c.TempDir(), "absent"),
					"--dry-run",
					"--format", "{{ json . }}",
				}
			},
			wantStderr: "no such file or directory",
		},
		{
			name: "checksum mismatch",
			args: func(c *qt.C) []string {
				return []string{
					"migrate", "apply",
					"--url", "sqlite://" + filepath.Join(c.TempDir(), "tampered.db"),
					"--dir", "file://" + malformedAtlasDir(c),
					"--dry-run",
				}
			},
			wantStderr: "Error: checksum mismatch\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			run := newCompatProcess(binPath, tt.args(c)...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr)
			c.Assert(exitErr.ExitCode(), qt.Equals, 1)
			c.Assert(stderr.String(), qt.Contains, tt.wantStderr)
		})
	}
}
