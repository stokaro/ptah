package migrateup_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrateup"
)

// The native surface keeps the dry-run statement narration the Atlas-compatible
// surface deliberately drops (stokaro/ptah#967): it is genuinely useful when
// reviewing a plan, and it is already routed through the command's observability
// runtime, so `--log-level` selects whether it appears and `--log-format`
// selects the stream and encoding. These pins hold that wiring, and hold the
// narration down to one "[DRY RUN] Would initialize migrations metadata" record
// per run — the dry-run revision-state fix (stokaro/ptah#963) repeated it once
// per metadata read.

// runUpStreams runs `ptah migrations up` with stdout and the log stream kept
// apart, which is what the log-format contract is about.
func runUpStreams(args ...string) (stdout, stderr string, err error) {
	cmd := migrateup.NewMigrateUpCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func dryRunUpArgs(t *testing.T, extra ...string) []string {
	t.Helper()
	args := []string{
		"--db-url", "sqlite://" + t.TempDir() + "/dry-run-logs.db",
		"--migrations-dir", writeUpMigrations(t),
		"--dry-run",
	}
	return append(args, extra...)
}

func TestMigrateUpDryRunNarratesThroughTheLogStream(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpStreams(dryRunUpArgs(t)...)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", stdout, stderr))
	// The human report stays on stdout.
	c.Assert(stdout, qt.Contains, "=== DRY RUN MODE ===")
	c.Assert(stdout, qt.Contains, "Would have applied 2 migrations")
	c.Assert(stdout, qt.Not(qt.Contains), "[DRY RUN] Would execute SQL")
	// The per-statement narration is a log record, not report output.
	c.Assert(stderr, qt.Contains, "[DRY RUN] Would begin transaction")
	c.Assert(stderr, qt.Contains, "[DRY RUN] Would execute SQL")
	c.Assert(stderr, qt.Contains, "[DRY RUN] Would commit transaction")
	// Regression pin for stokaro/ptah#963: one metadata record, not one per read.
	c.Assert(strings.Count(stderr, "[DRY RUN] Would initialize migrations metadata"), qt.Equals, 1)
}

func TestMigrateUpDryRunNarrationHonorsLogLevel(t *testing.T) {
	tests := []struct {
		name          string
		logLevel      string
		wantNarration bool
	}{
		{name: "default info level narrates", logLevel: "info", wantNarration: true},
		{name: "debug level narrates", logLevel: "debug", wantNarration: true},
		{name: "warn level is quiet", logLevel: "warn", wantNarration: false},
		{name: "error level is quiet", logLevel: "error", wantNarration: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := runUpStreams(dryRunUpArgs(t, "--log-level", tt.logLevel)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", stdout, stderr))
			// The stdout report is unaffected by the log level either way.
			c.Assert(stdout, qt.Contains, "Would have applied 2 migrations")
			c.Assert(strings.Contains(stderr, "[DRY RUN] Would execute SQL"), qt.Equals, tt.wantNarration,
				qt.Commentf("stderr=%s", stderr))
		})
	}
}

func TestMigrateUpDryRunJSONLogFormatKeepsStdoutParseable(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpStreams(dryRunUpArgs(t, "--log-format", "json")...)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout=%s stderr=%s", stdout, stderr))
	// JSON mode folds the human report into the log stream, so stdout carries
	// one JSON record per line and the error stream stays clean.
	c.Assert(stderr, qt.Equals, "")
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	c.Assert(len(lines) > 0, qt.IsTrue)
	for _, line := range lines {
		var record map[string]any
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("line: %s", line))
	}
	c.Assert(stdout, qt.Contains, `"[DRY RUN] Would execute SQL"`)
}
