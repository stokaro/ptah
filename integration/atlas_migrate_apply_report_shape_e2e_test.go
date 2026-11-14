//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// applyReportShapeRow is one `ptah-compat migrate apply` run in a directory of
// its own, with the migration directory at `migrations` and the database at
// `db.sqlite`, the relative paths the community binary was measured with.
type applyReportShapeRow struct {
	name   string
	format string
	// applied are written, hashed and applied before the run.
	applied map[string]string
	// added are written and hashed after the applied files, before the run.
	added map[string]string
	args  []string
	// wantStdout is the run's stdout with each Start and End timestamp
	// written as T.
	wantStdout string
	// wantStderr is the run's stderr with each log line's time attribute
	// removed.
	wantStderr string
	wantCode   int
}

var (
	applyReportShapeTimeRe    = regexp.MustCompile(`"(Start|End)":"[^"]*"`)
	applyReportShapeLogTimeRe = regexp.MustCompile(`(?m)^time=\S+ `)
)

// runApplyReportShapeBinary runs the binary under test inside dir with stdout
// and stderr kept apart.
func runApplyReportShapeBinary(ctx context.Context, dir, binaryPath string, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()
	return out.String(), errOut.String(), err
}

// writeApplyReportShapeFiles writes files into the row's migration directory
// and hashes it with the binary under test.
func writeApplyReportShapeFiles(c *qt.C, ctx context.Context, dir, binaryPath, dirURL string, files map[string]string) {
	c.Helper()
	for _, name := range slices.Sorted(maps.Keys(files)) {
		c.Assert(os.WriteFile(filepath.Join(dir, "migrations", name), []byte(files[name]), 0o600), qt.IsNil)
	}
	_, stderr, err := runApplyReportShapeBinary(ctx, dir, binaryPath, "migrate", "hash", "--dir", dirURL)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate hash: %s", stderr))
}

// runApplyReportShapeRow applies the row's history, adds its files and runs
// the apply under test, returning both streams redacted.
func runApplyReportShapeRow(c *qt.C, ctx context.Context, binaryPath string, row applyReportShapeRow) (stdout, stderr string, code int) {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	dirURL := "file://migrations" + row.format
	apply := []string{"migrate", "apply", "--url", "sqlite://db.sqlite", "--dir", dirURL}
	writeApplyReportShapeFiles(c, ctx, dir, binaryPath, dirURL, row.applied)
	_, setupErr, err := runApplyReportShapeBinary(ctx, dir, binaryPath, apply...)
	c.Assert(err, qt.IsNil, qt.Commentf("history apply: %s", setupErr))
	writeApplyReportShapeFiles(c, ctx, dir, binaryPath, dirURL, row.added)

	stdout, stderr, err = runApplyReportShapeBinary(ctx, dir, binaryPath, append(apply, row.args...)...)

	return applyReportShapeTimeRe.ReplaceAllString(stdout, `"$1":"T"`),
		applyReportShapeLogTimeRe.ReplaceAllString(stderr, ""),
		exitStatusOf(c, err)
}

const (
	applyReportShapeURL = `"Driver":"sqlite","URL":{"Scheme":"sqlite","Opaque":"","User":null,"Host":"db.sqlite",` +
		`"Path":"","Fragment":"","RawQuery":"","RawPath":"","RawFragment":"","ForceQuery":false,"OmitHost":false,"Schema":"main"}`
	applyReportShapeNonLinear = " See: https://atlasgo.io/versioned/apply#non-linear-error\n"
)

// TestAtlasMigrateApplyReportShapeE2E_Parity drives the built binary over
// directories the pinned community binary v1.3.0 was measured on, on SQLite.
// Each row is that binary's stdout, stderr and exit code byte for byte once
// the timestamps are redacted: a report leaves out each member it has no value
// for, a Flyway repeatable is named by Name alone, a run with nothing pending
// has no Target, and a linear refusal names the files it refused, writing no
// report under --format.
func TestAtlasMigrateApplyReportShapeE2E_Parity(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)
	jsonFormat := []string{"--format", "{{ json . }}"}

	tests := []applyReportShapeRow{
		{
			name:  "every member a run fills, and none it does not",
			added: map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "2_b.sql": "CREATE TABLE b (id int);\nINSERT INTO b VALUES (1);\n"},
			args:  jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations",` +
				`"Pending":[{"Name":"1_a.sql","Version":"1","Description":"a"},{"Name":"2_b.sql","Version":"2","Description":"b"}],` +
				`"Applied":[{"Name":"1_a.sql","Version":"1","Description":"a","Start":"T","End":"T","Applied":["CREATE TABLE a (id int);"]},` +
				`{"Name":"2_b.sql","Version":"2","Description":"b","Start":"T","End":"T","Applied":["CREATE TABLE b (id int);","INSERT INTO b VALUES (1);"]}],` +
				`"Target":"2","Start":"T","End":"T","Message":"Migrated to version 2 from  (2 migrations in total)"}`,
		},
		{
			name:  "a file with no statements",
			added: map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "2_empty.sql": "-- nothing here\n"},
			args:  jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations",` +
				`"Pending":[{"Name":"1_a.sql","Version":"1","Description":"a"},{"Name":"2_empty.sql","Version":"2","Description":"empty"}],` +
				`"Applied":[{"Name":"1_a.sql","Version":"1","Description":"a","Start":"T","End":"T","Applied":["CREATE TABLE a (id int);"]},` +
				`{"Name":"2_empty.sql","Version":"2","Description":"empty","Start":"T","End":"T"}],` +
				`"Target":"2","Start":"T","End":"T","Message":"Migrated to version 2 from  (2 migrations in total)"}`,
		},
		{
			name:   "a Flyway repeatable",
			format: "?format=flyway",
			added:  map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "R__view.sql": "CREATE VIEW v AS SELECT id FROM a;\n"},
			args:   jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations?format=flyway",` +
				`"Pending":[{"Name":"V1__a.sql","Version":"1","Description":"a"},{"Name":"R__view.sql","Description":"view"}],` +
				`"Applied":[{"Name":"V1__a.sql","Version":"1","Description":"a","Start":"T","End":"T","Applied":["CREATE TABLE a (id int);"]},` +
				`{"Name":"R__view.sql","Description":"view","Start":"T","End":"T","Applied":["CREATE VIEW v AS SELECT id FROM a;"]}],` +
				`"Start":"T","End":"T","Message":"Migrated to version  from  (2 migrations in total)"}`,
		},
		{
			name:    "an applied Flyway repeatable",
			format:  "?format=flyway",
			applied: map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "R__view.sql": "CREATE VIEW v AS SELECT id FROM a;\n"},
			args:    jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations?format=flyway",` +
				`"Current":"1","Start":"T","End":"T","Message":"No migration files to execute"}`,
		},
		{
			name:    "nothing pending",
			applied: map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:    jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations",` +
				`"Current":"1","Start":"T","End":"T","Message":"No migration files to execute"}`,
		},
		{
			name:       "a template reading Current and Target on a first run",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"--format", "[{{ .Current }}|{{ .Target }}]"},
			wantStdout: "[|1]",
		},
		{
			name:       "a template reading Current and Target with nothing pending",
			applied:    map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"--format", "[{{ .Current }}|{{ .Target }}]"},
			wantStdout: "[1|]",
		},
		{
			name:       "a template reading the Version of a Flyway repeatable",
			format:     "?format=flyway",
			added:      map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "R__view.sql": "CREATE VIEW v AS SELECT id FROM a;\n"},
			args:       []string{"--format", "{{ range .Pending }}[{{ .Version }}]{{ end }}{{ range .Applied }}<{{ .Version }}>{{ end }}"},
			wantStdout: "[1][]<1><>",
		},
		{
			name:       "one file out of order",
			applied:    map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "3_c.sql": "CREATE TABLE c (id int);\n"},
			added:      map[string]string{"2_b.sql": "CREATE TABLE b (id int);\n"},
			args:       []string{"--exec-order", "linear"},
			wantStderr: "Error: migration file 2_b.sql was added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			name:       "one file out of order under --format",
			applied:    map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "3_c.sql": "CREATE TABLE c (id int);\n"},
			added:      map[string]string{"2_b.sql": "CREATE TABLE b (id int);\n"},
			args:       append([]string{"--exec-order", "linear"}, jsonFormat...),
			wantStderr: "Error: migration file 2_b.sql was added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			name:    "two files out of order in a dry run under --format",
			applied: map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "4_d.sql": "CREATE TABLE d (id int);\n"},
			added: map[string]string{
				"2_b.sql": "CREATE TABLE b (id int);\n",
				"3_c.sql": "CREATE TABLE c (id int);\n",
				"5_e.sql": "CREATE TABLE e (id int);\n",
			},
			args:       append([]string{"--dry-run"}, jsonFormat...),
			wantStderr: "Error: migration files 2_b.sql, 3_c.sql were added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			name:   "a golang-migrate directory names each file as it holds it",
			format: "?format=golang-migrate",
			added: map[string]string{
				"1_a.up.sql": "CREATE TABLE a (id int);\n", "1_a.down.sql": "DROP TABLE a;\n",
				"2_b.up.sql": "CREATE TABLE b (id int);\n", "2_b.down.sql": "DROP TABLE b;\n",
			},
			args: jsonFormat,
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations?format=golang-migrate",` +
				`"Pending":[{"Name":"1_a.up.sql","Version":"1","Description":"a"},{"Name":"2_b.up.sql","Version":"2","Description":"b"}],` +
				`"Applied":[{"Name":"1_a.up.sql","Version":"1","Description":"a","Start":"T","End":"T","Applied":["CREATE TABLE a (id int);"]},` +
				`{"Name":"2_b.up.sql","Version":"2","Description":"b","Start":"T","End":"T","Applied":["CREATE TABLE b (id int);"]}],` +
				`"Target":"2","Start":"T","End":"T","Message":"Migrated to version 2 from  (2 migrations in total)"}`,
		},
		{
			name:   "a golang-migrate file out of order",
			format: "?format=golang-migrate",
			applied: map[string]string{
				"1_a.up.sql": "CREATE TABLE a (id int);\n", "1_a.down.sql": "DROP TABLE a;\n",
				"3_c.up.sql": "CREATE TABLE c (id int);\n", "3_c.down.sql": "DROP TABLE c;\n",
			},
			added:      map[string]string{"2_b.up.sql": "CREATE TABLE b (id int);\n", "2_b.down.sql": "DROP TABLE b;\n"},
			wantStderr: "Error: migration file 2_b.up.sql was added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			name:   "a golang-migrate file out of order under --format",
			format: "?format=golang-migrate",
			applied: map[string]string{
				"1_a.up.sql": "CREATE TABLE a (id int);\n", "1_a.down.sql": "DROP TABLE a;\n",
				"3_c.up.sql": "CREATE TABLE c (id int);\n", "3_c.down.sql": "DROP TABLE c;\n",
			},
			added:      map[string]string{"2_b.up.sql": "CREATE TABLE b (id int);\n", "2_b.down.sql": "DROP TABLE b;\n"},
			args:       jsonFormat,
			wantStderr: "Error: migration file 2_b.up.sql was added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			name:       "an amount that is not a number",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"x"},
			wantStderr: `Error: invalid amount argument "x" (invalid syntax). Omit the argument or pass a valid integer instead` + "\n",
			wantCode:   1,
		},
		{
			name:       "an amount that is not a number under --format",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       append([]string{"x"}, jsonFormat...),
			wantStderr: `Error: invalid amount argument "x" (invalid syntax). Omit the argument or pass a valid integer instead` + "\n",
			wantCode:   1,
		},
		{
			name:       "an amount of zero",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"0"},
			wantStderr: "Error: cannot apply '0' migration files\n",
			wantCode:   1,
		},
		{
			name:       "a negative amount under --format",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       append(slices.Clone(jsonFormat), "--", "-1"),
			wantStderr: "Error: cannot apply '-1' migration files\n",
			wantCode:   1,
		},
		{
			name:       "two amounts",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"1", "2"},
			wantStderr: "Error: accepts at most 1 arg(s), received 2\n",
			wantCode:   1,
		},
		{
			name:       "a baseline the directory does not hold",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"--baseline", "7"},
			wantStderr: `Error: baseline version "7" not found` + "\n",
			wantCode:   1,
		},
		{
			name:       "a baseline the directory does not hold under --format",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       append([]string{"--baseline", "7"}, jsonFormat...),
			wantStderr: `Error: baseline version "7" not found` + "\n",
			wantCode:   1,
		},
		{
			name:       "a baseline spelled with a leading zero",
			added:      map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n"},
			args:       []string{"--baseline", "01"},
			wantStderr: `Error: baseline version "01" not found` + "\n",
			wantCode:   1,
		},
		{
			name:    "an interval and a textual insertion in a Flyway directory",
			format:  "?format=flyway",
			applied: map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "V3__b.sql": "CREATE TABLE b (id int);\n"},
			added:   map[string]string{"V2.5__c.sql": "CREATE TABLE c (id int);\n", "V10__d.sql": "CREATE TABLE d (id int);\n"},
			wantStderr: "Error: migration files V2.5__c.sql, V10__d.sql were added out of order." +
				applyReportShapeNonLinear,
			wantCode: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runApplyReportShapeRow(c, ctx, binaryPath, test)

			c.Assert(code, qt.Equals, test.wantCode, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Equals, test.wantStderr)
			c.Assert(stdout, qt.Equals, test.wantStdout)
		})
	}
}

// TestAtlasMigrateApplyReportShapeE2E_RetainedDivergence pins the rows whose
// answer differs from the pinned community binary v1.3.0 on purpose, each with
// that binary's answer beside it.
func TestAtlasMigrateApplyReportShapeE2E_RetainedDivergence(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)
	flywayHistory := map[string]string{"V2__a.sql": "CREATE TABLE a (id int);\n"}
	flywayAdded := map[string]string{"V10__b.sql": "CREATE TABLE b (id int);\n", "V1.5__c.sql": "CREATE TABLE c (id int);\n"}

	tests := []applyReportShapeRow{
		{
			// The pinned binary skips both files and prints nothing on
			// stderr. Each skipped file gets a warning here, naming it by the
			// version its directory holds, so a skip is never silent.
			name:       "linear-skip over a Flyway directory",
			format:     "?format=flyway",
			applied:    flywayHistory,
			added:      flywayAdded,
			args:       []string{"--exec-order", "linear-skip"},
			wantStdout: "No migration files to execute\n",
			wantStderr: `level=WARN msg="Skipping out-of-order migration" version=1.5 currentVersion=2` + "\n" +
				`level=WARN msg="Skipping out-of-order migration" version=10 currentVersion=2` + "\n",
		},
		{
			// The pinned binary writes the same document and nothing on
			// stderr. The warning names the file the run left pending.
			name:    "linear-skip under --format",
			applied: map[string]string{"1_a.sql": "CREATE TABLE a (id int);\n", "3_c.sql": "CREATE TABLE c (id int);\n"},
			added:   map[string]string{"2_b.sql": "CREATE TABLE b (id int);\n"},
			args:    []string{"--exec-order", "linear-skip", "--format", "{{ json . }}"},
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations",` +
				`"Current":"3","Start":"T","End":"T","Message":"No migration files to execute"}`,
			wantStderr: `level=WARN msg="Skipping out-of-order migration" version=2 currentVersion=3` + "\n",
		},
		{
			// V1.5__c.sql sorts below every applied revision. The pinned
			// binary passes over such a prefix file without a word and names
			// only V10__b.sql; both refuse the run and apply nothing. This
			// names every file the refusal covers.
			name:       "a prefix and a textual insertion in a Flyway directory",
			format:     "?format=flyway",
			applied:    flywayHistory,
			added:      flywayAdded,
			wantStderr: "Error: migration files V1.5__c.sql, V10__b.sql were added out of order." + applyReportShapeNonLinear,
			wantCode:   1,
		},
		{
			// The pinned binary sends VACUUM inside the transaction and
			// reports the statements it sent and the database's answer in the
			// document. Ptah refuses the file before any statement is sent, so
			// the document carries the refusal and lists no statement.
			name:  "a statement refused before any is sent, under --format",
			added: map[string]string{"1_a.sql": "CREATE TABLE t1 (id int);\n", "2_b.sql": "CREATE TABLE t2 (id int);\nVACUUM;\n"},
			args:  []string{"--format", "{{ json . }}"},
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations",` +
				`"Pending":[{"Name":"1_a.sql","Version":"1","Description":"a"},{"Name":"2_b.sql","Version":"2","Description":"b"}],` +
				`"Target":"2","Start":"T","End":"T",` +
				`"Error":"2_b.sql cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block; ` +
				"mark the file `-- +ptah no_transaction`, or move the statement into a migration of its own\\nSQL: VACUUM\"}",
			wantCode: 1,
		},
		{
			// `-- +ptah check` is a Ptah directive the pinned binary reads as
			// a comment. Every text in the report names the migration by the
			// version its directory holds, never by the numeric key its
			// conversion runs under.
			name:   "a failed check in a Flyway file",
			format: "?format=flyway",
			added: map[string]string{
				"V1__a.sql":   "CREATE TABLE a (id int);\n",
				"V1.5__b.sql": "-- +ptah check assert=\"SELECT 0\"\nCREATE TABLE b (id int);\n",
			},
			args: []string{"--format", "{{ json . }}"},
			wantStdout: `{` + applyReportShapeURL + `,"Dir":"file://migrations?format=flyway",` +
				`"Pending":[{"Name":"V1__a.sql","Version":"1","Description":"a"},{"Name":"V1.5__b.sql","Version":"1.5","Description":"b"}],` +
				`"Applied":[{"Name":"V1__a.sql","Version":"1","Description":"a","Start":"T","End":"T","Applied":["CREATE TABLE a (id int);"]},` +
				`{"Name":"V1.5__b.sql","Version":"1.5","Description":"b","Start":"T","End":"T",` +
				`"Checks":[{"Stmts":[{"Stmt":"SELECT 0","Error":"pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)"}],` +
				`"Error":{"Stmt":"SELECT 0","Text":"pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)"}}],` +
				`"Error":{"Text":"pre-migration check failed for migration 1.5: pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)"}}],` +
				`"Target":"1.5","Start":"T","End":"T",` +
				`"Error":"pre-migration check failed for migration 1.5: pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)"}`,
			wantCode: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runApplyReportShapeRow(c, ctx, binaryPath, test)

			c.Assert(code, qt.Equals, test.wantCode, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Equals, test.wantStderr)
			c.Assert(stdout, qt.Equals, test.wantStdout)
		})
	}
}
