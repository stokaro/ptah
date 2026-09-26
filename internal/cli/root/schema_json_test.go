package root_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/root"
)

// These rows drive `ptah schema plan --json` and `ptah schema apply --json`
// through the root command, which is where the PTAH_* binding is installed and
// where a refused command line becomes a diagnostic and an exit status.

// runRoot runs the native tree the way the process does and reports the
// status it would exit with.
func runRoot(c *qt.C, args ...string) (stdout, stderr string, code int) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetIn(strings.NewReader(""))
	code = root.RunContext(c.Context(), cmd, args...)
	return out.String(), errOut.String(), code
}

// schemaJSONFixture is an empty SQLite target and a desired schema of two
// tables, so a plan against it has two statements.
func schemaJSONFixture(c *qt.C) (dbURL, schemaPath string) {
	c.Helper()
	dir := c.TempDir()
	schemaPath = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	return atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")), schemaPath
}

// TestSchemaJSONCommandLineRefusalsPrintNoDocument pins the exception to "one
// document on success and on failure". Each command line here is refused
// before the verb starts, so no run exists to report: standard output stays
// empty, the diagnostic goes to standard error, and the status is 2.
func TestSchemaJSONCommandLineRefusalsPrintNoDocument(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "an unknown flag",
			args:    []string{"schema", "plan", "--json", "--nope"},
			wantErr: `unknown flag: --nope`,
		},
		{
			name:    "a --json value that does not parse",
			args:    []string{"schema", "apply", "--json=maybe"},
			wantErr: `invalid argument "maybe" for "--json" flag: strconv\.ParseBool: parsing "maybe": invalid syntax`,
		},
		{
			name: "flags that cannot be combined on plan",
			args: []string{"schema", "plan", "--json", "--save", "--dry-run"},
			wantErr: `if any flags in the group \[save dry-run\] are set none of the others can be; ` +
				`\[dry-run save\] were all set`,
		},
		{
			name: "flags that cannot be combined on apply",
			args: []string{"schema", "apply", "--json", "--to", "file://schema.sql", "--root-dir", "."},
			wantErr: `if any flags in the group \[to root-dir\] are set none of the others can be; ` +
				`\[root-dir to\] were all set`,
		},
		{
			name:    "a positional argument",
			args:    []string{"schema", "apply", "--json", "extra"},
			wantErr: `unexpected positional arguments \["extra"\]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runRoot(c, tt.args...)

			c.Assert(code, qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Matches, "error: "+tt.wantErr+"\n")
		})
	}
}

// TestSchemaJSONEnvironmentRefusalPrintsNoDocument is the same exception for a
// value the environment supplies: a variable that does not parse refuses the
// command line before the verb starts.
func TestSchemaJSONEnvironmentRefusalPrintsNoDocument(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_DRY_RUN", "maybe")
	dbURL, schemaPath := schemaJSONFixture(c)

	stdout, stderr, code := runRoot(c, "schema", "apply",
		"--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve", "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "error: invalid boolean value \"maybe\" for PTAH_DRY_RUN\n")
}

// TestSchemaVerbsDoNotReadPTAHJSON holds the schema verbs' output to the
// command line. PTAH_JSON is exported for the versioned verbs, and it must not
// turn a plan preview or an apply transcript into a document nobody asked for.
// A value that does not parse is not refused either, since these verbs do not
// read the variable at all.
func TestSchemaVerbsDoNotReadPTAHJSON(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "an exported value", value: "1"},
		{name: "a value that does not parse", value: "maybe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_JSON", tt.value)
			dbURL, schemaPath := schemaJSONFixture(c)

			preview, previewErr, code := runRoot(c, "schema", "plan",
				"--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", previewErr))
			decoder := json.NewDecoder(strings.NewReader(preview))
			decoder.DisallowUnknownFields()
			var plan atlasschema.PlanFile
			c.Assert(decoder.Decode(&plan), qt.IsNil, qt.Commentf("stdout:\n%s", preview))
			c.Assert(plan.Statements, qt.HasLen, 2)

			transcript, transcriptErr, code := runRoot(c, "schema", "apply",
				"--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", transcriptErr))
			c.Assert(transcript, qt.Matches, `(?s)Planned schema changes:\n.*Schema apply completed successfully\.\n`)
			c.Assert(transcriptErr, qt.Equals, "")
		})
	}
}

// TestPTAHJSONStillReachesTheVersionedVerbs is the control for the row above:
// the variable reaches the verbs it belongs to, so the schema verbs were closed
// to it rather than the variable dropped from the tree.
func TestPTAHJSONStillReachesTheVersionedVerbs(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_JSON", "1")
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrationsDir, 0o750), qt.IsNil)

	stdout, stderr, code := runRoot(c, "migrations", "status",
		"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "versioned.db")),
		"--migrations-dir", migrationsDir)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
	var status struct {
		ContractVersion int `json:"contract_version"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &status), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(status.ContractVersion, qt.Not(qt.Equals), 0)
}

// TestSchemaApplyJSONRefusesAnExportedEdit is the --edit refusal on the
// spelling a command line does not show. PTAH_EDIT is bound to --edit, so an
// environment that exports it opens an editor on every apply, and under --json
// the refusal has to name the variable rather than a flag nobody typed. The
// editor named here does not exist, so a run that reached it would fail on the
// launch rather than on the refusal.
func TestSchemaApplyJSONRefusesAnExportedEdit(t *testing.T) {
	c := qt.New(t)
	c.Setenv("PTAH_EDIT", "1")
	c.Setenv("VISUAL", "")
	c.Setenv("EDITOR", filepath.Join(c.TempDir(), "no-such-editor"))
	dbURL, schemaPath := schemaJSONFixture(c)

	stdout, stderr, code := runRoot(c, "schema", "apply",
		"--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve", "--json")

	const refusal = "PTAH_EDIT cannot be combined with --json: the JSON document is read by a program, " +
		"and --edit waits for a person in an editor"
	c.Assert(code, qt.Equals, 2)
	c.Assert(stderr, qt.Equals, "error: "+refusal+"\n")
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var report atlasschema.ApplyReport
	c.Assert(decoder.Decode(&report), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(report, qt.DeepEquals, atlasschema.ApplyReport{
		ContractVersion: atlasschema.ApplyReportContractVersion,
		Outcome:         atlasschema.ApplyOutcomeFailed,
		Error:           refusal,
	})
}
