package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// unreachableDevURL points at a directory that does not exist, so opening the
// dev database fails at connect time. It is the same URL the issue reproduced
// with, and the pinned community binary v1.3.0 exits 1 on it under --dry-run
// and under --auto-approve alike.
const unreachableDevURL = "sqlite:///nonexistent-dir-940-xyz/dev.db"

func writeSchemaApplyGateFixture(c *qt.C, dir, table string) string {
	c.Helper()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE "+table+" (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return schemaPath
}

func runSchemaApply(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"schema", "apply"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaApplyDryRunRefusesAnUnreachableDevDatabase is the regression test
// for stokaro/ptah#940 item A: `schema apply --dry-run` returned the plan and
// exited 0 with a dev database the real apply refuses, so a CI job gating on the
// dry run was a false green.
//
// TestSchemaApplyDryRunPlansOnAReachableDevDatabase is the control that keeps
// this one honest: it proves the probe separates the two dev URLs rather than
// failing every dry run.
func TestSchemaApplyDryRunRefusesAnUnreachableDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	schemaPath := writeSchemaApplyGateFixture(c, dir, "rehearsal_users")

	out, err := runSchemaApply(c,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--dev-url", unreachableDevURL,
		"--dry-run",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err, qt.ErrorMatches, `(?s).*--dev-url.*`)
	c.Assert(sqliteTableCount(c, dbPath, "rehearsal_users"), qt.Equals, 0)
}

// TestSchemaApplyDryRunPlansOnAReachableDevDatabase is the control for the
// refusal above. The dry run still has to reach a plan on a dev database that
// opens, and it still has to leave the target untouched while doing so.
func TestSchemaApplyDryRunPlansOnAReachableDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	schemaPath := writeSchemaApplyGateFixture(c, dir, "rehearsal_users")

	out, err := runSchemaApply(c,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Planned schema changes:")
	c.Assert(sqliteTableCount(c, dbPath, "rehearsal_users"), qt.Equals, 0)
}

// TestSchemaApplyDryRunAndApplyAgreeOnADevDatabase states item A's contract
// directly: the dry run and the real apply must reach the same verdict about a
// dev database, so a plan that survives the dry run cannot be refused by the
// apply for the dev database's sake.
func TestSchemaApplyDryRunAndApplyAgreeOnADevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaApplyGateFixture(c, dir, "agree_users")

	dryOut, dryErr := runSchemaApply(c,
		"--url", "sqlite://"+filepath.Join(dir, "dry.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", unreachableDevURL,
		"--dry-run",
	)
	applyOut, applyErr := runSchemaApply(c,
		"--url", "sqlite://"+filepath.Join(dir, "apply.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", unreachableDevURL,
		"--auto-approve",
	)

	c.Assert(dryErr, qt.IsNotNil, qt.Commentf("%s", dryOut))
	c.Assert(applyErr, qt.IsNotNil, qt.Commentf("%s", applyOut))
	c.Assert(sqliteTableCount(c, filepath.Join(dir, "apply.db"), "agree_users"), qt.Equals, 0)
}

// TestSchemaApplyRequiresDevURLForNonDatabaseSource is the regression test for
// stokaro/ptah#940 item D: `schema apply --to file://…` with no --dev-url at all
// planned and APPLIED, where the pinned community binary v1.3.0 exits 1 with
// `--dev-url cannot be empty`. It is the branch item A's fix cannot reach,
// because an empty dev URL makes the rehearsal a no-op.
func TestSchemaApplyRequiresDevURLForNonDatabaseSource(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "dry run", args: []string{"--dry-run"}},
		{name: "real apply", args: []string{"--auto-approve"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "target.db")
			schemaPath := writeSchemaApplyGateFixture(c, dir, "gate_users")

			out, err := runSchemaApply(c, append([]string{
				"--url", "sqlite://" + dbPath,
				"--to", "file://" + schemaPath,
			}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, `--dev-url cannot be empty`, qt.Commentf("%s", out))
			c.Assert(sqliteTableCount(c, dbPath, "gate_users"), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyDatabaseSourceNeedsNoDevURL is item D's negative control: the
// requirement is scoped to desired states that are not already a database, and
// the community binary exits 0 on this one with no --dev-url.
func TestSchemaApplyDatabaseSourceNeedsNoDevURL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	schemaPath := writeSchemaApplyGateFixture(c, dir, "source_users")

	seedOut, seedErr := runSchemaApply(c,
		"--url", "sqlite://"+sourcePath,
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--auto-approve",
	)
	c.Assert(seedErr, qt.IsNil, qt.Commentf("%s", seedOut))

	out, err := runSchemaApply(c,
		"--url", "sqlite://"+targetPath,
		"--to", "sqlite://"+sourcePath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c, targetPath, "source_users"), qt.Equals, 1)
}

// TestSchemaApplyWithoutDevURLEnvVarRestoresPlanning pins the escape hatch item
// D's default gate is paired with: compatibility never removes a capability, so
// planning a file desired state with no dev database stays reachable on the same
// surface.
func TestSchemaApplyWithoutDevURLEnvVarRestoresPlanning(t *testing.T) {
	c := qt.New(t)
	allowSchemaApplyWithoutDevURL(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	schemaPath := writeSchemaApplyGateFixture(c, dir, "restored_users")

	out, err := runSchemaApply(c,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "restored_users"), qt.Equals, 1)
}

// The two desired-state formats a local file can be, side by side, because
// separating them is the whole of stokaro/ptah#1334 and the fixture that missed
// it wrote only SQL. Both cells are the pinned community binary v1.3.0's own
// answers, measured on the same schema:
//
//	schema apply --url sqlite://x.db --to file://schema.hcl --auto-approve  -> exit 0, creates the table
//	schema apply --url sqlite://x.db --to file://schema.sql --auto-approve  -> exit 1, --dev-url cannot be empty
//
// An HCL document is already a schema definition and needs nothing replayed. A
// SQL file is a script that has to be run somewhere first, and that somewhere
// is the dev database.
func TestSchemaApplyDevURLRuleSeparatesTheFileFormats(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		contents string
		// wantError is "" for the cell the community binary applies, and the
		// refusal for the cell it refuses. wantOutput is what the apply has to
		// reach the target with, so the passing cell proves an apply and not
		// merely a gate that let it past.
		wantError  string
		wantOutput string
	}{
		{
			name: "an HCL desired state applies with no dev database",
			file: "schema.hcl",
			contents: "schema \"main\" {\n}\n" +
				"table \"parity_hcl\" {\n  schema = schema.main\n" +
				"  column \"id\" {\n    null = false\n    type = integer\n  }\n" +
				"  primary_key {\n    columns = [column.id]\n  }\n}\n",
			wantOutput: "parity_hcl",
		},
		{
			name:      "a SQL desired state still requires one",
			file:      "schema.sql",
			contents:  "CREATE TABLE parity_sql (id INTEGER PRIMARY KEY);\n",
			wantError: "--dev-url cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			schemaPath := filepath.Join(dir, tt.file)
			c.Assert(os.WriteFile(schemaPath, []byte(tt.contents), 0o600), qt.IsNil)
			dbPath := filepath.Join(dir, "target.db")

			out, err := runSchemaApply(c,
				"--url", "sqlite://"+dbPath,
				"--to", "file://"+schemaPath,
				"--auto-approve",
			)

			c.Assert(schemaApplyErrorText(err), qt.Contains, tt.wantError,
				qt.Commentf("output:\n%s", out))
			c.Assert(err == nil, qt.Equals, tt.wantError == "")
			c.Assert(out, qt.Contains, tt.wantOutput)
		})
	}
}

// schemaApplyErrorText is "" for a nil error, so one assertion covers the cell
// that applies and the cell that is refused.
func schemaApplyErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
