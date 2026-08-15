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

func writeSchemaApplyGateFixture(tb testing.TB, dir, table string) string {
	c := qt.New(tb)
	c.Helper()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath,
		[]byte("CREATE TABLE "+table+" (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return schemaPath
}

func runSchemaApply(tb testing.TB, args ...string) (string, error) {
	c := qt.New(tb)
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"schema", "apply"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaApplyDryRunRehearsesOnDevDatabase is the regression test for
// stokaro/ptah#940 item A: `schema apply --dry-run` returned the plan and exited
// 0 with a dev database the real apply refuses, so a CI job gating on the dry
// run was a false green.
//
// The reachable row is the control that keeps the failing row honest: it proves
// the probe separates the two dev URLs rather than failing every dry run.
func TestSchemaApplyDryRunRehearsesOnDevDatabase(t *testing.T) {
	tests := []struct {
		name   string
		devURL func(dir string) string
		assert func(c *qt.C, out string, err error)
	}{
		{
			name:   "an unreachable dev database refuses the dry run",
			devURL: func(string) string { return unreachableDevURL },
			assert: func(c *qt.C, out string, err error) {
				c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
				c.Assert(err, qt.ErrorMatches, `(?s).*--dev-url.*`)
			},
		},
		{
			name:   "a reachable dev database passes the dry run",
			devURL: func(dir string) string { return "sqlite://" + filepath.Join(dir, "dev.db") },
			assert: func(c *qt.C, out string, err error) {
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				c.Assert(out, qt.Contains, "Planned schema changes:")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "target.db")
			schemaPath := writeSchemaApplyGateFixture(c.TB, dir, "rehearsal_users")

			out, err := runSchemaApply(c.TB,
				"--url", "sqlite://"+dbPath,
				"--to", "file://"+schemaPath,
				"--dev-url", test.devURL(dir),
				"--dry-run",
			)

			test.assert(c, out, err)
			c.Assert(sqliteTableCount(c.TB, dbPath, "rehearsal_users"), qt.Equals, 0)
		})
	}
}

// TestSchemaApplyDryRunAndApplyAgreeOnADevDatabase states item A's contract
// directly: the dry run and the real apply must reach the same verdict about a
// dev database, so a plan that survives the dry run cannot be refused by the
// apply for the dev database's sake.
func TestSchemaApplyDryRunAndApplyAgreeOnADevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaApplyGateFixture(c.TB, dir, "agree_users")

	dryOut, dryErr := runSchemaApply(c.TB,
		"--url", "sqlite://"+filepath.Join(dir, "dry.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", unreachableDevURL,
		"--dry-run",
	)
	applyOut, applyErr := runSchemaApply(c.TB,
		"--url", "sqlite://"+filepath.Join(dir, "apply.db"),
		"--to", "file://"+schemaPath,
		"--dev-url", unreachableDevURL,
		"--auto-approve",
	)

	c.Assert(dryErr, qt.IsNotNil, qt.Commentf("%s", dryOut))
	c.Assert(applyErr, qt.IsNotNil, qt.Commentf("%s", applyOut))
	c.Assert(sqliteTableCount(c.TB, filepath.Join(dir, "apply.db"), "agree_users"), qt.Equals, 0)
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
			schemaPath := writeSchemaApplyGateFixture(c.TB, dir, "gate_users")

			out, err := runSchemaApply(c.TB, append([]string{
				"--url", "sqlite://" + dbPath,
				"--to", "file://" + schemaPath,
			}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, `--dev-url cannot be empty`, qt.Commentf("%s", out))
			c.Assert(sqliteTableCount(c.TB, dbPath, "gate_users"), qt.Equals, 0)
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
	schemaPath := writeSchemaApplyGateFixture(c.TB, dir, "source_users")

	seedOut, seedErr := runSchemaApply(c.TB,
		"--url", "sqlite://"+sourcePath,
		"--to", "file://"+schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--auto-approve",
	)
	c.Assert(seedErr, qt.IsNil, qt.Commentf("%s", seedOut))

	out, err := runSchemaApply(c.TB,
		"--url", "sqlite://"+targetPath,
		"--to", "sqlite://"+sourcePath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c.TB, targetPath, "source_users"), qt.Equals, 1)
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
	schemaPath := writeSchemaApplyGateFixture(c.TB, dir, "restored_users")

	out, err := runSchemaApply(c.TB,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+schemaPath,
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c.TB, dbPath, "restored_users"), qt.Equals, 1)
}
