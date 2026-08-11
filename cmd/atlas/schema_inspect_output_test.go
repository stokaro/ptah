package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// runSchemaInspectOutput runs `atlas schema inspect` against dbPath with extra
// arguments and returns what reached stdout.
func runSchemaInspectOutput(c *qt.C, dbPath string, extra ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	args := append([]string{"schema", "inspect", "--url", "sqlite://" + dbPath}, extra...)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaInspectOutputWritesTheFile pins where the rendered schema goes.
//
// Reverted, every row fails with `unknown flag: --output` (or `-o`).
func TestSchemaInspectOutputWritesTheFile(t *testing.T) {
	tests := []struct {
		name     string
		flagName string
		format   []string
		contains string
	}{
		{
			name:     "long flag writes HCL",
			flagName: "--output",
			contains: `table "users"`,
		},
		{
			name:     "shorthand writes HCL",
			flagName: "-o",
			contains: `table "users"`,
		},
		{
			name:     "the chosen format is what lands in the file",
			flagName: "--output",
			format:   []string{"--format", "sql"},
			contains: "CREATE TABLE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dbPath := filepath.Join(dir, "inspect-output.db")
			createSQLiteSchemaCleanTable(c, dbPath, "users")
			target := filepath.Join(dir, "schema.out")

			args := append([]string{test.flagName, target}, test.format...)
			out, err := runSchemaInspectOutput(c, dbPath, args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			// Nothing on stdout: the file is the output, and a pipeline that
			// redirects both would otherwise get the document twice.
			c.Assert(out, qt.Equals, "")
			written, readErr := os.ReadFile(target)
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(written), qt.Contains, test.contains)
		})
	}
}

// TestSchemaInspectWithoutOutputStillPrints pins the unflagged path, so routing
// the file write cannot silence the default.
func TestSchemaInspectWithoutOutputStillPrints(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "inspect-stdout.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")

	out, err := runSchemaInspectOutput(c, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "users"`)
}

// TestSchemaInspectOutputReplacesAnExistingFile pins that a repeated
// inspection overwrites rather than refusing or appending.
func TestSchemaInspectOutputReplacesAnExistingFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "inspect-replace.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")
	target := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(target, []byte("stale contents\n"), 0o600), qt.IsNil)

	out, err := runSchemaInspectOutput(c, dbPath, "--output", target)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	written, readErr := os.ReadFile(target)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(written), qt.Not(qt.Contains), "stale contents")
	c.Assert(string(written), qt.Contains, `table "users"`)
}

// TestSchemaInspectOutputReportsAnUnwritableDestination checks that a bad
// destination fails loudly instead of dropping the inspection on the floor.
func TestSchemaInspectOutputReportsAnUnwritableDestination(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "inspect-unwritable.db")
	createSQLiteSchemaCleanTable(c, dbPath, "users")
	target := filepath.Join(dir, "no-such-directory", "schema.hcl")

	out, err := runSchemaInspectOutput(c, dbPath, "--output", target)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "stage output file")
}
