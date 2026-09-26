package schema_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/root"
)

// runApplyThroughRoot runs `ptah schema apply` from the root command. The
// PTAH_* binding is installed there, so a tree assembled from the schema
// command alone reads no environment variable and can say nothing about a
// fence that arrives through one.
func runApplyThroughRoot(args ...string) (stdout, stderr string, err error) {
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"schema", "apply"}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// saveRowsPlan saves, with no fence, the plan that renames the fixture's one
// region, and returns its path.
func (f rowsFixture) saveRowsPlan(c *qt.C) string {
	c.Helper()
	planPath := filepath.Join(f.dir, "rename-region.plan.json")
	out, err := runSchema("", "plan", "--db-url", f.dbURL, "--root-dir", f.entities, "--output", planPath)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return planPath
}

// regionName reads the name the fixture's regions table holds for code.
func regionName(c *qt.C, dbPath, code string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var name string
	c.Assert(conn.QueryRowContext(context.Background(),
		`SELECT name FROM regions WHERE code = ?`, code).Scan(&name), qt.IsNil)
	return name
}

// protectedTableSources are the two spellings of one fence. Each row sets
// PTAH_PROTECTED_TABLE, to an empty value where the fence is typed: the
// binding reads an empty value as no value, so the typed row runs with no
// fence from the environment, whatever the shell exported.
var protectedTableSources = []struct {
	name        string
	environment string
	args        []string
	source      string
}{
	{
		name:   "typed flag",
		args:   []string{"--protected-table", "regions"},
		source: "--protected-table",
	},
	{
		name:        "environment",
		environment: "regions",
		source:      "PTAH_PROTECTED_TABLE",
	},
}

// TestSchemaApplyPlanRefusesAProtectedTable is the fence on the plan-file path.
// A saved plan records its statements, not which declared row sets they
// change, so this apply cannot decide the fence. Without the refusal it takes
// the fence and applies the plan anyway, and the row the fence names changes.
func TestSchemaApplyPlanRefusesAProtectedTable(t *testing.T) {
	for _, tc := range protectedTableSources {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			f := newRowsFixture(c)
			planPath := f.saveRowsPlan(c)
			t.Setenv("PTAH_PROTECTED_TABLE", tc.environment)

			stdout, stderr, err := runApplyThroughRoot(append([]string{
				"--db-url", f.dbURL, "--plan", planPath, "--auto-approve", "--json",
			}, tc.args...)...)

			c.Assert(err, qt.ErrorMatches,
				`ptah schema apply --plan cannot be combined with `+tc.source+`: `+
					`the fence is read when the plan is computed.*`,
				qt.Commentf("stderr:\n%s", stderr))
			c.Assert(err, qt.ErrorMatches, `.*ptah schema plan --protected-table.*`)
			c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout), qt.DeepEquals, atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeFailed,
				Error:           err.Error(),
			})
			c.Assert(regionName(c, f.dbPath, "NO"), qt.Equals, "Norway")
		})
	}
}

// TestSchemaApplyPlanWithoutAFenceChangesTheRow is the control for the refusal
// above: the same plan with no fence renames the region, so the row the
// refusal leaves alone is one the plan would have changed.
func TestSchemaApplyPlanWithoutAFenceChangesTheRow(t *testing.T) {
	c := qt.New(t)
	f := newRowsFixture(c)
	planPath := f.saveRowsPlan(c)
	t.Setenv("PTAH_PROTECTED_TABLE", "")

	_, stderr, err := runApplyThroughRoot("--db-url", f.dbURL, "--plan", planPath, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(regionName(c, f.dbPath, "NO"), qt.Equals, "Norge")
}

// TestSchemaApplyWithoutAPlanRefusesAProtectedTable is the control on the
// computed path: both spellings reach the fence there, and the apply refuses
// the change with the protected-table refusal rather than a usage error.
func TestSchemaApplyWithoutAPlanRefusesAProtectedTable(t *testing.T) {
	for _, tc := range protectedTableSources {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			f := newRowsFixture(c)
			t.Setenv("PTAH_PROTECTED_TABLE", tc.environment)

			stdout, stderr, err := runApplyThroughRoot(append([]string{
				"--db-url", f.dbURL, "--root-dir", f.entities, "--auto-approve", "--json",
			}, tc.args...)...)

			c.Assert(err, qt.ErrorMatches, `refusing to change protected table\(s\) regions: .*`,
				qt.Commentf("stderr:\n%s", stderr))
			c.Assert(decodeOneDocument[atlasschema.ApplyReport](c, stdout).Refusal, qt.DeepEquals,
				&atlasschema.Refusal{Code: atlasschema.RefusalProtectedTable, Tables: []string{"regions"}})
			c.Assert(regionName(c, f.dbPath, "NO"), qt.Equals, "Norway")
		})
	}
}
