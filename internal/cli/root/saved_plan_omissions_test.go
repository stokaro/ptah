package root_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// savedPlanFixture saves the plan of leavingOutSchema to a file and returns
// the fixture and the plan's path.
func savedPlanFixture(c *qt.C) (omissionFixture, string) {
	c.Helper()
	f := newOmissionFixture(c)
	planPath := filepath.Join(f.migrations, "t.plan.json")
	code, stdout, stderr := runStreams(c, "schema", "plan", "--db-url", f.dbURL, "--schema-file", f.schema, "--output", planPath)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	return f, planPath
}

// TestSavedPlan_RecordsWhatItsStatementsLeaveOut reads the saved plan back:
// the document holds each omission the plan step warned about.
func TestSavedPlan_RecordsWhatItsStatementsLeaveOut(t *testing.T) {
	c := qt.New(t)
	_, planPath := savedPlanFixture(c)

	contents, err := os.ReadFile(planPath)

	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Contains, `"omissions"`)
	c.Assert(string(contents), qt.Contains, `"detail": "InnoDB"`)
	c.Assert(string(contents), qt.Contains, `"detail": "HASH"`)
}

// TestSavedPlan_ApplyWarnsFromTheDocument applies the saved plan. The run did
// not plan, yet it warns about each omission the document records.
func TestSavedPlan_ApplyWarnsFromTheDocument(t *testing.T) {
	c := qt.New(t)
	f, planPath := savedPlanFixture(c)

	code, stdout, stderr := runStreams(c, "schema", "apply", "--db-url", f.dbURL, "--plan", planPath, "--auto-approve")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(skippedWarnings(stderr), qt.DeepEquals, leftOutWarnings)
}

// TestSavedPlan_ApproveWarnsFromTheDocument is the approver's view: signing the
// plan names each omission the document records.
func TestSavedPlan_ApproveWarnsFromTheDocument(t *testing.T) {
	c := qt.New(t)
	_, lookErr := exec.LookPath("ssh-keygen")
	c.Assert(lookErr, qt.IsNil)
	_, planPath := savedPlanFixture(c)
	key := filepath.Join(c.TempDir(), "key")
	out, err := exec.Command("ssh-keygen", "-t", "ed25519", "-N", "", "-C", "alice@example.com", "-f", key).CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	code, stdout, stderr := runStreams(c, "schema", "approve", "--plan", planPath, "--key", key)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(skippedWarnings(stderr), qt.DeepEquals, leftOutWarnings)
}

// TestMigrationsGenerate_WarnsOnEveryRoute runs `migrations generate` through
// the replay and the shadow-database routes. Each warns once, as the direct
// route does.
func TestMigrationsGenerate_WarnsOnEveryRoute(t *testing.T) {
	tests := []struct {
		name string
		args func(f omissionFixture, scratch string) []string
	}{
		{name: "replay", args: func(f omissionFixture, scratch string) []string {
			return []string{"migrations", "generate", "--schema-file", f.schema, "--migrations-dir", f.migrations,
				"--replay", "--dev-url", "sqlite://" + filepath.ToSlash(filepath.Join(scratch, "dev.db"))}
		}},
		{name: "shadow database", args: func(f omissionFixture, scratch string) []string {
			return []string{"migrations", "generate", "--db-url", f.dbURL, "--schema-file", f.schema, "--migrations-dir", f.migrations,
				"--shadow-db", "sqlite://" + filepath.ToSlash(filepath.Join(scratch, "shadow.db"))}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newOmissionFixture(c)

			code, stdout, stderr := runStreams(c, test.args(f, c.TempDir())...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(skippedWarnings(stderr), qt.DeepEquals, leftOutWarnings)
		})
	}
}
