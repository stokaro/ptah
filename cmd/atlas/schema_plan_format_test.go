package atlas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// The pinned community binary settles nothing here: it aborts the whole
// `schema plan` path, and `--format` answers `unknown flag` on it, so there is
// no payload to match and no behavior to diverge from. What these tests pin is
// the shape this tree chose and documented (stokaro/ptah#1700).

// planFormatFixture is a live SQLite target one CREATE TABLE behind its
// desired state.
func planFormatFixture(c *qt.C, name string) planFixture {
	c.Helper()
	return newPlanFixture(c, name,
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
}

// TestSchemaPlanFormatRendersTheTemplate is what `--format` used to refuse.
func TestSchemaPlanFormatRendersTheTemplate(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := planFormatFixture(c, "format")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--dry-run", "--format", "{{ json . }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(out), &payload), qt.IsNil, qt.Commentf("%s", out))
	c.Assert(payload["Dialect"], qt.Equals, "sqlite")
	c.Assert(payload["Destructive"], qt.Equals, false)
	c.Assert(payload["From"], qt.Contains, "sha256:")
	c.Assert(payload["To"], qt.Contains, "sha256:")
	changes, ok := payload["Changes"].([]any)
	c.Assert(ok, qt.IsTrue)
	c.Assert(changes, qt.HasLen, 1)
	first, ok := changes[0].(map[string]any)
	c.Assert(ok, qt.IsTrue)
	c.Assert(first["Cmd"], qt.Contains, `CREATE TABLE "added"`)
	c.Assert(first["Severity"], qt.Equals, "safe")
}

// TestSchemaPlanFormatReplacesTheDefaultOutput proves the flag SELECTS the
// output rather than adding to it.
//
// A CI job that asked for JSON and got the plan document printed above it
// would have to strip a prefix nothing documents, so the plan-block preview
// has to be gone, not merely followed by the rendering.
func TestSchemaPlanFormatReplacesTheDefaultOutput(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := planFormatFixture(c, "replace")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--dry-run", "--format", "{{ .Name }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "plan \"")
	c.Assert(out, qt.Not(qt.Contains), "migration = <<-SQL")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE")
}

// TestSchemaPlanFormatStillSavesThePlanFile is the other half of "selects the
// output": it selects the OUTPUT, and the artifact is not output.
//
// A job that renders a document to parse and keeps the plan file in the same
// run is the reason to have both flags, and the saved-to line is suppressed
// because it would otherwise land in the middle of the rendered document.
func TestSchemaPlanFormatStillSavesThePlanFile(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := planFormatFixture(c, "saved")
	planPath := filepath.Join(fixture.dir, "saved.plan.hcl")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "--format", "{{ .Name }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Plan saved to")
	saved, readErr := os.ReadFile(planPath) // #nosec G304 -- test-controlled path
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(saved), qt.Contains, `CREATE TABLE "added"`)
}

// TestSchemaPlanFormatRendersASyncedSchemaToo is the outcome a scripted caller
// hits most often and the one a human sentence would break.
func TestSchemaPlanFormatRendersASyncedSchemaToo(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	const same = `CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`
	fixture := newPlanFixture(c, "synced", same, same)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--dry-run", "--format", "{{ json . }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
	var payload map[string]any
	c.Assert(json.Unmarshal([]byte(out), &payload), qt.IsNil, qt.Commentf("%s", out))
	// An empty list rather than null: a consumer ranging over .Changes must
	// not have to special-case the synced answer.
	c.Assert(payload["Changes"], qt.DeepEquals, make([]any, 0))
}

// TestSchemaPlanFormatMigrationBodyIsTheArtifact separates the two SQL
// readings the payload offers, on a plan that has a directive: `.Changes` and
// `sql` describe the statements, `.MigrationBody` reproduces the file.
func TestSchemaPlanFormatMigrationBodyIsTheArtifact(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := planFormatFixture(c, "body")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--dry-run", "-d", "atlas:txmode none", "--format", "{{ .MigrationBody }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "-- atlas:txmode none")
	c.Assert(out, qt.Contains, `CREATE TABLE "added"`)
}

// TestSchemaPlanFormatRefusesABadTemplateBeforeConnecting proves the template
// is parsed before any database work: the --from URL here names a file that
// does not exist, so a run that reached the connection would fail differently.
func TestSchemaPlanFormatRefusesABadTemplateBeforeConnecting(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "empty", format: "", want: `--format must not be empty`},
		{name: "unclosed action", format: "{{ json .", want: `parse --format template: .*`},
		{name: "unknown function", format: "{{ frobnicate . }}", want: `parse --format template: .*frobnicate.*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := chdirToScratchC(c)

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				// sqliteURLFromPath rather than a hand-built URL: a Windows
				// path is `C:\...`, and pasting one after `sqlite://` yields
				// something that is not a URL at all -- which would make this
				// row pass for the wrong reason on the one platform where the
				// difference shows.
				"--from", sqliteURLFromPath(filepath.Join(scratch, "absent.db")),
				"--to", "file://"+filepath.Join(scratch, "absent.sql"),
				"--save",
				"--format", test.format,
			)

			c.Assert(err, qt.ErrorMatches, test.want, qt.Commentf("%s", out))
			assertNoPlanFileWritten(c, scratch)
		})
	}
}

// TestSchemaPlanNewFormatRendersTheTemplate is the row that left `new`'s
// refusal table: the sub-verb produces a plan document, so it renders one.
func TestSchemaPlanNewFormatRendersTheTemplate(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := newSubverbFixture(c, "new-format")
	planPath := filepath.Join(fixture.dir, "new-format.plan.hcl")

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath, "--format", "{{ .Name }}")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Plan saved to")
	// `new` always writes, --format or not.
	_, statErr := os.Stat(planPath)
	c.Assert(statErr, qt.IsNil)
}
