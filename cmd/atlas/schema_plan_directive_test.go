package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// TestSchemaPlanDirectiveIsWrittenIntoThePlanFile is the row that left the
// parent verb's refusal table.
//
// The assertion is the FILE, in the exact position the Atlas directive family
// is read from: the unbroken comment run that starts the migration body,
// closed by a blank line. A directive written anywhere else parses, saves and
// exits 0 while governing nothing (stokaro/ptah#1700).
func TestSchemaPlanDirectiveIsWrittenIntoThePlanFile(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := planFormatFixture(c, "directive")
	planPath := filepath.Join(fixture.dir, "directive.plan.hcl")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "-d", "atlas:txmode none")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	saved, readErr := os.ReadFile(planPath) // #nosec G304 -- test-controlled path
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(saved), qt.Contains, "  -- atlas:txmode none\n\n  CREATE TABLE")
}

// TestSchemaPlanDirectiveRoundTripsThroughThePlanReader is the property the
// issue asks for by name: a spelling Ptah's own reader reads back.
func TestSchemaPlanDirectiveRoundTripsThroughThePlanReader(t *testing.T) {
	tests := []struct {
		name string
		file string
	}{
		{name: "atlas hcl", file: "round.plan.hcl"},
		{name: "native json", file: "round.plan.json"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chdirToScratchC(c)
			fixture := planFormatFixture(c, "round")
			planPath := filepath.Join(fixture.dir, test.file)

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args("--output", planPath, "-d", "atlas:txmode none")...)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			plan, _, readErr := atlasschema.ReadPlanDocument(planPath)

			c.Assert(readErr, qt.IsNil)
			mode, modeErr := atlasschema.PlanTxMode(planPath, plan.SQL())
			c.Assert(modeErr, qt.IsNil)
			c.Assert(string(mode), qt.Equals, "none")
		})
	}
}

// TestSchemaPlanDirectiveRefusesWhatNothingActsOn keeps the flag from writing
// decoration, and does it before a plan file exists.
func TestSchemaPlanDirectiveRefusesWhatNothingActsOn(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "unknown atlas directive",
			args: []string{"-d", "atlas:checkpoint"},
			want: `--directive "atlas:checkpoint": Ptah writes only directives something acts on, .*`,
		},
		{
			name: "outside the namespace",
			args: []string{"-d", "txmode none"},
			want: `--directive "txmode none": a plan directive is written in the atlas namespace; .*`,
		},
		{
			name: "refused transaction mode",
			args: []string{"-d", "atlas:txmode all"},
			want: `--directive "atlas:txmode all": txmode "all" is not allowed in file directive .*`,
		},
		{
			name: "two transaction modes",
			args: []string{"-d", "atlas:txmode none", "-d", "atlas:txmode file"},
			want: `--directive "atlas:txmode file": the plan already sets a transaction mode, .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := chdirToScratchC(c)
			fixture := planFormatFixture(c, "reject")

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args(append([]string{"--save"}, test.args...)...)...)

			c.Assert(err, qt.ErrorMatches, test.want, qt.Commentf("%s", out))
			// The refusal has to protect the artifact, not just the exit code.
			assertNoPlanFileWritten(c, scratch)
			assertNoPlanFileWritten(c, fixture.dir)
		})
	}
}

// TestSchemaPlanNoLintDirectiveSilencesThePlanLinter proves the second
// supported key is honored, and by a different subsystem than the first.
//
// That difference is the reason directives live in the migration text rather
// than in a field of the plan file: `schema plan lint` reads `atlas:nolint`
// out of the SQL, and a plan-file layer that lifted the header into a struct
// would have had to hand it back.
func TestSchemaPlanNoLintDirectiveSilencesThePlanLinter(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture := newPlanFixture(c, "nolint-flag",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE drop_me (id INTEGER PRIMARY KEY);",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)
	planPath := filepath.Join(fixture.dir, "nolint.plan.hcl")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "-d", "atlas:nolint destructive")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	stdout, stderr, lintErr := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "lint",
		fixture.args("--file", "file://"+planPath)...)

	c.Assert(lintErr, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Not(qt.Contains), "destructive changes detected")
}

// planDirectiveApplyFixture plans a transition with `atlas:txmode none`
// recorded and returns the saved plan file's path.
func planDirectiveApplyFixture(c *qt.C, name string) (planFixture, string) {
	c.Helper()
	fixture := planFormatFixture(c, name)
	planPath := filepath.Join(fixture.dir, name+".plan.hcl")
	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "-d", "atlas:txmode none")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return fixture, planPath
}

// TestSchemaPlanApplyHonorsTheTransactionModeDirective is the directive doing
// work rather than being recorded.
//
// --tx-mode is not passed in the first row, so the global mode is the default
// `file` and the plan's `none` overrides it exactly as a migration file's
// directive does; the second row is the control that the same plan still
// applies when the operator names the mode themselves. What the directive
// changes about the DATABASE needs a target that can tell the difference, and
// that is integration/atlas_schema_plan_directive_e2e_test.go.
func TestSchemaPlanApplyHonorsTheTransactionModeDirective(t *testing.T) {
	tests := []struct {
		name   string
		txMode []string
	}{
		{name: "default global mode"},
		{name: "explicit file", txMode: []string{"--tx-mode", "file"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chdirToScratchC(c)
			fixture, planPath := planDirectiveApplyFixture(c, "apply")

			applyOut, applyErr := runAtlasArgs(append([]string{
				"schema", "apply",
				"--url", fixture.dbURL,
				"--to", fixture.schemaURL,
				"--plan", "file://" + planPath,
				"--auto-approve",
			}, test.txMode...)...)

			c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applyOut))
			c.Assert(applyOut, qt.Contains, "Schema apply completed successfully.")
		})
	}
}

// TestSchemaPlanApplyRefusesTheDirectiveUnderTxModeAll is the one combination
// the shared rule refuses instead of resolving, so a plan cannot execute under
// a transaction mode its reviewer did not approve.
func TestSchemaPlanApplyRefusesTheDirectiveUnderTxModeAll(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c)
	fixture, planPath := planDirectiveApplyFixture(c, "apply-all")

	applyOut, applyErr := runAtlasArgs(
		"schema", "apply",
		"--url", fixture.dbURL,
		"--to", fixture.schemaURL,
		"--plan", "file://"+planPath,
		"--auto-approve",
		"--tx-mode", "all",
	)

	c.Assert(applyErr, qt.ErrorMatches,
		`cannot set txmode directive to "none" in ".*" when txmode "all" is set globally`,
		qt.Commentf("%s", applyOut))
}
