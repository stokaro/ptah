package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/safety"
)

// `atlas schema plan new` has no oracle: CE aborts the whole `schema plan`
// path and does not know the sub-verb name (see the measurements in
// schema_plan_subverb_surface_test.go). Its flag set comes from the published
// Atlas CLI reference; what the verb does with those flags is not established
// there. The tests below therefore pin two different kinds of claim, and say
// which: SURFACE claims follow the published reference, BEHAVIOR claims are the
// reading this tree implements.

// newSubverbFixture is the additive fixture the `new` tests share: one live
// SQLite table, a desired state with one more.
func newSubverbFixture(tb testing.TB, name string) planFixture {
	c := qt.New(tb)
	c.Helper()
	return newPlanFixture(c.TB, name,
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE added (id INTEGER PRIMARY KEY);")
}

// TestSchemaPlanNewWritesThePlanFileWithoutASaveFlag is the load-bearing
// behavior claim of this sub-verb, and the one nothing measured settles: Atlas
// registers neither --save nor --dry-run on `new`, and its documented purpose
// is to create the plan file, so it must write one unasked.
//
// The assertion is the FILE, not the exit code: `schema plan` with no save
// destination prints the plan document to stdout and exits 0, so an
// implementation that forgot to force saving would pass any exit-code check
// while producing nothing.
func TestSchemaPlanNewWritesThePlanFileWithoutASaveFlag(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newSubverbFixture(c.TB, "new-default")

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "new", fixture.args()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	matches, globErr := filepath.Glob(filepath.Join(dir, "*"+atlasschema.PlanFileSuffixHCL))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1, qt.Commentf("stdout=%q", stdout))
	// The default name is the Atlas UTC timestamp, as on `schema plan --save`.
	c.Assert(filepath.Base(matches[0]), qt.Matches, `\d{14}\.plan\.hcl`)
	c.Assert(stdout, qt.Contains, "Plan saved to file://")
	// The plan document must not also be dumped to stdout: that is the
	// no-destination branch of `schema plan`, and reaching it here would mean
	// the file was a side effect rather than the point.
	c.Assert(stdout, qt.Not(qt.Contains), "migration = <<-SQL")

	plan, planFormat, readErr := atlasschema.ReadPlanDocument(matches[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(planFormat, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, `CREATE TABLE "added"`)
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Safe)
}

// TestSchemaPlanNewDoesNotEmitInternalProvenance pins that development
// provenance stays out of operator-facing output.
func TestSchemaPlanNewDoesNotEmitInternalProvenance(t *testing.T) {
	c := qt.New(t)
	chdirToScratchC(c.TB)
	fixture := newSubverbFixture(c.TB, "new-note")

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "new", fixture.args()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Not(qt.Contains), "stokaro/ptah#")
}

// TestSchemaPlanNewHonorsOutputPathAndFormat proves --output still selects both
// the destination and the encoding, which is the half of `schema plan` that
// forcing --save on must not have disturbed.
func TestSchemaPlanNewHonorsOutputPathAndFormat(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newSubverbFixture(c.TB, "new-output")

	tests := []struct {
		name       string
		file       string
		wantFormat atlasschema.PlanFormat
	}{
		{name: "hcl", file: "chosen.plan.hcl", wantFormat: atlasschema.PlanFormatHCL},
		{name: "json", file: "chosen.plan.json", wantFormat: atlasschema.PlanFormatJSON},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(dir, test.file)

			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
				fixture.args("--output", path)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			_, planFormat, readErr := atlasschema.ReadPlanDocument(path)
			c.Assert(readErr, qt.IsNil)
			c.Assert(planFormat, qt.Equals, test.wantFormat)
		})
	}
}

// TestSchemaPlanNewNamingFlagsBehaveAsOnTheParent covers the three naming
// paths, including the one where both flags are given. The default-name case
// and the --name case are separated by asserting the FILE NAME, not just that
// some plan file exists: a `new` that ignored --name would still write a plan.
func TestSchemaPlanNewNamingFlagsBehaveAsOnTheParent(t *testing.T) {
	t.Run("name", func(t *testing.T) {
		c := qt.New(t)
		dir := chdirToScratchC(c.TB)
		fixture := newSubverbFixture(c.TB, "new-name")

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
			fixture.args("--name", "release-42")...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		_, statErr := os.Stat(filepath.Join(dir, "release-42.plan.hcl"))
		c.Assert(statErr, qt.IsNil)
	})

	t.Run("name_format", func(t *testing.T) {
		c := qt.New(t)
		dir := chdirToScratchC(c.TB)
		fixture := newSubverbFixture(c.TB, "new-name-format")

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
			fixture.args("--name-format", "plan_{{ slice .ToHash 0 6 }}")...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		matches, globErr := filepath.Glob(filepath.Join(dir, "plan_*.plan.hcl"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 1)
		c.Assert(filepath.Base(matches[0]), qt.Matches, `plan_.{6}\.plan\.hcl`)
	})

	t.Run("name_and_name_format_are_mutually_exclusive", func(t *testing.T) {
		c := qt.New(t)
		dir := chdirToScratchC(c.TB)
		fixture := newSubverbFixture(c.TB, "new-name-both")

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
			fixture.args("--name", "a", "--name-format", "b")...)

		c.Assert(err, qt.ErrorMatches,
			`if any flags in the group \[name name-format\] are set none of the others can be.*`,
			qt.Commentf("%s", out))
		assertNoPlanFileWritten(c.TB, dir)
	})

	t.Run("malformed_name_format_writes_nothing", func(t *testing.T) {
		c := qt.New(t)
		dir := chdirToScratchC(c.TB)
		fixture := newSubverbFixture(c.TB, "new-name-bad")

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
			fixture.args("--name-format", "{{ .Nope }")...)

		c.Assert(err, qt.IsNotNil)
		assertNoPlanFileWritten(c.TB, dir)
		c.Assert(out, qt.Not(qt.Contains), "Plan saved to")
	})
}

// TestSchemaPlanNewRefusesToOverwriteADefaultNamedPlan asserts the protected
// state: a second run with the same plan name must leave the FIRST plan's
// bytes in place. Asserting only the error would pass an implementation that
// truncated the reviewed plan and then complained.
func TestSchemaPlanNewRefusesToOverwriteADefaultNamedPlan(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newSubverbFixture(c.TB, "new-collision")
	path := filepath.Join(dir, "pinned.plan.hcl")

	first, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--name", "pinned")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	before, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	second, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--name", "pinned")...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(second, qt.Contains, "already exists; pass --name or --output")
	after, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(after), qt.Equals, string(before))
}

// TestSchemaPlanNewOnASyncedSchemaWritesNoPlan covers the branch where there is
// nothing to plan. `new` must not manufacture an empty plan file just because
// its documented job is to produce one — an empty plan applies cleanly and
// changes nothing, which reads as success.
func TestSchemaPlanNewOnASyncedSchemaWritesNoPlan(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newPlanFixture(c.TB, "new-synced",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`,
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new", fixture.args()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced, no changes to be made.")
	assertNoPlanFileWritten(c.TB, dir)
}

// TestSchemaPlanNewRefusesUnimplementedTransitionFlags proves the sub-verb
// refuses through the SAME table the parent uses, and names itself while doing
// it: a diagnostic that says `atlas schema plan` after the operator typed
// `atlas schema plan new` sends them to the wrong command's help.
func TestSchemaPlanNewRefusesUnimplementedTransitionFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "repo", args: []string{"--repo", "atlas://app"}, want: "accepts --repo, but schema repositories require a hosted registry"},
		{name: "format", args: []string{"--format", "{{ json . }}"}, want: "accepts --format, but Ptah does not implement"},
		{name: "lock_timeout", args: []string{"--lock-timeout", "10s"}, want: "accepts --lock-timeout, but Ptah does not implement"},
		{name: "include", args: []string{"--include", "public.*"}, want: "accepts --include, but Ptah only supports"},
		{name: "schema", args: []string{"--schema", "public"}, want: "accepts --schema, but Ptah only supports"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := chdirToScratchC(c.TB)
			fixture := newSubverbFixture(c.TB, "new-reject")

			out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
				fixture.args(test.args...)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "atlas schema plan new "+test.want)
			assertNoPlanFileWritten(c.TB, dir)
		})
	}
}

// TestSchemaPlanNewRejectsPositionalArguments pins the captured usage line
// `atlas schema plan new [flags]`, which has no positional — unlike the parent
// `atlas schema plan [flags] [name]`.
func TestSchemaPlanNewRejectsPositionalArguments(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newSubverbFixture(c.TB, "new-positional")

	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("some-name")...)

	c.Assert(err, qt.IsNotNil)
	assertNoPlanFileWritten(c.TB, dir)
	c.Assert(out, qt.Not(qt.Contains), "Plan saved to")
}

// TestSchemaPlanNewEditRoundTripsTheStatementsVerbatim covers the branch that
// cost the parent a defect: an editor session that changes nothing must produce
// the same document, comments included, because on a destructive plan the
// generated warning comment is the only in-artifact signal that the plan
// destroys data.
func TestSchemaPlanNewEditRoundTripsTheStatementsVerbatim(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratchC(c.TB)
	fixture := newPlanFixture(c.TB, "new-edit",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE drop_me (id INTEGER);",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)
	withoutEdit := filepath.Join(dir, "without.plan.hcl")
	withEdit := filepath.Join(dir, "with.plan.hcl")

	// The name is pinned because the default is a timestamp with one-second
	// resolution, so the two runs disagree whenever they straddle a second
	// boundary -- rare locally, routine on CI. This test is about the editor
	// round trip, not about naming.
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--name", "roundtrip", "--output", withoutEdit)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	installScriptEditor(t, "true")
	out, err = runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--name", "roundtrip", "--output", withEdit, "--edit")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	plain, readErr := os.ReadFile(withoutEdit)
	c.Assert(readErr, qt.IsNil)
	edited, readErr := os.ReadFile(withEdit)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(edited), qt.Equals, string(plain))
	c.Assert(string(edited), qt.Contains, "-- WARNING: This will delete all data!")
}
