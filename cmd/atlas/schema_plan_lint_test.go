package atlas_test

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// `atlas schema plan lint` has no oracle to copy: the pinned community binary
// refuses the whole `schema plan` path, so nothing about this verb's report,
// its ordering or its exit code is measurable there. Its flag set comes from
// the published Atlas CLI reference (see schema_plan_subverb_surface_test.go);
// everything below is the reading this tree implements and is what pins it.
//
// The reading, in one sentence: the verb reports and does not gate. A plan
// carrying a destructive change exits 0 with the change described, because a
// plan is a document an operator approves and a report that refuses on their
// behalf is one they cannot approve anything with. That is the claim
// TestSchemaPlanLintReportsFindingsWithoutFailing owns, and the opt-in
// threshold below is its control: without the control, a build that simply
// never noticed a finding would satisfy it.

// lintFixture is a live SQLite target, a desired-state file, and a plan file
// computed between them.
type lintFixture struct {
	planFixture
	planPath string
	dbPath   string
}

// newLintFixture seeds the target, writes the desired state, and produces a
// plan file for the transition using the command under test's sibling.
func newLintFixture(c *qt.C, name, seedSQL, desiredSQL string) lintFixture {
	c.Helper()
	fixture := newPlanFixture(c, name, seedSQL, desiredSQL)
	planPath := filepath.Join(fixture.dir, name+".plan.hcl")
	out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "new",
		fixture.args("--output", planPath)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return lintFixture{
		planFixture: fixture,
		planPath:    planPath,
		dbPath:      filepath.Join(fixture.dir, name+".db"),
	}
}

func (f lintFixture) lintArgs(extra ...string) []string {
	return f.args(append([]string{"--file", "file://" + f.planPath}, extra...)...)
}

// The two fixtures every test below picks from. `destructive` drops a whole
// table, which is the change the rules classify at error severity and the only
// input that can tell a reporting verb from a gating one; `additive` adds a
// nullable column, which no rule names.
const (
	lintSeedSQL       = "CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE drop_me (id INTEGER);"
	lintDestructiveTo = "CREATE TABLE keep_me (id INTEGER PRIMARY KEY);"
	lintAdditiveTo    = "CREATE TABLE keep_me (id INTEGER PRIMARY KEY, note TEXT);\n" +
		"CREATE TABLE drop_me (id INTEGER);"
)

func TestSchemaPlanLint_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		desiredSQL string
		want       []string
	}{
		{
			name:       "a plan with nothing to report",
			desiredSQL: lintAdditiveTo,
			want: []string{
				"Analyzing planned statements (1 in total):",
				"  -- no diagnostics found",
				"  -- 1 schema change",
			},
		},
		{
			name:       "a destructive plan reports the drop",
			desiredSQL: lintDestructiveTo,
			want: []string{
				"  -- destructive changes detected:",
				`Dropping table "drop_me" https://atlasgo.io/lint/analyzers#DS102`,
				"  -- suggested fix:",
				`-> Add a pre-migration check to ensure table "drop_me" is empty before dropping it`,
				"  -- 1 diagnostic",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newLintFixture(c, "happy", lintSeedSQL, test.desiredSQL)

			stdout, stderr, err := runSchemaPlanSubverbStreams(
				atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
			for _, want := range test.want {
				c.Assert(stdout, qt.Contains, want)
			}
		})
	}
}

// TestSchemaPlanLintReportsFindingsWithoutFailing is the load-bearing claim of
// the verb: the findings are the product and the exit code is not a verdict.
//
// The fixture is destructive on purpose. An additive plan would satisfy this
// assertion whether the verb gates or not, because there would be nothing to
// gate on — which is the shape of a test that cannot fail.
func TestSchemaPlanLintReportsFindingsWithoutFailing(t *testing.T) {
	c := qt.New(t)
	fixture := newLintFixture(c, "nogate", lintSeedSQL, lintDestructiveTo)

	stdout, stderr, err := runSchemaPlanSubverbStreams(
		atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "destructive changes detected")
	c.Assert(stdout, qt.Contains, "  -- 1 diagnostic")
}

// TestSchemaPlanLintStatesItsCoverageOnStderr is the duty the exit code above
// creates. Ptah's rules are its own and do not name every hazard a schema
// change can carry, so a reader who sees "no diagnostics found" and an exit
// code of 0 has to be told what that report is a statement about.
//
// Both rows matter, and the clean one matters more: it is the run that looks
// like an all-clear, so a caveat printed only alongside findings would leave
// exactly the wrong case uncaveated.
func TestSchemaPlanLintStatesItsCoverageOnStderr(t *testing.T) {
	tests := []struct {
		name       string
		desiredSQL string
	}{
		{name: "on a clean report", desiredSQL: lintAdditiveTo},
		{name: "on a report with findings", desiredSQL: lintDestructiveTo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newLintFixture(c, "coverage", lintSeedSQL, test.desiredSQL)

			stdout, stderr, err := runSchemaPlanSubverbStreams(
				atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
			c.Assert(stderr, qt.Contains, "do not name every hazard a schema")
			c.Assert(stderr, qt.Contains, "Findings do not change the exit code")
			c.Assert(stderr, qt.Contains, "PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR=1")
			// The report itself stays on stdout, so a pipeline capturing it
			// keeps a document of findings and nothing else.
			c.Assert(stdout, qt.Contains, "Analyzing planned statements")
			c.Assert(stderr, qt.Not(qt.Contains), "Analyzing planned statements")
		})
	}
}

// TestSchemaPlanLintFailOnErrorGate_HappyPath is the acceptance control for the
// refusal below: the variable, enabled, leaves a run with no error-severity
// finding at exit 0, and disabled it leaves even a destructive plan there.
// Without it, a build that failed nothing at all would pass the refusal test by
// never reaching the threshold.
//
// Where the severity boundary itself sits — error fails, warning does not — is
// pinned one level down, by TestHasErrorSeverity_HappyPath in
// go.5x5.cz/ptah/internal/planlint, which can put a warning-only statement in
// front of the predicate without needing a plan whose replay reaches --to.
func TestSchemaPlanLintFailOnErrorGate_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		desiredSQL string
	}{
		{name: "enabled with nothing to report", value: "1", desiredSQL: lintAdditiveTo},
		{name: "spelled true with nothing to report", value: "true", desiredSQL: lintAdditiveTo},
		{name: "disabled with a destructive plan", value: "0", desiredSQL: lintDestructiveTo},
		{name: "spelled false with a destructive plan", value: "false", desiredSQL: lintDestructiveTo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR", test.value)
			fixture := newLintFixture(c, "gateok", lintSeedSQL, test.desiredSQL)

			stdout, stderr, err := runSchemaPlanSubverbStreams(
				atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "Analyzing planned statements")
		})
	}
}

// TestSchemaPlanLintFailOnErrorGate_FailurePath pins the opt-in threshold: an
// error-severity finding exits 1, and the report is still printed, because a
// gate that swallowed the reason it failed would be unusable in the pipeline
// it was turned on for.
func TestSchemaPlanLintFailOnErrorGate_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR", "1")
	fixture := newLintFixture(c, "gatefail", lintSeedSQL, lintDestructiveTo)

	stdout, stderr, err := runSchemaPlanSubverbStreams(
		atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "destructive changes detected")
	c.Assert(stderr, qt.Contains, "PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR is set")
	c.Assert(stderr, qt.Not(qt.Contains), "Findings do not change the exit code")
}

// TestSchemaPlanLintRefusesAMalformedThreshold checks the value before any work
// is done. A typo in a CI environment file must not stay dormant on the runs
// that report nothing — those are the whole of a healthy pipeline, and the
// operator would learn about it on the one run that mattered.
func TestSchemaPlanLintRefusesAMalformedThreshold(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "exported empty", value: ""},
		{name: "unparsable", value: "yes-please"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR", test.value)
			fixture := newLintFixture(c, "badenv", lintSeedSQL, lintAdditiveTo)

			stdout, stderr, err := runSchemaPlanSubverbStreams(
				atlas.NewCompatCommand("atlas"), "lint", fixture.lintArgs()...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains,
				`invalid boolean value "`+test.value+`" for PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR`)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaPlanLintRefusesAPlanThatDoesNotDescribeTheTransition is the reason
// the verification runs first. A report about a plan that does not describe
// this transition would be an accurate report about a change nobody is going to
// make, and printing it alongside the refusal would leave the accurate-looking
// half in the log.
func TestSchemaPlanLintRefusesAPlanThatDoesNotDescribeTheTransition(t *testing.T) {
	c := qt.New(t)
	fixture := newLintFixture(c, "stale", lintSeedSQL, lintDestructiveTo)
	// The plan drops a table; this desired state keeps it and adds a column, so
	// replaying the plan cannot arrive there.
	otherPath := filepath.Join(fixture.dir, "other.sql")
	c.Assert(os.WriteFile(otherPath, []byte(lintAdditiveTo), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "lint",
		"--from", fixture.dbURL, "--to", "file://"+otherPath, "--file", "file://"+fixture.planPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "does not converge to the desired state")
	c.Assert(stdout, qt.Equals, "")
}

// TestSchemaPlanLintSuppressionMatchesTheCompatibilitySurface proves the
// analysis is the compatibility one rather than a private rule set: the
// `atlas:nolint` selector `ptah-compat migrate lint` honors silences the same
// finding here.
//
// The plan file is written by hand, with fingerprints Ptah cannot recompute, so
// the from-state check is skipped and the replay is the only gate — which is
// exactly the shape of an Atlas-written plan and the reason a hand-written one
// is a legitimate input rather than a test-only construction.
func TestSchemaPlanLintSuppressionMatchesTheCompatibilitySurface(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "nolint", lintSeedSQL, lintDestructiveTo)
	planPath := filepath.Join(fixture.dir, "hand.plan.hcl")
	// Fingerprints Ptah cannot recompute, in the Base64 SHA-256 shape the plan
	// reader requires. The values are opaque on purpose: what they stand for is
	// "a digest computed by something else", which is what makes the replay the
	// only from-state gate here.
	foreign := base64.StdEncoding.EncodeToString(make([]byte, sha256.Size))
	document := "plan \"hand\" {\n" +
		"  from      = \"" + foreign + "\"\n" +
		"  to        = \"" + foreign + "\"\n" +
		"  migration = <<-SQL\n" +
		"  -- atlas:nolint destructive\n" +
		"  DROP TABLE \"drop_me\";\n" +
		"  SQL\n" +
		"}\n"
	c.Assert(os.WriteFile(planPath, []byte(document), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "lint",
		fixture.args("--file", "file://"+planPath)...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "  -- no diagnostics found")
	c.Assert(stdout, qt.Not(qt.Contains), "destructive changes detected")
}

func TestSchemaPlanLintRequiredInputs_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing file",
			args: []string{"--from", "sqlite://x.db", "--to", "file://x.sql"},
			want: "--file is required: atlas schema plan lint reads an existing plan file",
		},
		{
			name: "missing from",
			args: []string{"--to", "file://x.sql", "--file", "file://x.plan.hcl"},
			want: "--from is required",
		},
		{
			name: "missing to",
			args: []string{"--from", "sqlite://x.db", "--file", "file://x.plan.hcl"},
			want: "--to is required",
		},
		{
			name: "a registry plan URL",
			args: []string{
				"--from", "sqlite://x.db", "--to", "file://x.sql",
				"--file", "atlas://app/plans/one",
			},
			want: "Ptah has no plan registry",
		},
		{
			name: "--format has no plan output contract to render",
			args: []string{
				"--from", "sqlite://x.db", "--to", "file://x.sql",
				"--file", "file://x.plan.hcl", "--format", "{{ json . }}",
			},
			want: "atlas schema plan lint accepts --format, but Ptah does not implement --format for schema plan yet",
		},
		{
			name: "--repo is registry work",
			args: []string{
				"--from", "sqlite://x.db", "--to", "file://x.sql",
				"--file", "file://x.plan.hcl", "--repo", "atlas://app",
			},
			want: "schema repositories require a hosted registry",
		},
		{
			name: "--exclude would verify a different transition",
			args: []string{
				"--from", "sqlite://x.db", "--to", "file://x.sql",
				"--file", "file://x.plan.hcl", "--exclude", "tmp_*",
			},
			want: "a plan file records the exclude patterns it was computed with",
		},
		{
			name: "--lock-timeout is unimplemented",
			args: []string{
				"--from", "sqlite://x.db", "--to", "file://x.sql",
				"--file", "file://x.plan.hcl", "--lock-timeout", "5s",
			},
			want: "Ptah does not implement database lock waiting yet",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chdirToScratchC(c)

			stdout, stderr, err := runSchemaPlanSubverbStreams(
				atlas.NewCompatCommand("atlas"), "lint", test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(stderr, qt.Contains, test.want)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// TestSchemaPlanLintLeavesTheTargetDatabaseUnchanged asserts the protected state
// rather than the proxy. `lint` verifies the plan by replaying its SQL for real
// — on a dev database, which is reset destructively first. If that replay ever
// reached the target, a report would still be printed and exit 0 returned by a
// command whose whole contract is that it inspects and changes nothing.
//
// The two subtests are not variations on one theme. The first is a control:
// with no --dev-url there is an ephemeral dev database and nothing could have
// touched the target, so it can only ever pass. The second carries the weight —
// it points --dev-url AT the target, the single input where the reset would
// destroy it. Only the second can fail, and it fails the moment `lint` stops
// handing the target URL to the shared verification.
func TestSchemaPlanLintLeavesTheTargetDatabaseUnchanged(t *testing.T) {
	newSeededFixture := func(tb testing.TB, name string) lintFixture {
		c := qt.New(tb)
		c.Helper()
		fixture := newLintFixture(c, name, lintSeedSQL, lintDestructiveTo)
		execOnTarget(c, fixture.dbURL, `INSERT INTO keep_me (id) VALUES (1), (2), (3);`)
		return fixture
	}

	t.Run("control_no_dev_url_cannot_touch_the_target", func(t *testing.T) {
		c := qt.New(t)
		fixture := newSeededFixture(c, "lint-readonly")
		before := readTargetSchema(c, fixture.dbURL)

		stdout, stderr, err := runSchemaPlanSubverbStreams(atlas.NewCompatCommand("atlas"), "lint",
			fixture.lintArgs()...)

		c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
		after := readTargetSchema(c, fixture.dbURL)
		c.Assert(tableNames(after), qt.DeepEquals, tableNames(before))
		c.Assert(tableNames(after), qt.DeepEquals, []string{"drop_me", "keep_me"})
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})

	t.Run("dev_url_pointing_at_the_target_is_refused", func(t *testing.T) {
		c := qt.New(t)
		fixture := newSeededFixture(c, "lint-devurl-is-target")
		before := readTargetSchema(c, fixture.dbURL)

		out, err := runSchemaPlanSubverb(atlas.NewCompatCommand("atlas"), "lint",
			fixture.lintArgs("--dev-url", fixture.dbURL)...)

		c.Assert(err, qt.IsNotNil)
		c.Assert(out, qt.Contains, "--dev-url must not point at the target database")
		// The protected state, not the message: the refusal is worthless if the
		// reset already ran. `drop_me` is the table the plan drops — its absence
		// would prove the plan was applied to the target.
		after := readTargetSchema(c, fixture.dbURL)
		c.Assert(tableNames(after), qt.DeepEquals, tableNames(before))
		c.Assert(tableNames(after), qt.DeepEquals, []string{"drop_me", "keep_me"})
		c.Assert(countRows(c, fixture.dbPath, "keep_me"), qt.Equals, 3)
	})
}
