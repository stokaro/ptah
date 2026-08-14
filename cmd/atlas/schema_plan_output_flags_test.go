package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/safety"
)

// Measured against the pinned Atlas CE v1.2.0 binary on 2026-08-02: `schema
// plan` is a registered community-abort stub that declares no command-specific
// flags at all. Every one of --format, --name-format, --directive, --edit and
// --skip-lint answers `unknown flag`, byte-identical to the nonsense controls
// --name-formxxxx and --totally-bogus-flag, and so does --dev-url, a flag CE
// certainly knows on sibling verbs. Only the inherited --env/-c/--var parse,
// and they reach the abort. CE therefore constrains none of this file's
// behavior; the flags exist on Atlas's non-community surface only.

// installScriptEditor installs an $EDITOR whose shell body runs with the file
// being edited as "$1", so a test can rewrite, empty, or fail on the planned
// SQL. CI is Linux-only, so a /bin/sh script is safe here (same precedent as
// installAppendEditor).
func installScriptEditor(t *testing.T, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan-editor.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil { //nolint:gosec // test editor script must be executable
		t.Fatal(err)
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", path)
}

// runSchemaPlanStreams executes `schema plan` with stdout and stderr captured
// separately, so a test can assert that nothing was reported on stderr. The
// shared runSchemaPlan helper merges them, which is right for reading a failure
// but cannot distinguish "printed nothing" from "printed to the other stream".
func runSchemaPlanStreams(root *cobra.Command, args ...string) (stdout, stderr string, err error) {
	var out, errBuf bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errBuf)
	root.SetArgs(append([]string{"schema", "plan"}, args...))
	err = root.Execute()
	return out.String(), errBuf.String(), err
}

// planFixture is a live SQLite target plus a desired-state schema file whose
// difference is exactly one CREATE TABLE.
type planFixture struct {
	dir        string
	dbURL      string
	schemaURL  string
	outputPath string
}

func newPlanFixture(c *qt.C, name, seedSQL, desiredSQL string) planFixture {
	c.Helper()
	dir := c.TB.TempDir()
	dbPath := filepath.Join(dir, name+".db")
	schemaPath := filepath.Join(dir, "schema.sql")
	if seedSQL != "" {
		seedSQLiteSchema(c, dbPath, seedSQL)
	}
	c.Assert(os.WriteFile(schemaPath, []byte(desiredSQL), 0o600), qt.IsNil)
	return planFixture{
		dir:        dir,
		dbURL:      sqliteURLFromPath(dbPath),
		schemaURL:  "file://" + schemaPath,
		outputPath: filepath.Join(dir, name+".plan.json"),
	}
}

func (f planFixture) args(extra ...string) []string {
	return append([]string{"--from", f.dbURL, "--to", f.schemaURL}, extra...)
}

// assertNoPlanFileWritten proves a refusal protected the filesystem: the thing
// a bad plan name or a failed edit must never produce is a plan file, and an
// error return alone does not prove one was not written first.
//
// The working directory is checked alongside dir because `--save` without
// `--output` writes there, so a refusal that leaked would leave its artifact
// outside any fixture directory.
func assertNoPlanFileWritten(c *qt.C, dir string) {
	c.Helper()
	for _, base := range []string{dir, "."} {
		for _, pattern := range []string{"*.plan.json", "*.plan.hcl"} {
			matches, err := filepath.Glob(filepath.Join(base, pattern))
			c.Assert(err, qt.IsNil)
			c.Assert(matches, qt.HasLen, 0, qt.Commentf("%s/%s", base, pattern))
		}
	}
}

// chdirToScratch points the working directory at a fresh temporary directory,
// so a test whose `--save` unexpectedly succeeds writes its default-named plan
// file there instead of into the package source tree.
func chdirToScratch(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	return dir
}

// ---------------------------------------------------------------- --skip-lint

func TestSchemaPlanSkipLintIsANoOp(t *testing.T) {
	c := qt.New(t)
	// The fixture is DESTRUCTIVE on purpose. A lint step reports; it does not
	// rewrite the plan, so an additive fixture gives any plausible linter
	// nothing to say and the assertion passes whether the flag is honored,
	// ignored, or absent. Dropping a table is the input a linter would speak
	// up about, which is what makes this test able to fail.
	fixture := newPlanFixture(c, "skiplint",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE drop_me (id INTEGER);",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)
	// Both runs write to the same path so the "Plan saved to" line is
	// identical and the whole reported output can be compared verbatim.
	planPath := filepath.Join(fixture.dir, "p.plan.json")

	stdoutWithout, stderrWithout, errWithout := runSchemaPlanStreams(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath)...)
	c.Assert(errWithout, qt.IsNil, qt.Commentf("%s%s", stdoutWithout, stderrWithout))
	withoutDocument, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(planPath), qt.IsNil)

	stdoutWith, stderrWith, errWith := runSchemaPlanStreams(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "--skip-lint")...)

	// Two claims, and they need different assertions.
	//
	// "The flag changes nothing" is the equality: whole reported output and
	// saved document, both byte for byte. A document-only comparison could not
	// tell a linter that honors the flag from one that ignores it, because a
	// linter reports rather than rewriting the plan.
	//
	// "There is no lint step at all" is the empty stderr. Equality alone
	// cannot carry that one — a linter that ignores the flag emits in BOTH
	// runs and leaves them equal. Diagnostics belong on stderr, so a
	// successful plan over a destructive fixture producing nothing there is
	// the evidence that no such step exists.
	//
	// The native JSON format is used deliberately — its default plan name is
	// fingerprint-derived and so reproducible across runs, whereas the
	// .plan.hcl name is a one-second-granularity timestamp.
	c.Assert(errWith, qt.IsNil, qt.Commentf("%s%s", stdoutWith, stderrWith))
	c.Assert(stdoutWith, qt.Equals, stdoutWithout)
	c.Assert(stderrWith, qt.Equals, stderrWithout)
	c.Assert(stderrWithout, qt.Equals, "")
	c.Assert(stderrWith, qt.Equals, "")
	withDocument, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(withDocument), qt.Equals, string(withoutDocument))
}

func TestSchemaPlanSkipLintCombinesWithEveryOutputMode(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args []string
	}{
		{name: "dry_run", args: []string{"--dry-run", "--skip-lint"}},
		{name: "stdout", args: []string{"--skip-lint"}},
		{name: "save", args: []string{"--save", "--skip-lint"}},
		{name: "with_name_format", args: []string{"--save", "--skip-lint", "--name-format", "combo_name"}},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			chdirToScratch(c.TB.(*testing.T))
			fixture := newPlanFixture(c, "combo", "", `CREATE TABLE combo_users (id INTEGER PRIMARY KEY);`)

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args(tt.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
		})
	}
}

// ------------------------------------------------------------- --name-format

func TestSchemaPlanNameFormatRendersPlanName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	fixture := newPlanFixture(c, "nameformat", "", `CREATE TABLE nf_users (id INTEGER PRIMARY KEY);`)
	referencePath := filepath.Join(dir, "reference.plan.json")
	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--output", referencePath)...)
	c.Assert(err, qt.IsNil)
	reference, err := atlasschema.ReadPlanFile(referencePath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(referencePath), qt.IsNil)

	templateData, err := atlasreport.NewSchemaPlanName(reference.FromFingerprint, reference.ToFingerprint)
	c.Assert(err, qt.IsNil)
	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--name-format", "plan_{{ slice .ToHash 0 12 }}")...)

	// The template sees this plan's fingerprints in Atlas's untagged Base64
	// representation. Asserting the exact expected name — not merely a shape —
	// separates "the template ran" from "the template ran on the right value".
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	want := "plan_" + templateData.ToHash[:12]
	c.Assert(out, qt.Contains, "Plan saved to file://"+want+".plan.hcl")
	plan, format, err := atlasschema.ReadPlanDocument(filepath.Join(dir, want+".plan.hcl"))
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatHCL)
	c.Assert(plan.Name, qt.Equals, want)
}

func TestSchemaPlanNameFormatDistinguishesFromHashAndToHash(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	// The seeded table makes the current schema non-empty, so the two
	// fingerprints differ. Without that, .FromHash and .ToHash would coincide
	// and a template wired to the wrong one would still pass.
	fixture := newPlanFixture(c, "hashes",
		`CREATE TABLE hashes_keep (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE hashes_keep (id INTEGER PRIMARY KEY);\nCREATE TABLE hashes_add (id INTEGER PRIMARY KEY);\n")
	referencePath := filepath.Join(dir, "reference.plan.json")
	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--output", referencePath)...)
	c.Assert(err, qt.IsNil)
	reference, err := atlasschema.ReadPlanFile(referencePath)
	c.Assert(err, qt.IsNil)
	c.Assert(reference.FromFingerprint, qt.Not(qt.Equals), reference.ToFingerprint)
	templateData, err := atlasreport.NewSchemaPlanName(reference.FromFingerprint, reference.ToFingerprint)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name     string
		template string
		want     string
	}{
		{name: "to_hash", template: "to_{{ slice .ToHash 0 12 }}", want: "to_" + templateData.ToHash[:12]},
		{name: "from_hash", template: "from_{{ slice .FromHash 0 12 }}", want: "from_" + templateData.FromHash[:12]},
		{
			name:     "both",
			template: "{{ slice .FromHash 0 4 }}_{{ slice .ToHash 0 4 }}",
			want:     templateData.FromHash[:4] + "_" + templateData.ToHash[:4],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			outPath := filepath.Join(c.TB.TempDir(), "named.plan.json")

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args("--output", outPath, "--name-format", tt.template)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			plan, err := atlasschema.ReadPlanFile(outPath)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Name, qt.Equals, tt.want)
		})
	}
}

func TestSchemaPlanNameFormatOverridesTheJSONDefaultName(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "jsonname", "", `CREATE TABLE json_named (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--name-format", "templated_name")...)

	// The native JSON format defaults to a fingerprint-derived plan_<hex>
	// name; --name-format must win over it, not only over the .plan.hcl
	// timestamp default.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Equals, "templated_name")
}

func TestSchemaPlanNameFormatTrimsSurroundingWhitespace(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "trimmed", "", `CREATE TABLE trimmed_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--name-format", "\n  spaced_name  \n")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Equals, "spaced_name")
}

func TestSchemaPlanNameFormatAllowsPathSeparatorWithExplicitOutput(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		suffix string
	}{
		{name: "native json", suffix: ".plan.json"},
		{name: "atlas hcl", suffix: ".plan.hcl"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			fixture := newPlanFixture(c, "explicit-name-path", "", `CREATE TABLE explicit_name_users (id INTEGER PRIMARY KEY);`)
			outputPath := filepath.Join(fixture.dir, "explicit"+test.suffix)

			out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args("--output", outputPath, "--name-format", "nested/plan")...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			plan, _, err := atlasschema.ReadPlanDocument(outputPath)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Name, qt.Equals, "nested/plan")
		})
	}
}

func TestSchemaPlanNameFormatLastValueWins(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "repeated", "", `CREATE TABLE repeated_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--name-format", "first", "--name-format", "second")...)

	// A repeated string flag takes its last value; pinning it stops a future
	// change to a repeatable flag type from silently reordering the result.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Equals, "second")
}

func TestSchemaPlanNameFormatIsMutuallyExclusiveWithName(t *testing.T) {
	c := qt.New(t)
	chdirToScratch(t)
	fixture := newPlanFixture(c, "exclusive", "", `CREATE TABLE exclusive_users (id INTEGER PRIMARY KEY);`)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--name", "literal", "--name-format", "templated")...)

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[name name-format\] are set none of the others can be.*`)
	assertNoPlanFileWritten(c, fixture.dir)
}

func TestSchemaPlanNameFormatRejectionsWriteNothing(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name:     "empty",
			template: "",
			want:     `--name-format must not be empty`,
		},
		{
			name:     "malformed_template",
			template: "plan_{{ slice .ToHash 0 8",
			want:     `parse --name-format template: .*`,
		},
		{
			name:     "unknown_field",
			template: "plan_{{ .NoSuchField }}",
			want:     `execute --name-format template: .*NoSuchField.*`,
		},
		{
			name:     "renders_empty",
			template: `{{ "" }}`,
			want:     `--name-format: the plan name is empty`,
		},
		{
			name:     "renders_whitespace_only",
			template: "   ",
			want:     `--name-format: the plan name is empty`,
		},
		{
			name:     "renders_path_separator",
			template: "nested/plan",
			want:     `--name-format: the plan name "nested/plan" contains a path separator; use --output to choose the plan file location`,
		},
		{
			name:     "renders_control_character",
			template: "plan{{ printf \"\\n\" }}name",
			want:     `--name-format: the plan name .* contains a control character`,
		},
		{
			name:     "renders_current_directory",
			template: ".",
			want:     `--name-format: the plan name "\." is a directory reference, not a name`,
		},
		{
			name:     "renders_parent_directory",
			template: "..",
			want:     `--name-format: the plan name "\.\." is a directory reference, not a name`,
		},
		{
			name:     "renders_windows_wildcard",
			template: "plan*name",
			want:     `--name-format: the plan name "plan\*name" contains one of :\*\?"<>\|, which cannot appear in a file name on Windows`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			chdirToScratch(c.TB.(*testing.T))
			fixture := newPlanFixture(c, "reject", "", `CREATE TABLE reject_users (id INTEGER PRIMARY KEY);`)

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args("--save", "--name-format", tt.template)...)

			// Each case asserts the protected state, not only the message: a
			// refusal that arrives after the plan file was written would still
			// satisfy an error-text assertion.
			c.Assert(err, qt.ErrorMatches, tt.want)
			assertNoPlanFileWritten(c, fixture.dir)
		})
	}
}

func TestSchemaPlanNameFormatCannotCorruptThePlanBlockLabel(t *testing.T) {
	c := qt.New(t)

	// Each case names the layer that refuses it, so the test cannot pass
	// because planning failed for some unrelated reason. Two are caught by the
	// name rules before the writer ever sees them — a backslash is a path
	// separator, a double quote is illegal in a Windows file name — and the
	// HCL interpolation sequences are legal in a file name, so only the writer
	// can refuse those.
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{
			name:     "double_quote",
			template: `plan"name`,
			want:     `--name-format: the plan name "plan\\"name" contains one of :\*\?"<>\|, which cannot appear in a file name on Windows`,
		},
		{
			name:     "backslash",
			template: `plan\name`,
			want:     `--name-format: the plan name "plan\\\\name" contains a path separator.*`,
		},
		{
			name:     "hcl_interpolation",
			template: "plan${x}name",
			want:     `plan name "plan\$\{x\}name" contains characters that cannot be written verbatim into an Atlas \.plan\.hcl quoted string`,
		},
		{
			name:     "hcl_directive",
			template: "plan%{x}name",
			want:     `plan name "plan%\{x\}name" contains characters that cannot be written verbatim into an Atlas \.plan\.hcl quoted string`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			chdirToScratch(c.TB.(*testing.T))
			fixture := newPlanFixture(c, "label", "", `CREATE TABLE label_users (id INTEGER PRIMARY KEY);`)

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args("--save", "--name-format", tt.template)...)

			// A templated name reaches the `plan "<label>"` block label, so a
			// quote or an HCL interpolation sequence in it would produce a
			// document that no longer parses as the plan it claims to be. The
			// writer refuses rather than escaping, and nothing lands on disk.
			c.Assert(err, qt.ErrorMatches, tt.want)
			assertNoPlanFileWritten(c, fixture.dir)
		})
	}
}

func TestSchemaPlanNameFormatIsParsedBeforeAnyDatabaseWork(t *testing.T) {
	c := qt.New(t)
	chdirToScratch(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE u (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	// A SQLite path under a directory that does not exist cannot be opened,
	// so reaching the connection at all produces a connect error instead.
	unreachable := sqliteURLFromPath(filepath.Join(dir, "no-such-directory", "target.db"))

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		"--from", unreachable,
		"--to", "file://"+schemaPath,
		"--save", "--name-format", "plan_{{ slice .ToHash 0 8",
	)

	// The template is parsed before the target is contacted, so a typo is
	// reported as a typo. Without the pre-flight parse the operator sees a
	// connection failure and never learns the template is malformed. This is
	// the only observable difference — neither ordering writes a file — so it
	// is the input that holds the rule.
	c.Assert(err, qt.ErrorMatches, `parse --name-format template: .*`)
	assertNoPlanFileWritten(c, dir)
}

func TestSchemaPlanNameFormatCollisionRecommendsAReachableFlag(t *testing.T) {
	c := qt.New(t)
	dir := chdirToScratch(t)
	fixture := newPlanFixture(c, "collide", "", `CREATE TABLE collide_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--name-format", "collides")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	_, statErr := os.Stat(filepath.Join(dir, "collides.plan.hcl"))
	c.Assert(statErr, qt.IsNil)

	_, err = runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--name-format", "collides")...)

	// --name and --name-format are mutually exclusive, so telling someone who
	// used --name-format to "pass --name" sends them at a combination this
	// command refuses. The remediation names the flag they actually have.
	c.Assert(err, qt.ErrorMatches, `plan file collides\.plan\.hcl already exists; pass --name-format or --output to choose a distinct plan file`)
}

func TestSchemaPlanNameCollisionRecommendsName(t *testing.T) {
	c := qt.New(t)
	chdirToScratch(t)
	fixture := newPlanFixture(c, "collidename", "", `CREATE TABLE collidename_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--save", "--name", "taken")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	_, err = runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--save", "--name", "taken")...)

	c.Assert(err, qt.ErrorMatches, `plan file taken\.plan\.hcl already exists; pass --name or --output to choose a distinct plan file`)
}

func TestSchemaPlanNameFormatAcceptsAtlasTemplateHelpers(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "helpers", "", `CREATE TABLE helper_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--name-format", "plan_{{ upper (slice .ToHash 0 8) }}")...)

	// The helper set is shared with the eight verbs that implement --format,
	// so an Atlas pipeline's template keeps working when it names a plan.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Matches, `plan_[A-Z0-9+/]{8}`)
}

func TestSchemaPlanNameFormatDryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	chdirToScratch(t)
	fixture := newPlanFixture(c, "nfdry", "", `CREATE TABLE nfdry_users (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--dry-run", "--name-format", "dry_named")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan := parsePlanDocumentOutput(c, out)
	c.Assert(plan.Name, qt.Equals, "dry_named")
	assertNoPlanFileWritten(c, fixture.dir)
}

func TestSchemaPlanLocalFlagsResetAcrossSuccessfulRootReuse(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")
	first := newPlanFixture(c, "reuse-first", "", `CREATE TABLE first_users (id INTEGER PRIMARY KEY);`)
	second := newPlanFixture(c, "reuse-second", "", `CREATE TABLE second_users (id INTEGER PRIMARY KEY);`)
	firstPath := filepath.Join(first.dir, "first.plan.json")
	secondPath := filepath.Join(second.dir, "second.plan.json")

	_, err := runSchemaPlan(root, first.args("--output", firstPath, "--name-format", "first_name")...)
	c.Assert(err, qt.IsNil)
	_, err = runSchemaPlan(root, second.args("--output", secondPath)...)

	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.ReadPlanFile(secondPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
}

func TestSchemaPlanLocalFlagsResetAcrossFailedRootReuse(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")
	first := newPlanFixture(c, "reuse-failed-first", "", `CREATE TABLE first_users (id INTEGER PRIMARY KEY);`)
	second := newPlanFixture(c, "reuse-failed-second", "", `CREATE TABLE second_users (id INTEGER PRIMARY KEY);`)
	secondPath := filepath.Join(second.dir, "second.plan.json")

	_, err := runSchemaPlan(root, first.args("--dry-run", "--name-format", "{{ .Missing }}")...)
	c.Assert(err, qt.ErrorMatches, `execute --name-format template: .*Missing.*`)
	_, err = runSchemaPlan(root, second.args("--output", secondPath)...)

	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.ReadPlanFile(secondPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
}

func TestSchemaPlanLocalFlagsResetAcrossParseFailureRootReuse(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")
	fixture := newPlanFixture(c, "reuse-parse", "", `CREATE TABLE parse_users (id INTEGER PRIMARY KEY);`)
	outputPath := filepath.Join(fixture.dir, "after-parse.plan.json")

	_, err := runSchemaPlan(root, "--edit=not-a-bool")
	c.Assert(err, qt.ErrorMatches, `invalid argument "not-a-bool" for "--edit" flag: .*`)
	_, err = runSchemaPlan(root, fixture.args("--output", outputPath)...)

	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.ReadPlanFile(outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
}

func TestSchemaPlanLocalFlagsResetAcrossHelpRootReuse(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")
	fixture := newPlanFixture(c, "reuse-help", "", `CREATE TABLE help_users (id INTEGER PRIMARY KEY);`)
	outputPath := filepath.Join(fixture.dir, "after-help.plan.json")

	_, err := runSchemaPlan(root, "--name-format", "sticky_name", "--help")
	c.Assert(err, qt.IsNil)
	_, err = runSchemaPlan(root, fixture.args("--output", outputPath)...)

	c.Assert(err, qt.IsNil)
	plan, err := atlasschema.ReadPlanFile(outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Name, qt.Matches, `plan_[0-9a-f]{12}`)
}

// -------------------------------------------------------------------- --edit

func TestSchemaPlanEditSavesTheEditedSQL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	installScriptEditor(t, `printf 'CREATE TABLE edited_table (id INTEGER PRIMARY KEY);\n' > "$1"`)
	fixture := newPlanFixture(c, "edited", "", `CREATE TABLE original_table (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--save", "--edit")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	matches, globErr := filepath.Glob(filepath.Join(dir, "*.plan.hcl"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	plan, _, readErr := atlasschema.ReadPlanDocument(matches[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "edited_table")
	c.Assert(plan.Statements[0].SQL, qt.Not(qt.Contains), "original_table")
}

func TestSchemaPlanEditThatChangesNothingProducesTheSamePlan(t *testing.T) {
	c := qt.New(t)
	// The commonest editor session: open, read, quit without typing. /bin/true
	// leaves the file byte-for-byte as written.
	fixture := newPlanFixture(c, "noopedit",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE victim (id INTEGER);",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)
	planPath := filepath.Join(fixture.dir, "p.plan.hcl")

	outWithout, errWithout := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "--name", "fixed")...)
	c.Assert(errWithout, qt.IsNil, qt.Commentf("%s", outWithout))
	withoutDocument, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(planPath), qt.IsNil)

	installScriptEditor(t, `exit 0`)
	outWith, errWith := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", planPath, "--name", "fixed", "--edit")...)

	// An editor that changes nothing must change nothing. The .plan.hcl shape
	// has no severity field, so the generated "-- WARNING: This will delete all
	// data" comment is the ONLY in-artifact signal that this plan is
	// destructive; a round-trip that strips comments would silently turn a plan
	// that warns into one that does not, and a reviewer would see a bare DROP.
	c.Assert(errWith, qt.IsNil, qt.Commentf("%s", outWith))
	withDocument, err := os.ReadFile(planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(withDocument), qt.Equals, string(withoutDocument))
	c.Assert(string(withDocument), qt.Contains, "WARNING")
	c.Assert(string(withDocument), qt.Contains, "DROP TABLE")
}

func TestSchemaPlanEditPreservesCommentsOnEditedStatements(t *testing.T) {
	c := qt.New(t)
	installScriptEditor(t, `printf -- '-- reviewed by hand\nDROP TABLE victim;\n' > "$1"`)
	fixture := newPlanFixture(c, "editcomment",
		"CREATE TABLE keep_me (id INTEGER PRIMARY KEY);\nCREATE TABLE victim (id INTEGER);",
		`CREATE TABLE keep_me (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--edit")...)

	// A comment the operator writes is part of the artifact a reviewer reads,
	// so it is kept verbatim. The severity is still classified from the
	// executable body, not from the comment.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "-- reviewed by hand")
	c.Assert(plan.Statements[0].SQL, qt.Contains, "DROP TABLE victim")
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(plan.Destructive, qt.IsTrue)
}

func TestSchemaPlanEditIsNotOpenedWhenNamingFailsFirst(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "name_format_execute_failure",
			args: []string{"--save", "--name-format", "{{ .NoSuchField }}"},
			want: `execute --name-format template: .*NoSuchField.*`,
		},
		{
			name: "name_format_renders_unusable_name",
			args: []string{"--save", "--name-format", "nested/plan"},
			want: `--name-format: the plan name "nested/plan" contains a path separator.*`,
		},
		{
			name: "exclude_cannot_be_written_as_hcl",
			args: []string{"--save", "--exclude", "zzz_unrelated"},
			want: `the Atlas \.plan\.hcl format has no field for exclude patterns.*`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			t := c.TB.(*testing.T)
			dir := chdirToScratch(t)
			marker := filepath.Join(dir, "editor-ran")
			installScriptEditor(t, `touch "`+marker+`"`)
			fixture := newPlanFixture(c, "preflight", "", `CREATE TABLE preflight_users (id INTEGER PRIMARY KEY);`)

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
				fixture.args(append(tt.args, "--edit")...)...)

			// Every refusal that does not depend on the statement text belongs
			// before the editor. An edit is operator work, and the temporary
			// file this command edits is deleted on the way out, so a failure
			// after the editor loses that work for a reason that was knowable
			// beforehand. The marker is the assertion: an error message alone
			// cannot tell you whether the editor was opened.
			c.Assert(err, qt.ErrorMatches, tt.want)
			_, statErr := os.Stat(marker)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("editor must not open before a decidable refusal"))
			assertNoPlanFileWritten(c, fixture.dir)
		})
	}
}

func TestSchemaPlanEditRederivesSeverityAndDestructiveMarker(t *testing.T) {
	c := qt.New(t)
	installScriptEditor(t, `printf 'DROP TABLE victim;\n' > "$1"`)
	fixture := newPlanFixture(c, "rederive",
		`CREATE TABLE victim (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE victim (id INTEGER PRIMARY KEY);\nCREATE TABLE additive (id INTEGER PRIMARY KEY);\n")

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--edit")...)

	// The pre-edit plan was purely additive (destructive=false, Safe). The
	// edit replaces it with a DROP, so the saved plan must not keep the old
	// verdict. The native JSON format is essential to this test: the Atlas
	// .plan.hcl format records no severity, so its READER re-derives both
	// fields on load and would report the right answer even if the writer had
	// saved the stale one. Only the JSON document stores what was written.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "DROP TABLE victim")
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(plan.Statements[0].Reason, qt.Not(qt.Equals), "")
	c.Assert(plan.Destructive, qt.IsTrue)
}

func TestSchemaPlanEditKeepsTheSourceFingerprint(t *testing.T) {
	c := qt.New(t)
	fixture := newPlanFixture(c, "fingerprint",
		`CREATE TABLE fp_users (id INTEGER PRIMARY KEY);`,
		"CREATE TABLE fp_users (id INTEGER PRIMARY KEY);\nCREATE TABLE fp_orders (id INTEGER PRIMARY KEY);\n")
	referencePath := filepath.Join(fixture.dir, "reference.plan.json")
	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--output", referencePath)...)
	c.Assert(err, qt.IsNil)
	reference, err := atlasschema.ReadPlanFile(referencePath)
	c.Assert(err, qt.IsNil)
	installScriptEditor(t, `printf 'CREATE TABLE fp_other (id INTEGER PRIMARY KEY);\n' > "$1"`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--edit")...)

	// An edit rewrites the statements, never the fingerprints: `from` still
	// describes the live source database the plan is bound to, so apply-time
	// staleness detection keeps working on an edited plan.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan, err := atlasschema.ReadPlanFile(fixture.outputPath)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.FromFingerprint, qt.Equals, reference.FromFingerprint)
	c.Assert(plan.ToFingerprint, qt.Equals, reference.ToFingerprint)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "fp_other")
}

func TestSchemaPlanEditFailuresWriteNothing(t *testing.T) {
	c := qt.New(t)

	// installEditor is a closure per case rather than a body string plus a
	// flag, so the "no editor at all" case needs no conditional in the test.
	withBody := func(body string) func(*testing.T) {
		return func(t *testing.T) { installScriptEditor(t, body) }
	}
	tests := []struct {
		name          string
		installEditor func(*testing.T)
		want          string
	}{
		{
			name:          "emptied_file",
			installEditor: withBody(`: > "$1"`),
			want:          `the edited plan contains no SQL statement; nothing was saved`,
		},
		{
			name:          "comments_only",
			installEditor: withBody(`printf -- '-- everything removed\n' > "$1"`),
			want:          `the edited plan contains no SQL statement; nothing was saved`,
		},
		{
			name:          "editor_exits_non_zero",
			installEditor: withBody(`exit 3`),
			want:          `.*editor .* failed: .*`,
		},
		{
			name: "no_editor_configured",
			installEditor: func(t *testing.T) {
				t.Setenv("VISUAL", "")
				t.Setenv("EDITOR", "")
			},
			want: `.*no editor configured: set \$EDITOR or \$VISUAL`,
		},
		{
			name:          "heredoc_hostile_sql_for_the_hcl_format",
			installEditor: withBody(`printf 'CREATE TABLE t (c TEXT DEFAULT ${nope});\n' > "$1"`),
			want:          `.*interpolation.*`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			t := c.TB.(*testing.T)
			dir := t.TempDir()
			t.Chdir(dir)
			tt.installEditor(t)
			fixture := newPlanFixture(c, "editfail", "", `CREATE TABLE editfail_users (id INTEGER PRIMARY KEY);`)

			_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--save", "--edit")...)

			// The protected state is the absence of a plan file. A plan saved
			// before the refusal would still satisfy the message assertion,
			// and a saved-but-rejected plan is exactly the artifact an
			// operator would later apply.
			c.Assert(err, qt.ErrorMatches, tt.want)
			assertNoPlanFileWritten(c, dir)
			assertNoPlanFileWritten(c, fixture.dir)
		})
	}
}

func TestSchemaPlanEditRejectsInvalidUTF8BeforeWritingJSON(t *testing.T) {
	c := qt.New(t)
	installScriptEditor(t, `printf '\377' > "$1"`)
	fixture := newPlanFixture(c, "invalid-utf8", "", `CREATE TABLE utf8_users (id INTEGER PRIMARY KEY);`)

	_, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--output", fixture.outputPath, "--edit")...)

	c.Assert(err, qt.ErrorMatches, `edited schema plan SQL is not valid UTF-8`)
	_, statErr := os.Stat(fixture.outputPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

func TestSchemaPlanEditIsNotInvokedWhenSchemaIsSynced(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	marker := filepath.Join(dir, "editor-ran")
	installScriptEditor(t, `touch "`+marker+`"`)
	schemaSQL := `CREATE TABLE synced_edit_users (id INTEGER PRIMARY KEY);`
	fixture := newPlanFixture(c, "syncededit", schemaSQL, schemaSQL)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--save", "--edit")...)

	// There is nothing to edit when the schemas already agree; opening an
	// editor on an empty plan would be a confusing prompt with no effect.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schema is synced, no changes to be made.")
	_, statErr := os.Stat(marker)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("editor must not run on a synced schema"))
	assertNoPlanFileWritten(c, dir)
}

func TestSchemaPlanEditDryRunPrintsEditedDocumentWithoutSaving(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	installScriptEditor(t, `printf 'CREATE TABLE dry_edited (id INTEGER PRIMARY KEY);\n' > "$1"`)
	fixture := newPlanFixture(c, "dryedit", "", `CREATE TABLE dry_original (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"), fixture.args("--dry-run", "--edit")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	plan := parsePlanDocumentOutput(c, out)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "dry_edited")
	assertNoPlanFileWritten(c, dir)
	assertNoPlanFileWritten(c, fixture.dir)
}

func TestSchemaPlanEditCombinesWithNameFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	installScriptEditor(t, `printf 'CREATE TABLE combined_edit (id INTEGER PRIMARY KEY);\n' > "$1"`)
	fixture := newPlanFixture(c, "combined", "", `CREATE TABLE combined_original (id INTEGER PRIMARY KEY);`)

	out, err := runSchemaPlan(atlas.NewCompatCommand("atlas"),
		fixture.args("--save", "--edit", "--name-format", "edited_plan")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Plan saved to file://edited_plan.plan.hcl")
	plan, _, readErr := atlasschema.ReadPlanDocument(filepath.Join(dir, "edited_plan.plan.hcl"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(plan.Statements[0].SQL, qt.Contains, "combined_edit")
}
