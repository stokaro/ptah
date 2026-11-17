//go:build integration

package integration_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/clirun"
)

// The `--format` template surface of each compat verb, compared with the pinned
// community binary v1.3.0 on SQLite, which both binaries carry compiled in. A
// template either binary refuses is refused by the other, in the same words but
// for the Go type an execution error names, which is each binary's own.

// formatTemplateFixture is one run's files: a schema with one table, an empty
// schema, an empty migration directory, a migration directory holding one
// hashed file, and a target database file nothing has created yet.
type formatTemplateFixture struct {
	root    string
	schema  string
	empty   string
	dir     string
	lintDir string
	target  string
}

const formatTemplateDevURL = "sqlite://dev?mode=memory"

func newFormatTemplateFixture(c *qt.C) formatTemplateFixture {
	c.Helper()
	root := c.TempDir()
	f := formatTemplateFixture{
		root:    root,
		schema:  filepath.Join(root, "schema.sql"),
		empty:   filepath.Join(root, "empty.sql"),
		dir:     filepath.Join(root, "migrations"),
		lintDir: filepath.Join(root, "lint"),
		target:  filepath.Join(root, "target.db"),
	}
	c.Assert(os.WriteFile(f.schema, []byte("CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(f.empty, nil, 0o600), qt.IsNil)
	c.Assert(os.Mkdir(f.dir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(f.lintDir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(f.lintDir, "1_init.sql"), []byte("CREATE TABLE users (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	// The sum both binaries write for that one file.
	c.Assert(os.WriteFile(filepath.Join(f.lintDir, "atlas.sum"), []byte(
		"h1:qPDjCG0RrFuPwN9SX42bBFMTvhivZRX5Q5EN7L13Cdc=\n"+
			"1_init.sql h1:takRko4yf7f5+oBE/Esi0mYMN3NoZRu2jgrM15M3VK4=\n"), 0o600), qt.IsNil)
	return f
}

func fileURL(path string) string {
	return "file://" + filepath.ToSlash(path)
}

// The arguments of each verb's run over a fixture, without --format.

func migrateDiffArgs(f formatTemplateFixture) []string {
	return []string{"migrate", "diff", "--dir", fileURL(f.dir), "--to", fileURL(f.schema), "--dev-url", formatTemplateDevURL}
}

func schemaDiffArgs(f formatTemplateFixture) []string {
	return []string{"schema", "diff", "--from", fileURL(f.empty), "--to", fileURL(f.schema), "--dev-url", formatTemplateDevURL}
}

func schemaApplyArgs(f formatTemplateFixture) []string {
	return []string{"schema", "apply", "--url", "sqlite://" + filepath.ToSlash(f.target), "--to", fileURL(f.schema),
		"--dev-url", formatTemplateDevURL, "--dry-run"}
}

// schemaApplyPromptingArgs is a run that would ask before applying.
func schemaApplyPromptingArgs(f formatTemplateFixture) []string {
	return []string{"schema", "apply", "--url", "sqlite://" + filepath.ToSlash(f.target), "--to", fileURL(f.schema),
		"--dev-url", formatTemplateDevURL}
}

func schemaInspectArgs(f formatTemplateFixture) []string {
	return []string{"schema", "inspect", "--url", "sqlite://" + filepath.ToSlash(f.target)}
}

func migrateLintArgs(f formatTemplateFixture) []string {
	return []string{"migrate", "lint", "--dir", fileURL(f.lintDir), "--dev-url", formatTemplateDevURL, "--latest", "1"}
}

func migrateStatusArgs(f formatTemplateFixture) []string {
	return []string{"migrate", "status", "--dir", fileURL(f.lintDir), "--url", "sqlite://" + filepath.ToSlash(f.target)}
}

func schemaApplyAutoApproveArgs(f formatTemplateFixture) []string {
	return []string{"schema", "apply", "--url", "sqlite://" + filepath.ToSlash(f.target), "--to", fileURL(f.schema),
		"--dev-url", formatTemplateDevURL, "--auto-approve"}
}

// formatArgs is a verb's arguments with --format format.
func formatArgs(verb func(formatTemplateFixture) []string, f formatTemplateFixture, format string) []string {
	return append(verb(f), "--format", format)
}

// formatTemplateRefusal is a template the pinned community binary refuses on a
// verb, and the start of the error it writes on stderr, up to the Go type an
// execution error names.
type formatTemplateRefusal struct {
	name    string
	verb    func(formatTemplateFixture) []string
	format  string
	wantErr string
}

// formatTemplateRefusals is each template Ptah has rendered and the pinned
// community binary refuses, measured on v1.3.0.
var formatTemplateRefusals = []formatTemplateRefusal{
	{
		name: "migrate diff has no MarshalSQL", verb: migrateDiffArgs, format: `{{ .MarshalSQL }}`,
		wantErr: `Error: template: format:1:3: executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type `,
	},
	{
		name: "migrate diff has no json", verb: migrateDiffArgs, format: `{{ json . }}`,
		wantErr: `Error: parse format: template: format:1: function "json" not defined`,
	},
	{
		name: "a schema diff change has no Cmd", verb: schemaDiffArgs, format: `{{ range .Changes }}{{ .Cmd }}{{ end }}`,
		wantErr: `Error: template: format:1:23: executing "format" at <.Cmd>: can't evaluate field Cmd in type `,
	},
	{
		name: "schema diff has no json", verb: schemaDiffArgs, format: `{{ json . }}`,
		wantErr: `Error: parse log format: template: format:1: function "json" not defined`,
	},
	{
		name: "schema apply has no sql", verb: schemaApplyArgs, format: `{{ sql . }}`,
		wantErr: `Error: parse log format: template: format:1: function "sql" not defined`,
	},
	{
		name: "schema apply has no MarshalSQL", verb: schemaApplyArgs, format: `{{ .MarshalSQL }}`,
		wantErr: `Error: template: format:1:3: executing "format" at <.MarshalSQL>: can't evaluate field MarshalSQL in type `,
	},
	{
		name: "schema apply changes have no length", verb: schemaApplyArgs, format: `{{ len .Changes }}`,
		wantErr: `Error: template: format:1:3: executing "format" at <len .Changes>: error calling len: len of type `,
	},
	{
		name: "schema apply does not prompt with --format", verb: schemaApplyPromptingArgs, format: `{{ len .Changes.Pending }}`,
		wantErr: `Error: --log and --format can only be used with --dry-run or --auto-approve`,
	},
	{
		name: "schema inspect has no dbml", verb: schemaInspectArgs, format: `{{ dbml . }}`,
		wantErr: `Error: parse log format: template: format:1: function "dbml" not defined`,
	},
	{
		name: "schema inspect has no Schema", verb: schemaInspectArgs, format: `{{ .Schema }}`,
		wantErr: `Error: template: format:1:3: executing "format" at <.Schema>: can't evaluate field Schema in type `,
	},
	{
		name: "migrate lint has no upper", verb: migrateLintArgs, format: `{{ upper "x" }}`,
		wantErr: `Error: parse format: template: format:1: function "upper" not defined`,
	},
	{
		name: "migrate status has no add", verb: migrateStatusArgs, format: `{{ add 1 2 }}`,
		wantErr: `Error: parse format: template: format:1: function "add" not defined`,
	},
}

// formatTemplateControl is a template both binaries render, and what they
// write: the control that the refusals above come from the template rather
// than from the fixture.
type formatTemplateControl struct {
	name       string
	verb       func(formatTemplateFixture) []string
	format     string
	wantStdout string
}

var formatTemplateControls = []formatTemplateControl{
	{name: "schema diff counts its changes", verb: schemaDiffArgs, format: `{{ len .Changes }}`, wantStdout: "1"},
	{name: "schema apply lists its pending changes", verb: schemaApplyArgs, format: `{{ len .Changes.Pending }}`, wantStdout: "1"},
	{name: "schema inspect lists the realm's schemas", verb: schemaInspectArgs, format: `{{ len .Realm.Schemas }}`, wantStdout: "1"},
	{name: "migrate lint counts its files", verb: migrateLintArgs, format: `{{ len .Files }}`, wantStdout: "1"},
	{name: "migrate status names the status", verb: migrateStatusArgs, format: `{{ .Status }}`, wantStdout: "PENDING"},
}

// runFormatTemplateVerb runs binary with args in dir, without the `PTAH_*`
// variables of the calling environment, so that neither the template
// extensions nor strict mode decide what a row measures.
func runFormatTemplateVerb(c *qt.C, binary, dir string, env []string, args ...string) clirun.Result {
	c.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(c.Context(), binary, args...) // #nosec G204 G702 -- the binary is this repository's build or the operator-provided PTAH_ATLAS_REFERENCE
	cmd.Dir = dir
	cmd.Env = append(withoutPtahVariables(os.Environ()), env...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		code = exitErr.ExitCode()
	} else {
		c.Assert(err, qt.IsNil, qt.Commentf("%s %s", binary, strings.Join(args, " ")))
	}
	return clirun.Result{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: code}
}

func withoutPtahVariables(environment []string) []string {
	kept := make([]string, 0, len(environment))
	for _, item := range environment {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PTAH_") {
			continue
		}
		kept = append(kept, item)
	}
	return kept
}

// assertFormatTemplateRefused runs one refusal on binary over a fresh fixture.
func assertFormatTemplateRefused(c *qt.C, binary string, row formatTemplateRefusal) {
	c.Helper()
	f := newFormatTemplateFixture(c)

	got := runFormatTemplateVerb(c, binary, f.root, nil, formatArgs(row.verb, f, row.format)...)

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, row.wantErr)
	c.Assert(got.Stdout, qt.Equals, "")
}

// assertFormatTemplateRendered runs one control on binary over a fresh fixture.
func assertFormatTemplateRendered(c *qt.C, binary string, row formatTemplateControl) {
	c.Helper()
	f := newFormatTemplateFixture(c)

	got := runFormatTemplateVerb(c, binary, f.root, nil, formatArgs(row.verb, f, row.format)...)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, row.wantStdout)
}

// TestCompatFormatTemplate_RefusesWhatTheReferenceRefusesE2E is the shipped
// compat binary refusing each template the pinned community binary refuses.
func TestCompatFormatTemplate_RefusesWhatTheReferenceRefusesE2E(t *testing.T) {
	compat := clirun.Build(qt.New(t), clirun.Compat)
	for _, row := range formatTemplateRefusals {
		t.Run(row.name, func(t *testing.T) {
			assertFormatTemplateRefused(qt.New(t), compat, row)
		})
	}
}

// TestReferenceFormatTemplate_RefusesTheSameTemplatesE2E is the measurement the
// rows above restate, taken again against the pinned community binary, so a
// row that stops describing that binary fails here.
func TestReferenceFormatTemplate_RefusesTheSameTemplatesE2E(t *testing.T) {
	reference := requireEngineReference(t)
	for _, row := range formatTemplateRefusals {
		t.Run(row.name, func(t *testing.T) {
			assertFormatTemplateRefused(qt.New(t), reference, row)
		})
	}
}

// TestCompatFormatTemplate_RendersWhatTheReferenceRendersE2E is the control:
// the same verbs over the same fixtures render a template both binaries
// accept.
func TestCompatFormatTemplate_RendersWhatTheReferenceRendersE2E(t *testing.T) {
	compat := clirun.Build(qt.New(t), clirun.Compat)
	for _, row := range formatTemplateControls {
		t.Run(row.name, func(t *testing.T) {
			assertFormatTemplateRendered(qt.New(t), compat, row)
		})
	}
}

// TestReferenceFormatTemplate_RendersTheSameTemplatesE2E takes the controls
// against the pinned community binary.
func TestReferenceFormatTemplate_RendersTheSameTemplatesE2E(t *testing.T) {
	reference := requireEngineReference(t)
	for _, row := range formatTemplateControls {
		t.Run(row.name, func(t *testing.T) {
			assertFormatTemplateRendered(qt.New(t), reference, row)
		})
	}
}

// migrationFiles is the names of the .sql files in dir.
func migrationFiles(c *qt.C, dir string) []string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	return matches
}

// TestCompatMigrateDiffTemplateRefusal_WritesNoFileE2E holds the compat binary
// to what the pinned community binary does with a template it refuses: no
// migration file is written. The control writes one with a template it
// renders.
func TestCompatMigrateDiffTemplateRefusal_WritesNoFileE2E(t *testing.T) {
	c := qt.New(t)
	compat := clirun.Build(c, clirun.Compat)
	refused := newFormatTemplateFixture(c)
	rendered := newFormatTemplateFixture(c)

	refusal := runFormatTemplateVerb(c, compat, refused.root, nil, formatArgs(migrateDiffArgs, refused, `{{ .MarshalSQL }}`)...)
	control := runFormatTemplateVerb(c, compat, rendered.root, nil, formatArgs(migrateDiffArgs, rendered, `{{ sql . }}`)...)

	c.Assert(refusal.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", refusal.Stderr))
	c.Assert(migrationFiles(c, refused.dir), qt.HasLen, 0)
	c.Assert(control.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", control.Stderr))
	c.Assert(migrationFiles(c, rendered.dir), qt.HasLen, 1)
}

// TestReferenceMigrateDiffTemplateRefusal_WritesNoFileE2E is the same
// measurement against the pinned community binary.
func TestReferenceMigrateDiffTemplateRefusal_WritesNoFileE2E(t *testing.T) {
	reference := requireEngineReference(t)
	c := qt.New(t)
	refused := newFormatTemplateFixture(c)
	rendered := newFormatTemplateFixture(c)

	refusal := runFormatTemplateVerb(c, reference, refused.root, nil, formatArgs(migrateDiffArgs, refused, `{{ .MarshalSQL }}`)...)
	control := runFormatTemplateVerb(c, reference, rendered.root, nil, formatArgs(migrateDiffArgs, rendered, `{{ sql . }}`)...)

	c.Assert(refusal.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", refusal.Stderr))
	c.Assert(migrationFiles(c, refused.dir), qt.HasLen, 0)
	c.Assert(control.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", control.Stderr))
	c.Assert(migrationFiles(c, rendered.dir), qt.HasLen, 1)
}

// targetTables is the number of tables the fixture's target holds, read back
// through the compat binary's own inspection.
func targetTables(c *qt.C, compat string, f formatTemplateFixture) string {
	c.Helper()
	got := runFormatTemplateVerb(c, compat, f.root, nil,
		formatArgs(schemaInspectArgs, f, `{{ len (index .Realm.Schemas 0).Tables }}`)...)
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	return got.Stdout
}

// TestCompatSchemaApplyTemplateRefusal_AppliesNothingE2E refuses a template
// before the plan runs, so the target is left as it was. The control applies
// the same plan with a template the binary renders.
//
// This is deliberately stricter than the pinned community binary, which
// applies the plan and then fails on the template:
// TestReferenceSchemaApplyTemplateRefusal_AppliesThePlanE2E measures it. A run
// that exits 1 after changing the database reports a failure for a change that
// happened, so a pipeline that retries applies twice and one that rolls back
// has nothing to roll back to.
func TestCompatSchemaApplyTemplateRefusal_AppliesNothingE2E(t *testing.T) {
	c := qt.New(t)
	compat := clirun.Build(c, clirun.Compat)
	refused := newFormatTemplateFixture(c)
	rendered := newFormatTemplateFixture(c)

	refusal := runFormatTemplateVerb(c, compat, refused.root, nil, formatArgs(schemaApplyAutoApproveArgs, refused, `{{ .MarshalSQL }}`)...)
	control := runFormatTemplateVerb(c, compat, rendered.root, nil, formatArgs(schemaApplyAutoApproveArgs, rendered, `{{ len .Changes.Applied }}`)...)

	c.Assert(refusal.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", refusal.Stderr))
	c.Assert(targetTables(c, compat, refused), qt.Equals, "0")
	c.Assert(control.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", control.Stderr))
	c.Assert(control.Stdout, qt.Equals, "1")
	c.Assert(targetTables(c, compat, rendered), qt.Equals, "1")
}

// TestReferenceSchemaApplyTemplateRefusal_AppliesThePlanE2E measures the
// defect the compat binary does not copy: the pinned community binary applies
// the plan, then exits 1 on the template.
func TestReferenceSchemaApplyTemplateRefusal_AppliesThePlanE2E(t *testing.T) {
	reference := requireEngineReference(t)
	c := qt.New(t)
	compat := clirun.Build(c, clirun.Compat)
	f := newFormatTemplateFixture(c)

	got := runFormatTemplateVerb(c, reference, f.root, nil, formatArgs(schemaApplyAutoApproveArgs, f, `{{ .MarshalSQL }}`)...)

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `can't evaluate field MarshalSQL`)
	c.Assert(targetTables(c, compat, f), qt.Equals, "1")
}

// TestCompatTemplateExtensions_OpenPtahHelpersE2E renders, with
// PTAH_ATLAS_TEMPLATE_EXTENSIONS on, a template each verb refuses without it:
// compatibility withholds no capability Ptah has, it only stops offering it by
// default.
func TestCompatTemplateExtensions_OpenPtahHelpersE2E(t *testing.T) {
	tests := []struct {
		name       string
		verb       func(formatTemplateFixture) []string
		format     string
		wantStdout string
	}{
		{name: "schema apply", verb: schemaApplyArgs, format: `{{ printf "%.12s" (sql .) }}`, wantStdout: "-- Create \"u"},
		{name: "schema diff", verb: schemaDiffArgs, format: `{{ range .Changes }}{{ printf "%.12s" .Cmd }}{{ end }}`, wantStdout: "CREATE TABLE"},
		{name: "migrate lint", verb: migrateLintArgs, format: `{{ upper "x" }}`, wantStdout: "X"},
		{name: "migrate status", verb: migrateStatusArgs, format: `{{ add 1 2 }}`, wantStdout: "3"},
		{name: "schema inspect", verb: schemaInspectArgs, format: `{{ printf "%T" (dbml .) }}`, wantStdout: "string"},
	}
	compat := clirun.Build(qt.New(t), clirun.Compat)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFormatTemplateFixture(c)

			got := runFormatTemplateVerb(c, compat, f.root, []string{atlasreport.TemplateExtensionsEnvVar + "=1"}, formatArgs(test.verb, f, test.format)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			c.Assert(got.Stdout, qt.Equals, test.wantStdout)
		})
	}
}

// TestCompatTemplateExtensions_FailurePathE2E refuses the variable where it
// must not decide anything: strict mode, which selects the community binary's
// surface, refuses it on, and every verb it governs refuses a malformed value
// before doing work, in a run with no --format.
func TestCompatTemplateExtensions_FailurePathE2E(t *testing.T) {
	malformed := []string{atlasreport.TemplateExtensionsEnvVar + "=yes"}
	refusal := `invalid boolean value "yes" for ` + atlasreport.TemplateExtensionsEnvVar
	tests := []struct {
		name    string
		env     []string
		verb    func(formatTemplateFixture) []string
		wantErr string
	}{
		{
			name:    "strict mode refuses it",
			env:     []string{"PTAH_ATLAS_STRICT_COMPAT=1", atlasreport.TemplateExtensionsEnvVar + "=1"},
			verb:    schemaApplyArgs,
			wantErr: atlasreport.TemplateExtensionsEnvVar,
		},
		{name: "schema apply", env: malformed, verb: schemaApplyArgs, wantErr: refusal},
		{name: "schema diff", env: malformed, verb: schemaDiffArgs, wantErr: refusal},
		{name: "schema inspect", env: malformed, verb: schemaInspectArgs, wantErr: refusal},
		{name: "migrate diff", env: malformed, verb: migrateDiffArgs, wantErr: refusal},
		{name: "migrate lint", env: malformed, verb: migrateLintArgs, wantErr: refusal},
		{name: "migrate status", env: malformed, verb: migrateStatusArgs, wantErr: refusal},
	}
	compat := clirun.Build(qt.New(t), clirun.Compat)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFormatTemplateFixture(c)

			got := runFormatTemplateVerb(c, compat, f.root, test.env, test.verb(f)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.wantErr)
			c.Assert(migrationFiles(c, f.dir), qt.HasLen, 0)
		})
	}
}
