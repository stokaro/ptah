package atlas_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// `--export` selects an output template the project declares once, instead of
// one every invocation spells out. An exporter is a Go text/template over the
// same report `--format` renders, so the flag needs no evaluator of its own
// (stokaro/ptah#1620).

const exportProjectHCL = `exporter "markdown" {
  template = "## Rollout\n{{ range .Changes }}- {{ .Cmd }}\n{{ end }}"
}

env "local" {
  url      = "sqlite://%s"
  dev      = "sqlite://%s"
  exporter = "markdown"
}
`

// writeExportProject writes an atlas.hcl declaring one exporter and an env
// that selects it, and returns the config path.
func writeExportProject(c *qt.C, dir, body string) string {
	c.Helper()
	path := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

func runExportCommand(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaDiffExportRendersTheDeclaredTemplate is the flag end to end.
func TestSchemaDiffExportRendersTheDeclaredTemplate(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	dev := filepath.Join(dir, "dev.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	config := writeExportProject(c, dir, exportProjectBody(target, dev))

	out, err := runExportCommand(c, "schema", "diff",
		"-c", "file://"+config, "--env", "local",
		"--from", "sqlite://"+target, "--to", "file://"+desired,
		"--dev-url", "sqlite://"+dev, "--export")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "## Rollout")
	c.Assert(out, qt.Contains, "orders")
}

// TestSchemaDiffWithoutExportKeepsTheDefault is the control: the same project,
// the same command, one flag apart. Without it, the test above would pass on a
// build that rendered the template unconditionally.
func TestSchemaDiffWithoutExportKeepsTheDefault(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	dev := filepath.Join(dir, "dev.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	desired := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(desired,
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n"),
		0o600), qt.IsNil)
	config := writeExportProject(c, dir, exportProjectBody(target, dev))

	out, err := runExportCommand(c, "schema", "diff",
		"-c", "file://"+config, "--env", "local",
		"--from", "sqlite://"+target, "--to", "file://"+desired,
		"--dev-url", "sqlite://"+dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "## Rollout")
	c.Assert(out, qt.Contains, "CREATE TABLE")
}

// TestSchemaInspectExportRendersTheDeclaredTemplate covers the twin verb.
//
// The flag was registered on neither until now; registering it on one and not
// the other is the inconsistency the earlier batch flagged and deferred.
func TestSchemaInspectExportRendersTheDeclaredTemplate(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "json_report" {
  template = "EXPORTED {{ json . }}"
}

env "local" {
  url      = "sqlite://`+target+`"
  exporter = "json_report"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "EXPORTED ")
	c.Assert(out, qt.Contains, `"users"`)
}

// TestSchemaExportRefusesWhatItCannotResolve covers every way an export can
// fail to name a template.
//
// Each case would otherwise emit the ordinary report and let the operator
// believe their exporter ran, which is the failure the flag was a registered
// refusal to avoid. Implementing it must not reintroduce it.
func TestSchemaExportRefusesWhatItCannotResolve(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		extra  []string
		want   string
	}{
		{
			name: "env selects no exporter",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url = "sqlite://%s"
}
`,
			want: "this env selects no exporter",
		},
		{
			name: "env selects an exporter nothing declares",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url      = "sqlite://%s"
  exporter = "missing"
}
`,
			want: `no exporter "missing": this project declares [markdown]`,
		},
		{
			name: "both --export and --format choose the output",
			config: `exporter "markdown" {
  template = "x"
}

env "local" {
  url      = "sqlite://%s"
  exporter = "markdown"
}
`,
			extra: []string{"--format", "{{ json . }}"},
			want:  "--export and --format both choose the output",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			target := filepath.Join(dir, "target.db")
			createSQLiteSchemaCleanTable(c, target, "users")
			config := writeExportProject(c, dir, sprintfDB(test.config, target))

			args := append([]string{"schema", "inspect",
				"-c", "file://" + config, "--env", "local", "--export"}, test.extra...)
			out, err := runExportCommand(c, args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, test.want)
		})
	}
}

// TestSchemaExporterWithoutATemplateIsRefused covers a block that declares a
// name and nothing to render.
//
// An empty template renders to an empty string, so accepting it would print
// nothing at all and exit 0 -- the most complete version of the failure this
// flag exists to avoid, since there is not even a default report to notice.
func TestSchemaExporterWithoutATemplateIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "markdown" {
  format = "not-a-template"
}

env "local" {
  url      = "sqlite://`+target+`"
  exporter = "markdown"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `exporter "markdown" declares no template`)
}

// TestSchemaExportDeclaredTwiceIsRefused keeps two blocks under one name from
// making --export depend on which the parser reached last.
func TestSchemaExportDeclaredTwiceIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target := filepath.Join(dir, "target.db")
	createSQLiteSchemaCleanTable(c, target, "users")
	config := writeExportProject(c, dir, `exporter "markdown" {
  template = "first"
}

exporter "markdown" {
  template = "second"
}

env "local" {
  url      = "sqlite://`+target+`"
  exporter = "markdown"
}
`)

	out, err := runExportCommand(c, "schema", "inspect",
		"-c", "file://"+config, "--env", "local", "--export")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, `declares exporter "markdown" more than once`)
}

// exportProjectBody fills the shared project template with a target and dev
// database path.
func exportProjectBody(target, dev string) string {
	return fmt.Sprintf(exportProjectHCL, target, dev)
}

// sprintfDB fills a single %s in a config body with the database path, and
// leaves a body carrying none unchanged.
func sprintfDB(body, target string) string {
	if !strings.Contains(body, "%s") {
		return body
	}
	return fmt.Sprintf(body, target)
}
