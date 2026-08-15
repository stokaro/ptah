package atlas_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyScopesHCLSchemaVarsToProjectSources pins which desired-state
// URLs an atlas.hcl `data "hcl_schema" { vars }` block scopes: the ones the env
// itself selected, and not the ones the operator typed.
//
// Measured on the pinned Atlas community binary v1.3.0 with the fixture
// [writeAtlasSchemaVarsProject] writes, `schema apply --env local --dry-run`,
// exit codes read directly from unpiped invocations:
//
//	(no --to)                              0  DEFAULT 'acme'
//	--to file://s.hcl --var tenant=zzz     0  DEFAULT 'zzz'
//	--to file://s.hcl                      1  missing value for required variable "tenant"
//
// ptah-compat answered `DEFAULT 'acme'` at exit 0 to all three before this
// wiring: the flag's URL matched a file the env's data source also selects, so
// classification handed it the block's vars and dropped --var. The third row is
// the AGENTS.md rule (a) half of that -- exit 0 where the pinned binary exits 1
// -- and it lives in
// [TestSchemaApplyRefusesExplicitToOutsideTheProjectVarScope].
//
// The two rows here are a discriminating pair on ONE fixture: the same file,
// the same env, the same data source, and only the way the run names the
// desired state differs. A binary that scoped everything would print 'acme'
// twice; one that scoped nothing would fail the first row for want of a value.
func TestSchemaApplyScopesHCLSchemaVarsToProjectSources(t *testing.T) {
	tests := []struct {
		name        string
		extraArgs   []string
		wantPlanned string
	}{
		{
			// The control. The env's src is the data source, so the file it
			// selects takes that block's values.
			name:        "the env's own source takes the data source vars",
			extraArgs:   nil,
			wantPlanned: "DEFAULT 'acme'",
		},
		{
			name:        "an explicit --to naming the same file keeps --var",
			extraArgs:   []string{"--to", "file://s.hcl", "--var", "tenant=zzz"},
			wantPlanned: "DEFAULT 'zzz'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeAtlasSchemaVarsProject(t)

			output, err := executeAtlasProjectCommand(append(
				[]string{"schema", "apply", "--env", "local", "--dry-run"},
				test.extraArgs...,
			)...)

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
			c.Check(output, qt.Contains, test.wantPlanned)
		})
	}
}

// TestSchemaApplyRefusesExplicitToOutsideTheProjectVarScope is the rule (a) row
// of the table in [TestSchemaApplyScopesHCLSchemaVarsToProjectSources]: a --to
// the operator typed carries no project scope, so a schema file with a required
// variable and no --var has no value to read and the run fails, exactly as the
// pinned Atlas community binary v1.3.0 fails it.
func TestSchemaApplyRefusesExplicitToOutsideTheProjectVarScope(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeAtlasSchemaVarsProject(t)

	output, err := executeAtlasProjectCommand(
		"schema", "apply", "--env", "local", "--to", "file://s.hcl", "--dry-run")

	c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "tenant"`,
		qt.Commentf("command output:\n%s", output))
}

// TestSchemaDiffScopesHCLSchemaVarsToProjectSources is the sibling verb. The
// scope is attached during classification, which every desired-state flag goes
// through, so `schema diff --to` had the same defect and needs the same pin:
// measured on the pinned binary, `schema diff --env local --from
// sqlite://empty.db --to file://s.hcl` is exit 1 with the missing-value
// sentence, where ptah-compat printed the `'acme'` table at exit 0.
func TestSchemaDiffScopesHCLSchemaVarsToProjectSources(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeAtlasSchemaVarsProject(t)

	output, err := executeAtlasProjectCommand(
		"schema", "diff", "--env", "local", "--from", "sqlite://empty.db", "--to", "file://s.hcl")

	c.Assert(err, qt.ErrorMatches, `.*missing value for required variable "tenant"`,
		qt.Commentf("command output:\n%s", output))
}

// TestSchemaInspectAppliesProjectVarsThroughEnvSrc is the other half of the
// join, on the one verb that reaches a data source through `env://src` rather
// than through a substituted file URL.
//
// `schema inspect` materializes its desired state on the dev database itself
// instead of going through the shared resolver, and it used to hand the loader
// the URLs read back out of the classified set — which drops the scope those
// sources carry. Measured on the pinned Atlas community binary v1.3.0,
// `schema inspect --env local --url env://src`, exit codes read directly from
// unpiped invocations: 0 with `default = "acme"` there, 1 with `missing value
// for required variable "tenant"` here.
//
// The assertion is on the value reaching the file, not on the rendered text
// around it: this surface prints the default as `"'acme'"`, which it also does
// for a literal `--url file://s.hcl --var tenant=acme` that both binaries exit
// 0 on, so the quoting is a separate pre-existing divergence and pinning it
// here would freeze it. Without the scope the command does not render at all --
// it exits 1 -- so the nil-error assertion is what makes this discriminating.
func TestSchemaInspectAppliesProjectVarsThroughEnvSrc(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeAtlasSchemaVarsProject(t)

	output, err := executeAtlasProjectCommand(
		"schema", "inspect", "--env", "local", "--url", "env://src")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Check(output, qt.Contains, "acme")
}

// writeAtlasSchemaVarsProject writes the fixture the scope tests share into the
// current directory: one schema file whose column default is a required
// variable, and an atlas.hcl whose env selects that file through a data source
// carrying the value.
func writeAtlasSchemaVarsProject(tb testing.TB) {
	tb.Helper()
	c := qt.New(tb)
	c.Assert(os.WriteFile("s.hcl", []byte(`variable "tenant" {
  type = string
}

schema "main" {
}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "tenant" {
    type    = text
    default = var.tenant
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

env "local" {
  src = data.hcl_schema.app.url
  url = "sqlite://target.db"
  dev = "sqlite://dev.db"
}
`), 0o600), qt.IsNil)
}
