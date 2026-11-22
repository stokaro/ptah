//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// compatRuleCodeRows are migrations the shipped ptah-compat lints on SQLite,
// and how its default report names the rule that fires. A rule that reports an
// Atlas check prints the line Atlas CE v1.4.0 prints for the same directory
// with `migrate lint --dir file://migrations --dev-url 'sqlite://dev?mode=memory'
// --latest 1`, and carries the check code as the JSON Code. A rule Atlas has
// no check for keeps its Ptah name, in brackets after the line number and as
// the JSON Code; the community binary reports nothing for the empty file.
var compatRuleCodeRows = []struct {
	name   string
	change string
	exit   int
	line   string
	code   string
	absent string
}{
	{
		name:   "a dropped table is Atlas DS102",
		change: "DROP TABLE users;\n",
		exit:   1,
		line:   "      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n",
		code:   "DS102",
		absent: "destructive/drop-table",
	},
	{
		name:   "an added NOT NULL column is Atlas MF103",
		change: "ALTER TABLE users ADD COLUMN c int NOT NULL;\n",
		exit:   0,
		line: "      -- L1: Adding a non-nullable \"int\" column \"c\" will fail in case table \"users\" is not empty\n" +
			"         https://atlasgo.io/lint/analyzers#MF103\n",
		code:   "MF103",
		absent: "data-dependent/add-not-null-column",
	},
	{
		name:   "an empty migration has no Atlas check",
		change: "-- nothing to run\n",
		exit:   0,
		line: "      -- L0 [file-form/empty-migration]: the migration contains no executable statements; delete\n" +
			"         it or fill it in\n",
		code:   "file-form/empty-migration",
		absent: "analyzers#",
	},
}

// compatRuleCodeTemplate prints the code of every diagnostic of the report,
// one per line.
var compatRuleCodeTemplate = []string{"--format", `{{ range .Files }}{{ range .Reports }}{{ range .Diagnostics }}{{ .Code }}{{ "\n" }}{{ end }}{{ end }}{{ end }}`}

// TestCompatMigrateLintNamesRulesByAtlasCodeE2E holds the default policy of
// ptah-compat to the community binary's way of naming a lint finding, in the
// text report and in a `--format` template.
func TestCompatMigrateLintNamesRulesByAtlasCodeE2E(t *testing.T) {
	for _, row := range compatRuleCodeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := changeCountProject(c, map[string]string{"1_users.sql": "CREATE TABLE users (id int);\n", "2_change.sql": row.change})
			run := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

			text := clirun.Run(c, clirun.Compat, run, changeCountArgs("sqlite://dev?mode=memory", "1")...)
			codes := clirun.Run(c, clirun.Compat, run, changeCountArgs("sqlite://dev?mode=memory", "1", compatRuleCodeTemplate...)...)

			c.Assert(text.ExitCode, qt.Equals, row.exit, qt.Commentf("stdout:\n%s\nstderr:\n%s", text.Stdout, text.Stderr))
			c.Assert(text.Stdout, qt.Contains, row.line)
			c.Assert(text.Stdout, qt.Not(qt.Contains), row.absent)
			c.Assert(codes.Stdout, qt.Equals, row.code+"\n", qt.Commentf("stderr:\n%s", codes.Stderr))
		})
	}
}

// TestCompatMigrateLintPrintsTheSameCodeUnderBothPoliciesE2E is the control
// for the default report: the strict policy, which reproduces the community
// binary, prints the dropped table the same way. The policies differ in which
// rules run, so the strict run leaves out the empty migration's Ptah rule.
func TestCompatMigrateLintPrintsTheSameCodeUnderBothPoliciesE2E(t *testing.T) {
	c := qt.New(t)
	dir := changeCountProject(c, map[string]string{
		"1_users.sql":  "CREATE TABLE users (id int);\n",
		"2_change.sql": "DROP TABLE users;\n",
		"3_empty.sql":  "-- nothing to run\n",
	})
	args := changeCountArgs("sqlite://dev?mode=memory", "2", compatRuleCodeTemplate...)

	full := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)
	strict := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: strictEnvironment()}, args...)

	c.Assert(full.Stdout, qt.Equals, "DS102\nfile-form/empty-migration\n", qt.Commentf("stderr:\n%s", full.Stderr))
	c.Assert(strict.Stdout, qt.Equals, "DS102\n", qt.Commentf("stderr:\n%s", strict.Stderr))
}
