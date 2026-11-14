//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sourceLinesTemplate prints, for each analyzed file, the name, the text and
// the position of each diagnostic a `migrate lint --format` template reads.
const sourceLinesTemplate = `{{ range .Files }}{{ .Name }}|{{ printf "%q" .Text }}` +
	`{{ range .Reports }}{{ range .Diagnostics }}|{{ .Pos }}{{ end }}{{ end }}{{ end }}`

// sourceLinesFinding matches the line a text report places a finding on.
var sourceLinesFinding = regexp.MustCompile(`-- (L\d+): Dropping table "t"`)

// sourceLinesRows are directories in a foreign layout whose second version
// drops a table below lines the conversion to an Atlas migration removes.
// community and communityTemplate are what the pinned community binary v1.3.0
// prints under `migrate lint --dev-url sqlite://dev?mode=memory --latest 1`,
// measured on that binary; line and template are what ptah-compat prints. They
// differ on the goose and dbmate rows only, where that binary reports the
// statement on line 1, the section directive, and an offset into the up
// section rather than into the text it reports: we place the finding on the
// statement instead.
var sourceLinesRows = []struct {
	name              string
	format            string
	first             string
	second            string
	firstBody         string
	secondBody        string
	community         string
	communityTemplate string
	line              string
	template          string
}{
	{
		name: "liquibase", format: "liquibase", first: "1_init.sql", second: "2_drop.sql",
		firstBody:  "--liquibase formatted sql\n--changeset a:1\nCREATE TABLE t (id int);\n",
		secondBody: "--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t;\n",
		community:  "L4", communityTemplate: `2_drop.sql|"--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t;\n"|43`,
		line: "L4", template: `2_drop.sql|"--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t;\n"|43`,
	},
	{
		name: "golang-migrate", format: "golang-migrate", first: "1_a.up.sql", second: "2_b.up.sql",
		firstBody:  "CREATE TABLE t (id int);\n",
		secondBody: "-- note\n\nDROP TABLE t;\n",
		community:  "L3", communityTemplate: `2_b.up.sql|"-- note\n\nDROP TABLE t;\n"|9`,
		line: "L3", template: `2_b.up.sql|"-- note\n\nDROP TABLE t;\n"|9`,
	},
	{
		name: "flyway", format: "flyway", first: "V1__a.sql", second: "V2__b.sql",
		firstBody:  "CREATE TABLE t (id int);\n",
		secondBody: "-- note\n\nDROP TABLE t;\n",
		community:  "L3", communityTemplate: `V2__b.sql|"-- note\n\nDROP TABLE t;\n"|9`,
		line: "L3", template: `V2__b.sql|"-- note\n\nDROP TABLE t;\n"|9`,
	},
	{
		name: "goose", format: "goose", first: "1_a.sql", second: "2_b.sql",
		firstBody:         "-- +goose Up\nCREATE TABLE t (id int);\n",
		secondBody:        "-- +goose Up\n\n-- note\nDROP TABLE t;\n-- +goose Down\nCREATE TABLE t (id int);\n",
		community:         "L1",
		communityTemplate: `2_b.sql|"-- +goose Up\n\n-- note\nDROP TABLE t;\n-- +goose Down\nCREATE TABLE t (id int);\n"|10`,
		line:              "L4",
		template:          `2_b.sql|"-- +goose Up\n\n-- note\nDROP TABLE t;\n-- +goose Down\nCREATE TABLE t (id int);\n"|22`,
	},
	{
		name: "dbmate", format: "dbmate", first: "1_a.sql", second: "2_b.sql",
		firstBody:  "-- migrate:up\nCREATE TABLE t (id int);\n",
		secondBody: "-- migrate:up\n\n-- note\nDROP TABLE t;\n-- migrate:down\n",
		community:  "L1", communityTemplate: `2_b.sql|"-- migrate:up\n\n-- note\nDROP TABLE t;\n-- migrate:down\n"|9`,
		line: "L4", template: `2_b.sql|"-- migrate:up\n\n-- note\nDROP TABLE t;\n-- migrate:down\n"|23`,
	},
}

// sourceLinesProject writes the two files of a row under <root>/migrations,
// hashes the directory in its layout, and returns root and the --dir value.
func sourceLinesProject(c *qt.C, format, first, firstBody, second, secondBody string) (root, dir string) {
	c.Helper()
	root = c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	writeLintE2EFile(c, migrations, first, firstBody)
	writeLintE2EFile(c, migrations, second, secondBody)
	dir = "file://migrations?format=" + format
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", dir)
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	return root, dir
}

// sourceLinesArgs is the `migrate lint` invocation of a row.
func sourceLinesArgs(dir string, format ...string) []string {
	return append([]string{"migrate", "lint", "--dir", dir, "--dev-url", "sqlite://dev?mode=memory", "--latest", "1"}, format...)
}

// findingLine returns the line a text report places the drop on, "" when it
// places it on none.
func findingLine(stdout string) string {
	match := sourceLinesFinding.FindStringSubmatch(stdout)
	if match == nil {
		return ""
	}
	return match[1]
}

// TestCompatMigrateLintConvertedSourceLinesE2E reports a finding in a foreign
// layout in the terms of the source file the operator wrote: its name, its
// text, and the line and offset of the statement in it.
func TestCompatMigrateLintConvertedSourceLinesE2E(t *testing.T) {
	for _, test := range sourceLinesRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, dir := sourceLinesProject(c, test.format, test.first, test.firstBody, test.second, test.secondBody)
			opts := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}

			text := clirun.Run(c, clirun.Compat, opts, sourceLinesArgs(dir)...)
			template := clirun.Run(c, clirun.Compat, opts, sourceLinesArgs(dir, "--format", sourceLinesTemplate)...)

			c.Assert(text.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", text.Stdout, text.Stderr))
			c.Assert(findingLine(text.Stdout), qt.Equals, test.line, qt.Commentf("stdout:\n%s", text.Stdout))
			c.Assert(template.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", template.Stderr))
			c.Assert(template.Stdout, qt.Equals, test.template)
		})
	}
}

// TestCommunityMigrateLintConvertedSourceLinesE2E runs the pinned community
// binary on the same directories, so the expected values above stay its
// measurement.
func TestCommunityMigrateLintConvertedSourceLinesE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, test := range sourceLinesRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, dir := sourceLinesProject(c, test.format, test.first, test.firstBody, test.second, test.secondBody)

			text, textStderr, textErr := runCLIProcess(c.Context(), root, reference, sourceLinesArgs(dir)...)
			template, templateStderr, templateErr := runCLIProcess(c.Context(), root, reference,
				sourceLinesArgs(dir, "--format", sourceLinesTemplate)...)

			c.Assert(exitStatusOf(c, textErr), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", text, textStderr))
			c.Assert(findingLine(text), qt.Equals, test.community, qt.Commentf("stdout:\n%s", text))
			c.Assert(exitStatusOf(c, templateErr), qt.Equals, 1, qt.Commentf("stderr:\n%s", templateStderr))
			c.Assert(template, qt.Equals, test.communityTemplate)
		})
	}
}
