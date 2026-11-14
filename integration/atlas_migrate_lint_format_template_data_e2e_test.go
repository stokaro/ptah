//go:build integration

package integration_test

import (
	"cmp"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// lintTemplateDataCase is one `migrate lint --format` invocation over one
// directory. files are written to migrations/ and hashed for dirFormat, atlas
// when empty; edited are written after the hash, so the run sees a tampered
// atlas.sum. dir is the --dir value, file://migrations when empty.
type lintTemplateDataCase struct {
	name      string
	files     map[string]string
	edited    map[string]string
	dirFormat string
	dir       string
	latest    string
	format    string
}

const lintTemplateDataCounts = "{{ .TotalFiles }} {{ .TotalChanges }} {{ .DiagnosticsCount }}"

// lintTemplateDataTwoFiles is a base and one analyzed version.
var lintTemplateDataTwoFiles = map[string]string{
	"1.sql": "CREATE TABLE t1 (id int);\n",
	"2.sql": "CREATE TABLE t2 (id int);\n",
}

// lintTemplateDataDestructive drops a table on line 3 of version 2, then a
// column and a table on lines 1 and 2 of version 3.
var lintTemplateDataDestructive = map[string]string{
	"1.sql": "CREATE TABLE t1 (id int, a int);\nCREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n",
	"2.sql": "-- comment\n\nDROP TABLE t2;\n",
	"3.sql": "ALTER TABLE t1 DROP COLUMN a;\nDROP TABLE t3;\n",
}

// lintTemplateDataCases are the cells of stokaro/ptah#6000 and the shapes
// around them: several analyzed files, a file with diagnostics, fixes and an
// analyzer error, an atlas.sum failure, a failed replay, and a Flyway
// directory. Each prints only what both binaries agree on by design; the HCL
// bodies, the driver's wording of a replay error and the detail of a checksum
// failure are separate questions.
var lintTemplateDataCases = []lintTemplateDataCase{
	{
		name: "counts and the report address", files: lintTemplateDataTwoFiles, latest: "1",
		format: lintTemplateDataCounts + " [{{ .URL }}]",
	},
	{
		name: "a line and the fixes of a clean file", files: lintTemplateDataTwoFiles, latest: "1",
		format: "{{ range .Files }}{{ .Line 0 }} {{ .SuggestedFixes }} {{ json .SuggestedFixes }}{{ end }}",
	},
	{
		name: "the directory as given", files: lintTemplateDataTwoFiles, latest: "1", dir: "file://migrations",
		format: "{{ .Env.Dir }}",
	},
	{
		name: "the directory as given, cleaned", files: lintTemplateDataTwoFiles, latest: "1", dir: "file://./migrations",
		format: "{{ .Env.Dir }}",
	},
	{
		name: "counts per statement over two files",
		files: map[string]string{
			"1.sql": "CREATE TABLE t1 (id int);\n",
			"2.sql": "CREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n",
			"3.sql": "CREATE TABLE t4 (id int);\n",
		},
		latest: "2", format: lintTemplateDataCounts,
	},
	{
		name: "diagnostics, lines, fixes and analyzer errors", files: lintTemplateDataDestructive, latest: "2",
		format: lintTemplateDataCounts + "|{{ range .Files }}{{ $f := . }}{{ $f.Name }}[{{ .Error }}]" +
			"{{ range .Reports }}{{ range .Diagnostics }} {{ $f.Line .Pos }}{{ end }}{{ end }} {{ .SuggestedFixes }};{{ end }}" +
			"|{{ range .Steps }}{{ .Name }}: {{ .Text }}{{ with .Result }} [{{ .Error }}]{{ end }};{{ end }}",
	},
	{
		name: "an atlas.sum failure", files: lintTemplateDataTwoFiles, latest: "1",
		edited: map[string]string{"2.sql": "CREATE TABLE t2 (id int, x int);\n"},
		format: "{{ json .Schema }} " + lintTemplateDataCounts + " {{ .Env.Dir }}",
	},
	{
		name: "a failed replay", latest: "1",
		files:  map[string]string{"1.sql": "CREATE TABLE t1 (id int);\n", "2.sql": "CREATE TABLEX t2 (id int);\n"},
		format: "{{ json .Schema }} " + lintTemplateDataCounts + " {{ range .Files }}{{ .Name }} {{ .Line 0 }}{{ end }}",
	},
	{
		name: "a Flyway directory", dirFormat: "flyway", latest: "1",
		files:  map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "V1.5__b.sql": "CREATE TABLE b (id int);\n"},
		format: "{{ range .Files }}{{ .Name }}{{ end }}|{{ range .Steps }}{{ .Name }};{{ end }}",
	},
	{
		name: "a failed replay in a Flyway directory", dirFormat: "flyway", latest: "1",
		files:  map[string]string{"V1__a.sql": "CREATE TABLE a (id int);\n", "V1.5__b.sql": "CREATE TABLEX b (id int);\n"},
		format: "{{ range .Files }}{{ .Name }}{{ end }}|{{ range .Steps }}{{ .Name }};{{ end }}",
	},
}

// writeLintTemplateDataProject writes the case's migrations, hashes them with
// ptah-compat for the case's layout, and applies the edits.
func writeLintTemplateDataProject(c *qt.C, test lintTemplateDataCase) string {
	c.Helper()
	root := c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for name, body := range test.files {
		writeLintE2EFile(c, migrations, name, body)
	}
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations", "--dir-format", cmp.Or(test.dirFormat, "atlas"))
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	for name, body := range test.edited {
		writeLintE2EFile(c, migrations, name, body)
	}
	return root
}

// TestCompatMigrateLintFormatTemplateDataE2E runs the pinned community binary
// and the shipped ptah-compat on the same directory and template, and holds
// the exit code and both streams to the community binary's.
func TestCompatMigrateLintFormatTemplateDataE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, test := range lintTemplateDataCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeLintTemplateDataProject(c, test)
			args := []string{
				"migrate", "lint",
				"--dir", cmp.Or(test.dir, "file://migrations"),
				"--dir-format", cmp.Or(test.dirFormat, "atlas"),
				"--dev-url", "sqlite://dev?mode=memory",
				"--latest", test.latest,
				"--format", test.format,
			}

			communityOut, communityErr, err := runCLIProcess(c.Context(), root, reference, args...)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}, args...)

			c.Assert(compat.ExitCode, qt.Equals, exitStatusOf(c, err), qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(compat.Stdout, qt.Equals, communityOut)
			c.Assert(compat.Stderr, qt.Equals, withoutCommunityBuildNotice(communityErr))
		})
	}
}

// TestCompatMigrateLintFormatSchemaFramingE2E holds the framing of the two
// schema documents on a passing run: no generated-code marker and one
// trailing line feed, on both binaries. The bodies differ and are not
// compared here.
func TestCompatMigrateLintFormatSchemaFramingE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	c := qt.New(t)
	root := writeLintTemplateDataProject(c, lintTemplateDataCase{files: lintTemplateDataTwoFiles})
	args := []string{
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://dev?mode=memory",
		"--latest", "1",
		"--format", "{{ .Schema.Current }}|{{ .Schema.Desired }}|",
	}

	communityOut, _, err := runCLIProcess(c.Context(), root, reference, args...)
	compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}, args...)

	c.Assert(err, qt.IsNil)
	c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", compat.Stderr))
	for _, out := range []string{communityOut, compat.Stdout} {
		c.Assert(out, qt.Not(qt.Contains), "DO NOT EDIT")
		c.Assert(out, qt.Not(qt.Contains), "\n\n|")
		c.Assert(out, qt.Matches, `(?s).+\}\n\|.+\}\n\|`)
	}
}
