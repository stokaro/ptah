package migratetests_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCompatMigrateApplyFormat_FlywayFilesKeepTheirDirectoryNames pins the
// file entries of an apply report over a Flyway directory. The run executes
// each file as a numeric Atlas file whose description conversion rewrote
// (`4611686018427471935_b_c.sql`, "b_c"), and the pinned community binary
// v1.3.0 reports the file the directory holds under the description Flyway
// gives it, measured on SQLite with this directory.
func TestCompatMigrateApplyFormat_FlywayFilesKeepTheirDirectoryNames(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1__a.sql", body: "CREATE TABLE a (id int);\n"},
		{name: "V1.5__b c.sql", body: "CREATE TABLE b (id int);\n"},
	})

	stdout, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+filepath.Join(c.TempDir(), "report.db"),
		"--format", `{{ json .Pending }}|{{ range .Applied }}{{ .Name }}:{{ .Description }};{{ end }}`,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	pending, applied, found := strings.Cut(stdout, "|")
	c.Assert(found, qt.IsTrue, qt.Commentf("stdout: %s", stdout))
	var files []map[string]string
	c.Assert(json.Unmarshal([]byte(pending), &files), qt.IsNil)
	c.Assert(files, qt.DeepEquals, []map[string]string{
		{"Name": "V1__a.sql", "Version": "1", "Description": "a"},
		{"Name": "V1.5__b c.sql", "Version": "1.5", "Description": "b c"},
	})
	c.Assert(applied, qt.Equals, "V1__a.sql:a;V1.5__b c.sql:b c;")
}
