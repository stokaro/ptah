//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// revisionDescriptionFiles are the names whose description the community
// binary splits off differently from a humanized reading: a second
// underscore, no underscore at all, a dot, a space and non-ASCII text, with R
// and a timestamp so that file-name order, version-string order and numeric
// order all disagree.
var revisionDescriptionFiles = []string{
	"1_init.sql",
	"2__init.sql",
	"3_a_b.sql",
	"4.sql",
	"5_ünï.sql",
	"6_with space.sql",
	"7_a.b.sql",
	"20260101000000_x.sql",
	"R__view.sql",
}

// revisionDescriptionAppliedTemplate prints what the revision table says
// about each row and nothing that varies between runs.
const revisionDescriptionAppliedTemplate = `{{ range .Applied }}{{ .Version }} {{ printf "%q" .Description }}` + "\n" + `{{ end }}`

// revisionDescriptionRewritten is the row whose description the test rewrites
// in the database after the apply, and what it rewrites it to. Status reads
// Applied from the revision table, so the rewritten value is what it prints.
const (
	revisionDescriptionRewrittenVersion = "3"
	revisionDescriptionRewrittenTo      = "zzz"
)

// revisionDescriptionVarying matches the Applied fields that differ between two
// runs of the same directory and are blanked before a byte comparison:
// ExecutedAt and ExecutionTime are when and how long the run took, and
// OperatorVersion names the binary that wrote the row ("Atlas CLI v1.3.0" or
// "Ptah"). Every other byte of the list is compared, field order and
// "Description":"" included.
var revisionDescriptionVarying = regexp.MustCompile(`"(ExecutedAt|ExecutionTime|OperatorVersion)":("[^"]*"|[0-9]+)`)

// revisionDescriptionObservation is what one binary leaves behind and reports
// on the directory: Pending after the first version, then Available, Applied
// and the revision table rows after the rest, and the Applied JSON once a row's
// description has been rewritten in the database.
type revisionDescriptionObservation struct {
	pending     string
	available   string
	applied     string
	revisions   string
	appliedJSON string
}

func revisionDescriptionProject(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for index, name := range revisionDescriptionFiles {
		body := fmt.Sprintf("CREATE TABLE t%d (id int);\n", index)
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	return dir
}

// observeRevisionDescriptions applies the first version, reads Pending,
// applies the rest, and reads the lists and the revision table. run executes
// one invocation in dir and returns its standard output, failing the test on
// a non-zero exit.
func observeRevisionDescriptions(c *qt.C, dir string, run func(args ...string) string) revisionDescriptionObservation {
	c.Helper()
	target := []string{"--dir", "file://migrations", "--url", "sqlite://app.db"}
	run(append([]string{"migrate", "apply", "1"}, target...)...)
	pending := run(append([]string{"migrate", "status", "--format", "{{ json .Pending }}"}, target...)...)
	run(append([]string{"migrate", "apply"}, target...)...)
	observed := revisionDescriptionObservation{
		pending:   pending,
		available: run(append([]string{"migrate", "status", "--format", "{{ json .Available }}"}, target...)...),
		applied:   run(append([]string{"migrate", "status", "--format", revisionDescriptionAppliedTemplate}, target...)...),
		revisions: revisionTableDescriptions(c, filepath.Join(dir, "app.db")),
	}
	rewriteRevisionDescription(c, filepath.Join(dir, "app.db"))
	appliedJSON := run(append([]string{"migrate", "status", "--format", "{{ json .Applied }}"}, target...)...)
	observed.appliedJSON = revisionDescriptionVarying.ReplaceAllString(appliedJSON, `"$1":"varies"`)
	return observed
}

// rewriteRevisionDescription changes the description of one revision row in
// the database, leaving the directory alone.
func rewriteRevisionDescription(c *qt.C, path string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	result, err := db.ExecContext(c.Context(), "UPDATE atlas_schema_revisions SET description = ? WHERE version = ?",
		revisionDescriptionRewrittenTo, revisionDescriptionRewrittenVersion)
	c.Assert(err, qt.IsNil)
	rows, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.Equals, int64(1))
}

// revisionTableDescriptions reads atlas_schema_revisions back as
// "version|description" lines in version order.
func revisionTableDescriptions(c *qt.C, path string) string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT version, description FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var out strings.Builder
	for rows.Next() {
		var version, description string
		c.Assert(rows.Scan(&version, &description), qt.IsNil)
		fmt.Fprintf(&out, "%s|%s\n", version, description)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out.String()
}

func compatRevisionDescriptions(c *qt.C, dir string) revisionDescriptionObservation {
	c.Helper()
	return observeRevisionDescriptions(c, dir, func(args ...string) string {
		got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)
		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, got.Stdout, got.Stderr))
		return got.Stdout
	})
}

// TestCompatMigrateRevisionDescriptionE2E applies the directory with the
// shipped ptah-compat on SQLite and holds the revision table, Pending and
// Applied to what the pinned community binary v1.3.0 records and prints for
// the same directory.
func TestCompatMigrateRevisionDescriptionE2E(t *testing.T) {
	c := qt.New(t)
	dir := revisionDescriptionProject(c)

	got := compatRevisionDescriptions(c, dir)

	c.Assert(got.revisions, qt.Equals, "1|init\n"+
		"2|_init\n"+
		"20260101000000|x\n"+
		"3|a_b\n"+
		"4|\n"+
		"5|ünï\n"+
		"6|with space\n"+
		"7|a.b\n"+
		"R|_view\n")
	c.Assert(got.applied, qt.Equals, "1 \"init\"\n"+
		"2 \"_init\"\n"+
		"20260101000000 \"x\"\n"+
		"3 \"a_b\"\n"+
		"4 \"\"\n"+
		"5 \"ünï\"\n"+
		"6 \"with space\"\n"+
		"7 \"a.b\"\n"+
		"R \"_view\"\n")
	c.Assert(got.appliedJSON, qt.Contains, `{"Version":"3","Description":"zzz",`)
	c.Assert(got.appliedJSON, qt.Contains, `{"Version":"4","Description":"",`)
	c.Assert(got.pending, qt.Equals, `[`+
		`{"Name":"20260101000000_x.sql","Version":"20260101000000","Description":"x"},`+
		`{"Name":"2__init.sql","Version":"2","Description":"_init"},`+
		`{"Name":"3_a_b.sql","Version":"3","Description":"a_b"},`+
		`{"Name":"4.sql","Version":"4"},`+
		`{"Name":"5_ünï.sql","Version":"5","Description":"ünï"},`+
		`{"Name":"6_with space.sql","Version":"6","Description":"with space"},`+
		`{"Name":"7_a.b.sql","Version":"7","Description":"a.b"},`+
		`{"Name":"R__view.sql","Version":"R","Description":"_view"}`+
		`]`)
}

// TestCompatMigrateRevisionDescriptionMatchesTheCommunityBinaryE2E runs the
// pinned community binary and ptah-compat on copies of one directory and holds
// the revision table rows and the status lists to the community ones byte for
// byte.
func TestCompatMigrateRevisionDescriptionMatchesTheCommunityBinaryE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	communityDir := revisionDescriptionProject(c)
	compatDir := revisionDescriptionProject(c)

	community := observeRevisionDescriptions(c, communityDir, func(args ...string) string {
		stdout, stderr, err := runCLIProcess(c.Context(), communityDir, reference, args...)
		c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, stdout, stderr))
		return stdout
	})
	compat := compatRevisionDescriptions(c, compatDir)

	c.Assert(compat.revisions, qt.Equals, community.revisions)
	c.Assert(compat.available, qt.Equals, community.available)
	c.Assert(compat.pending, qt.Equals, community.pending)
	c.Assert(compat.applied, qt.Equals, community.applied)
	c.Assert(compat.appliedJSON, qt.Equals, community.appliedJSON)
}
