//go:build integration

package integration_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/testutils"
)

// versionOrderRun runs one invocation of a binary in a project directory.
type versionOrderRun func(args ...string) clirun.Result

// versionOrderTarget is the directory and database every invocation names.
var versionOrderTarget = []string{"--dir", "file://migrations", "--url", "sqlite://app.db"}

// versionOrderStatusTemplate prints the parts of the status report the
// linearity verdict decides.
const versionOrderStatusTemplate = `{{ json .OutOfOrder }}|{{ json .Pending }}|{{ .Next }}|{{ .Error }}`

func compatVersionOrderRunner(c *qt.C, dir string, env []string) versionOrderRun {
	c.Helper()
	return func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: env}, args...)
	}
}

func referenceVersionOrderRunner(c *qt.C, dir, binary string, env []string) versionOrderRun {
	c.Helper()
	return func(args ...string) clirun.Result {
		cmd := exec.CommandContext(c.Context(), binary, args...)
		cmd.Dir = dir
		cmd.Env = env
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		return clirun.Result{
			Stdout:   stdout.String(),
			Stderr:   testutils.WithoutAtlasCommunityBuildNotice(stderr.String()),
			ExitCode: exitStatusOf(c, err),
		}
	}
}

// writeVersionOrderFiles writes one CREATE TABLE file per name and rehashes
// the directory with run.
func writeVersionOrderFiles(c *qt.C, dir string, run versionOrderRun, names []string) {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	for _, name := range names {
		table := "t_" + strings.NewReplacer(".", "_").Replace(strings.TrimSuffix(name, ".sql"))
		body := fmt.Sprintf("CREATE TABLE %s (id int);\n", table)
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	hashed := run("migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
}

// prepareVersionOrder applies first, then adds the added files and rehashes,
// so the added files are what the next apply decides about.
func prepareVersionOrder(c *qt.C, dir string, run versionOrderRun, first, added []string) {
	c.Helper()
	writeVersionOrderFiles(c, dir, run, first)
	applied := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	writeVersionOrderFiles(c, dir, run, added)
}

// revisionTableVersions reads the versions atlas_schema_revisions records, in
// version order.
func revisionTableVersions(c *qt.C, path string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(), "SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// TestCompatAtlasLinearityByVersionString_HappyPath applies a file whose
// number is below the applied mark but whose version string sorts above it.
// The pinned community binary v1.3.0 applies 3__x.sql after
// 20260101000000_ts.sql, because "3" sorts above "20260101000000".
func TestCompatAtlasLinearityByVersionString_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	prepareVersionOrder(c, dir, run, []string{"1_a.sql", "20260101000000_ts.sql"}, []string{"3__x.sql"})

	got := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "20260101000000", "3"})
}

// TestCompatAtlasLinearityByVersionString_FailurePath refuses a file that
// sorts before an applied file by name. 10_b.sql added after 1_a.sql is
// refused by the pinned community binary v1.3.0 as well. 10_b.sql added after
// 9_a.sql is refused here and nowhere else: the binary neither applies nor
// refuses it, so it never runs and the apply exits 0.
func TestCompatAtlasLinearityByVersionString_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		first   []string
		applied []string
	}{
		{name: "after 1_a.sql", first: []string{"1_a.sql"}, applied: []string{"1"}},
		{name: "after 1_a.sql and 2_b.sql", first: []string{"1_a.sql", "2_b.sql"}, applied: []string{"1", "2"}},
		{name: "after 9_a.sql", first: []string{"9_a.sql"}, applied: []string{"9"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			run := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
			prepareVersionOrder(c, dir, run, tt.first, []string{"10_b.sql"})

			got := run(append([]string{"migrate", "apply"}, versionOrderTarget...)...)

			c.Assert(got.ExitCode, qt.Equals, 1)
			c.Assert(got.Stderr, qt.Contains, "migration file 10_b.sql was added out of order.")
			c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, tt.applied)
		})
	}
}

// TestCompatAtlasLinearityMatchesTheCommunityBinaryE2E runs the pinned
// community binary and ptah-compat on the same history and holds the status
// report, the apply refusal and the revision table to the community ones.
// Available is left out: the binary omits it while a file is out of order,
// and ptah-compat keeps listing the directory.
func TestCompatAtlasLinearityMatchesTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name  string
		first []string
		added []string
	}{
		{name: "string order above the mark", first: []string{"1_a.sql", "20260101000000_ts.sql"}, added: []string{"3__x.sql"}},
		{name: "name order below an applied file", first: []string{"1_a.sql"}, added: []string{"10_b.sql"}},
		{name: "a number above the current version", first: []string{"1_a.sql", "2_b.sql"}, added: []string{"10_b.sql"}},
		{name: "one late and one pending", first: []string{"1_a.sql", "2_b.sql"}, added: []string{"10_c.sql", "3_d.sql"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			communityDir, compatDir := c.TempDir(), c.TempDir()
			community := referenceVersionOrderRunner(c, communityDir, reference, environmentWithoutPtahVariables())
			compat := compatVersionOrderRunner(c, compatDir, environmentWithoutPtahVariables())
			prepareVersionOrder(c, communityDir, community, tt.first, tt.added)
			prepareVersionOrder(c, compatDir, compat, tt.first, tt.added)

			status := append([]string{"migrate", "status"}, versionOrderTarget...)
			c.Assert(compat(status...).Stdout, qt.Equals, community(status...).Stdout)
			formatted := append(slices.Clone(status), "--format", versionOrderStatusTemplate)
			c.Assert(compat(formatted...).Stdout, qt.Equals, community(formatted...).Stdout)
			apply := append([]string{"migrate", "apply"}, versionOrderTarget...)
			communityApply, compatApply := community(apply...), compat(apply...)
			c.Assert(compatApply.ExitCode, qt.Equals, communityApply.ExitCode)
			c.Assert(firstLine(compatApply.Stderr), qt.Equals, firstLine(communityApply.Stderr))
			c.Assert(revisionTableVersions(c, filepath.Join(compatDir, "app.db")), qt.DeepEquals,
				revisionTableVersions(c, filepath.Join(communityDir, "app.db")))
		})
	}
}

// TestNativeAtlasFormatsKeepNumericLinearityE2E runs the native binary over an
// Atlas directory and an Atlas revision table. The string rule belongs to
// ptah-compat, so the native run keeps the numeric one: 10_b.sql added after
// 1_a.sql and 2_b.sql applies, because 10 is above 2, where ptah-compat and
// the community binary refuse it (the "a number above the current version"
// row above).
func TestNativeAtlasFormatsKeepNumericLinearityE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	hash := compatVersionOrderRunner(c, dir, environmentWithoutPtahVariables())
	up := []string{
		"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations",
		"--dir-format", "atlas", "--revision-format", "atlas",
	}
	native := func() clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, up...)
	}
	for _, name := range []string{"1_a.sql", "2_b.sql"} {
		writeVersionOrderFiles(c, dir, hash, []string{name})
		applied := native()
		c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	}
	writeVersionOrderFiles(c, dir, hash, []string{"10_b.sql"})

	got := native()

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "10", "2"})
}

// executedAtShape is what a status template prints for one revision row
// written at +09:00: the JSON time with its offset, then the time's String
// form, whose zone is the bare offset rather than a zone name.
const executedAtShape = `^"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?\+09:00" \d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d+)? \+0900 \+0900$`

// executedAtStoredShape is how the SQLite revision table spells executed_at.
const executedAtStoredShape = `^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d+)?\+09:00$`

const executedAtTemplate = `{{ range .Applied }}{{ json .ExecutedAt }} {{ .ExecutedAt }}{{ end }}`

// readExecutedAt reads the text atlas_schema_revisions stores for the one row.
func readExecutedAt(c *qt.C, path string) string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var executedAt string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT CAST(executed_at AS TEXT) FROM atlas_schema_revisions").Scan(&executedAt), qt.IsNil)
	return executedAt
}

// observeExecutedAt applies one file under TZ=Asia/Tokyo and reads the row
// back under TZ=UTC, so the zone the row prints is the writer's.
func observeExecutedAt(c *qt.C, dir string, runner func(env []string) versionOrderRun) (stored, printed string) {
	c.Helper()
	writer := runner(append(environmentWithoutPtahVariables(), "TZ=Asia/Tokyo"))
	reader := runner(append(environmentWithoutPtahVariables(), "TZ=UTC"))
	writeVersionOrderFiles(c, dir, writer, []string{"1_a.sql"})
	applied := writer(append([]string{"migrate", "apply"}, versionOrderTarget...)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	status := reader(append(append([]string{"migrate", "status"}, versionOrderTarget...), "--format", executedAtTemplate)...)
	c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", status.Stderr))
	return readExecutedAt(c, filepath.Join(dir, "app.db")), status.Stdout
}

// TestCompatAtlasExecutedAtKeepsTheWriterOffsetE2E holds executed_at to what
// the pinned community binary v1.3.0 writes on SQLite: the writer's local time
// with its offset, printed back with that offset whatever zone reads it.
func TestCompatAtlasExecutedAtKeepsTheWriterOffsetE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	stored, printed := observeExecutedAt(c, dir, func(env []string) versionOrderRun {
		return compatVersionOrderRunner(c, dir, env)
	})

	c.Assert(stored, qt.Matches, executedAtStoredShape)
	c.Assert(printed, qt.Matches, executedAtShape)
}

// TestCompatAtlasExecutedAtMatchesTheCommunityBinaryE2E runs the same
// observation on the pinned community binary, so the shapes above are its.
func TestCompatAtlasExecutedAtMatchesTheCommunityBinaryE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	dir := c.TempDir()

	stored, printed := observeExecutedAt(c, dir, func(env []string) versionOrderRun {
		return referenceVersionOrderRunner(c, dir, reference, env)
	})

	c.Assert(stored, qt.Matches, executedAtStoredShape)
	c.Assert(printed, qt.Matches, executedAtShape)
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}
