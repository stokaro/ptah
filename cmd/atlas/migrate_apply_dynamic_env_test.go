package atlas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
)

const dynamicEnvironmentProject = `env {
  for_each = toset([
    "sqlite://bar.db?_fk=1",
    "sqlite://foo.db?_fk=1",
  ])
  name = atlas.env
  url  = each.value
  migration {
    dir = "file://migrations"
  }
}
`

const dynamicEnvironmentProjectWithThirdTarget = `env {
  for_each = toset([
    "sqlite://bar.db?_fk=1",
    "sqlite://foo.db?_fk=1",
    "sqlite://qux.db?_fk=1",
  ])
  name = atlas.env
  url  = each.value
  migration {
    dir = "file://migrations"
  }
}
`

type dynamicApplyReport struct {
	URL struct {
		Host string
	}
	Applied []struct {
		Version string
	}
	Target  string
	Error   string
	Message string
}

func TestCompatMigrateApplyDynamicEnvironments_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeDynamicEnvironmentProject(c.TB)
	writeDynamicEnvironmentMigrations(c.TB)

	stdout, stderr, err := executeDynamicEnvironmentApply("1")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, "")
	c.Assert(sqliteTableCount(c.TB, filepath.Join(root, "bar.db"), "t1"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, filepath.Join(root, "foo.db"), "t1"), qt.Equals, 1)
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "bar.db"), "c1_unique"), qt.Equals, 0)
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "foo.db"), "c1_unique"), qt.Equals, 0)
	c.Assert(stdout, qt.Contains, "}\n{")
	c.Assert(bytes.Count([]byte(stdout), []byte{'\n'}), qt.Equals, 1)

	reports := decodeDynamicApplyReports(c.TB, stdout)
	c.Assert(reports[0].URL.Host, qt.Equals, "bar.db")
	c.Assert(reports[0].Target, qt.Equals, "20240112070806")
	c.Assert(reports[0].Applied, qt.HasLen, 1)
	c.Assert(reports[1].URL.Host, qt.Equals, "foo.db")
	c.Assert(reports[1].Target, qt.Equals, "20240112070806")
	c.Assert(reports[1].Applied, qt.HasLen, 1)
}

func TestCompatMigrateApplyDynamicEnvironments_PartialFailureAndRetry(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeDynamicEnvironmentProjectWithThirdTarget(c.TB)
	writeDynamicEnvironmentMigrations(c.TB)

	initialStdout, initialStderr, initialErr := executeDynamicEnvironmentApply("1")
	c.Assert(initialErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", initialStdout, initialStderr))
	c.Assert(initialStderr, qt.Equals, "")
	c.Assert(sqliteTableCount(c.TB, filepath.Join(root, "qux.db"), "t1"), qt.Equals, 1)
	insertDuplicateDynamicEnvironmentRows(c.TB, filepath.Join(root, "foo.db"))

	failureStdout, failureStderr, failureErr := executeDynamicEnvironmentApply()

	c.Assert(failureErr, qt.ErrorMatches, `(?s).*UNIQUE constraint failed: t1.c1.*`)
	c.Assert(failureStderr, qt.Equals, "")
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "bar.db"), "c1_unique"), qt.Equals, 1)
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "foo.db"), "c1_unique"), qt.Equals, 0)
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "qux.db"), "c1_unique"), qt.Equals, 0)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "bar.db")), qt.DeepEquals, []string{"20240112070806", "20240116003831"})
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "foo.db")), qt.DeepEquals, []string{"20240112070806"})
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "qux.db")), qt.DeepEquals, []string{"20240112070806"})
	c.Assert(failureStdout, qt.Contains, "}\n{")
	c.Assert(bytes.Count([]byte(failureStdout), []byte{'\n'}), qt.Equals, 1)
	failureReports := decodeDynamicApplyReports(c.TB, failureStdout)
	c.Assert(failureReports[0].URL.Host, qt.Equals, "bar.db")
	c.Assert(failureReports[0].Target, qt.Equals, "20240116003831")
	c.Assert(failureReports[0].Applied, qt.HasLen, 1)
	c.Assert(failureReports[0].Error, qt.Equals, "")
	c.Assert(failureReports[1].URL.Host, qt.Equals, "foo.db")
	c.Assert(failureReports[1].Target, qt.Equals, "20240116003831")
	c.Assert(failureReports[1].Applied, qt.HasLen, 1)
	c.Assert(failureReports[1].Error, qt.Contains, "UNIQUE constraint failed: t1.c1")

	retryStdout, retryStderr, retryErr := executeDynamicEnvironmentApply()

	c.Assert(retryErr, qt.ErrorMatches, `(?s).*UNIQUE constraint failed: t1.c1.*`)
	c.Assert(retryStderr, qt.Equals, "")
	c.Assert(retryStdout, qt.Contains, "}\n{")
	c.Assert(bytes.Count([]byte(retryStdout), []byte{'\n'}), qt.Equals, 1)
	retryReports := decodeDynamicApplyReports(c.TB, retryStdout)
	c.Assert(retryReports[0].URL.Host, qt.Equals, "bar.db")
	c.Assert(retryReports[0].Applied, qt.HasLen, 0)
	c.Assert(retryReports[0].Message, qt.Equals, "No migration files to execute")
	c.Assert(retryReports[1].URL.Host, qt.Equals, "foo.db")
	c.Assert(retryReports[1].Applied, qt.HasLen, 1, qt.Commentf("stdout:\n%s", retryStdout))
	c.Assert(retryReports[1].Error, qt.Contains, "UNIQUE constraint failed: t1.c1")
	c.Assert(sqliteIndexCount(c.TB, filepath.Join(root, "qux.db"), "c1_unique"), qt.Equals, 0)
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "bar.db")), qt.DeepEquals, []string{"20240112070806", "20240116003831"})
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "foo.db")), qt.DeepEquals, []string{"20240112070806"})
	c.Assert(sqliteAtlasRevisionVersions(c.TB, filepath.Join(root, "qux.db")), qt.DeepEquals, []string{"20240112070806"})
}

func writeDynamicEnvironmentProject(tb testing.TB) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile("atlas.hcl", []byte(dynamicEnvironmentProject), 0o600), qt.IsNil)
}

func writeDynamicEnvironmentProjectWithThirdTarget(tb testing.TB) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile("atlas.hcl", []byte(dynamicEnvironmentProjectWithThirdTarget), 0o600), qt.IsNil)
}

func writeDynamicEnvironmentMigrations(tb testing.TB) {
	c := qt.New(tb)
	c.Helper()
	writeAtlasApplyProjectMigration(c.TB, "migrations", "20240112070806.sql", "CREATE TABLE t1(c1 int);\n")
	writeAtlasApplyProjectMigration(c.TB, "migrations", "20240116003831.sql", "CREATE UNIQUE INDEX c1_unique ON t1(c1);\n")
	writeAtlasApplyProjectSum(c.TB, "migrations")
}

func executeDynamicEnvironmentApply(amount ...string) (stdoutText, stderrText string, err error) {
	args := []string{"migrate", "apply"}
	args = append(args, amount...)
	args = append(args, "--env", "local", "--format", "{{ json . }}")
	cmd := atlas.NewCompatCommand("atlas")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func decodeDynamicApplyReports(tb testing.TB, output string) []dynamicApplyReport {
	c := qt.New(tb)
	c.Helper()
	decoder := json.NewDecoder(bytes.NewBufferString(output))
	var first dynamicApplyReport
	var second dynamicApplyReport
	var excess dynamicApplyReport
	c.Assert(decoder.Decode(&first), qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(decoder.Decode(&second), qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(decoder.Decode(&excess), qt.ErrorIs, io.EOF, qt.Commentf("command output:\n%s", output))
	return []dynamicApplyReport{first, second}
}

func insertDuplicateDynamicEnvironmentRows(tb testing.TB, dbPath string) {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO t1(c1) VALUES (1), (1)")
	c.Assert(err, qt.IsNil)
}
