package dbvulnerabilities_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/dbvulnerabilities"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/vulnerability"
)

func run(t *testing.T, args ...string) (stdoutText, stderrText string, err error) {
	t.Helper()
	cmd := dbvulnerabilities.NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func TestCommand_EmbeddedEngineIsNotApplicable(t *testing.T) {
	c := qt.New(t)
	url := "sqlite://" + filepath.Join(t.TempDir(), "scan.db")
	stdout, stderr, err := run(t, "--db-url", url, "--format", "json")
	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	var report vulnerability.Report
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Inventory.Product, qt.Equals, "sqlite")
	c.Assert(report.Source.Status, qt.Equals, "not_assessed")
	c.Assert(report.Applicability, qt.Equals, "not_applicable")
	c.Assert(report.IsIncomplete(), qt.IsFalse)
	stdout, _, err = run(t, "--db-url", url, "--format", "json", "--fail-on", "none")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "not_applicable")
	stdout, _, err = run(t, "--db-url", url, "--fail-on", "none")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Snapshot SHA-256:")
}

func TestCommand_ManagedDeploymentDoesNotClaimServerFindings(t *testing.T) {
	c := qt.New(t)
	url := "sqlite://" + filepath.Join(t.TempDir(), "scan.db")
	stdout, stderr, err := run(t, "--db-url", url, "--format", "json", "--deployment", "managed")
	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	var report vulnerability.Report
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Applicability, qt.Equals, "not_applicable")
	c.Assert(report.Source.Status, qt.Equals, "not_assessed")
	c.Assert(report.HasFindings(), qt.IsFalse)
	c.Assert(report.IsIncomplete(), qt.IsFalse)
	_, _, err = run(t, "--db-url", url, "--deployment", "other")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}

func TestCommand_MissingAdvisoriesFailRegardlessOfPolicy(t *testing.T) {
	c := qt.New(t)
	stdout, stderr, err := run(t,
		"--db-url", "sqlite:///:memory:", "--format", "json", "--fail-on", "none",
		"--advisories", filepath.Join(t.TempDir(), "missing.json"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "read advisory snapshot")
	var failure struct {
		Source struct {
			Status string `json:"status"`
		} `json:"source"`
		ScanError string `json:"scan_error"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &failure), qt.IsNil)
	c.Assert(failure.Source.Status, qt.Equals, "invalid")
	c.Assert(failure.ScanError, qt.Equals, "advisory snapshot is unavailable or invalid")
}

func TestCommand_RedactsFailedConnection(t *testing.T) {
	c := qt.New(t)
	stdout, stderr, err := run(t, "--db-url", "postgres://user:secret@127.0.0.1:1/app", "--format", "json")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout+stderr+err.Error(), qt.Not(qt.Contains), "secret")
	c.Assert(stdout, qt.Contains, "database connection failed")
}
