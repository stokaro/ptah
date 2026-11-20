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

type targetReport struct {
	Target string `json:"target"`
	vulnerability.Report
	ScanError string `json:"scan_error"`
}

type document struct {
	Targets []targetReport `json:"targets"`
}

func run(args ...string) (stdoutText, stderrText string, err error) {
	cmd := dbvulnerabilities.NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func decode(c *qt.C, stdout string) document {
	var doc document
	c.Assert(json.Unmarshal([]byte(stdout), &doc), qt.IsNil)
	return doc
}

func sqliteURL(c *qt.C) string {
	return "sqlite://" + filepath.Join(c.TempDir(), "scan.db")
}

func TestCommand_UnsupportedProductRemainsIncomplete(t *testing.T) {
	c := qt.New(t)
	url := sqliteURL(c)
	stdout, stderr, err := run("--db-url", url, "--format", "json")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Equals, "")
	doc := decode(c, stdout)
	c.Assert(doc.Targets, qt.HasLen, 1)
	c.Assert(doc.Targets[0].Target, qt.Equals, url)
	c.Assert(doc.Targets[0].ScanError, qt.Equals, "")
	c.Assert(doc.Targets[0].Inventory.Product, qt.Equals, "sqlite")
	c.Assert(doc.Targets[0].Source.Status, qt.Equals, "unavailable")
	c.Assert(doc.Targets[0].Applicability, qt.Equals, "unassessed")
	c.Assert(doc.Targets[0].IsIncomplete(), qt.IsTrue)
}

func TestCommand_FailOnNoneReportsTheGap(t *testing.T) {
	c := qt.New(t)
	url := sqliteURL(c)
	stdout, _, err := run("--db-url", url, "--format", "json", "--fail-on", "none")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "engine does not declare server vulnerability scanning")
	stdout, _, err = run("--db-url", url, "--fail-on", "none")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Target: "+url+"\n")
	c.Assert(stdout, qt.Contains, "Snapshot SHA-256:")
}

func TestCommand_ManagedDeploymentDoesNotClaimServerFindings(t *testing.T) {
	c := qt.New(t)
	stdout, stderr, err := run("--db-url", sqliteURL(c), "--format", "json", "--deployment", "managed")
	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	doc := decode(c, stdout)
	c.Assert(doc.Targets, qt.HasLen, 1)
	c.Assert(doc.Targets[0].Applicability, qt.Equals, "not_applicable")
	c.Assert(doc.Targets[0].Source.Status, qt.Equals, "not_assessed")
	c.Assert(doc.Targets[0].HasFindings(), qt.IsFalse)
	c.Assert(doc.Targets[0].IsIncomplete(), qt.IsFalse)
}

// TestCommand_EveryTargetIsReported holds the multi-target contract: a target
// that cannot be reached is written with its scan error beside the targets
// that were assessed, and the command exits 2 whatever --fail-on says.
func TestCommand_EveryTargetIsReported(t *testing.T) {
	c := qt.New(t)
	url := sqliteURL(c)
	stdout, stderr, err := run(
		"--db-url", url, "--db-url", "postgres://user:secret@127.0.0.1:1/app",
		"--format", "json", "--fail-on", "none")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout+stderr+err.Error(), qt.Not(qt.Contains), "secret")
	doc := decode(c, stdout)
	c.Assert(doc.Targets, qt.HasLen, 2)
	c.Assert(doc.Targets[0].Target, qt.Equals, url)
	c.Assert(doc.Targets[0].ScanError, qt.Equals, "")
	c.Assert(doc.Targets[0].Inventory.Product, qt.Equals, "sqlite")
	c.Assert(doc.Targets[1].ScanError, qt.Equals, "database connection failed")
	c.Assert(doc.Targets[1].Applicability, qt.Equals, "unassessed")
	c.Assert(doc.Targets[1].Source.Status, qt.Equals, "not_assessed")
	c.Assert(doc.Targets[1].IsIncomplete(), qt.IsTrue)
	c.Assert(doc.Targets[1].Target, qt.Contains, "127.0.0.1:1/app")
}

func TestCommand_MissingAdvisoriesFailRegardlessOfPolicy(t *testing.T) {
	c := qt.New(t)
	stdout, stderr, err := run(
		"--db-url", "sqlite:///:memory:", "--db-url", sqliteURL(c), "--format", "json", "--fail-on", "none",
		"--advisories", filepath.Join(c.TempDir(), "missing.json"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "read advisory snapshot")
	doc := decode(c, stdout)
	c.Assert(doc.Targets, qt.HasLen, 2)
	c.Assert(doc.Targets[0].Source.Status, qt.Equals, "invalid")
	c.Assert(doc.Targets[0].ScanError, qt.Equals, "advisory snapshot is unavailable or invalid")
	c.Assert(doc.Targets[1].Source.Status, qt.Equals, "invalid")
	c.Assert(doc.Targets[1].ScanError, qt.Equals, "advisory snapshot is unavailable or invalid")
}

func TestCommand_RedactsFailedConnection(t *testing.T) {
	c := qt.New(t)
	stdout, stderr, err := run("--db-url", "postgres://user:secret@127.0.0.1:1/app", "--format", "json")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout+stderr+err.Error(), qt.Not(qt.Contains), "secret")
	c.Assert(stdout, qt.Contains, "database connection failed")
	stdout, stderr, err = run("--db-url", "postgres://user:secret@127.0.0.1:1/app")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout+stderr+err.Error(), qt.Not(qt.Contains), "secret")
	c.Assert(stdout, qt.Contains, "SCAN ERROR: database connection failed\n")
}

func TestCommand_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "no database URL", args: nil, want: "--db-url is required and must not be empty"},
		{name: "empty database URL", args: []string{"--db-url", ""}, want: "--db-url is required and must not be empty"},
		{name: "unknown format", args: []string{"--db-url", "sqlite:///:memory:", "--format", "yaml"}, want: "--format must be table or json"},
		{name: "unknown deployment", args: []string{"--db-url", "sqlite:///:memory:", "--deployment", "other"}, want: "--deployment must be self-managed or managed"},
		{name: "unknown floor", args: []string{"--db-url", "sqlite:///:memory:", "--min-severity", "severe"}, want: `--min-severity: unknown severity "severe"`},
		{name: "unknown threshold", args: []string{"--db-url", "sqlite:///:memory:", "--fail-severity", "High"}, want: `--fail-severity: unknown severity "High"`},
		{name: "malformed ignore", args: []string{"--db-url", "sqlite:///:memory:", "--ignore", "GHSA-1234"}, want: `--ignore: invalid or repeated ignored advisory ID "GHSA-1234"`},
		{name: "repeated ignore", args: []string{"--db-url", "sqlite:///:memory:", "--ignore", "CVE-2026-9547", "--ignore", "CVE-2026-9547"}, want: `--ignore: invalid or repeated ignored advisory ID "CVE-2026-9547"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			stdout, stderr, err := run(tt.args...)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(stderr, qt.Contains, tt.want)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
