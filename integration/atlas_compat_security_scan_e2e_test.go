//go:build integration

package integration_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/vulnerability"
)

// compatSecurityScanReport is the part of the JSON report these tests read.
type compatSecurityScanReport struct {
	Targets []struct {
		ScanError string `json:"scan_error"`
		Inventory struct {
			Product      string `json:"product"`
			ServerBanner string `json:"server_banner"`
		} `json:"inventory"`
		Source struct {
			URL    string `json:"url"`
			Origin struct {
				Kind     string `json:"kind"`
				Fallback string `json:"fallback"`
			} `json:"origin"`
		} `json:"source"`
		Assessments []json.RawMessage `json:"assessments"`
	} `json:"targets"`
}

// compatSecurityScanEnvironment is the environment of a ptah-compat run with
// no PTAH_* variable but the ones given.
func compatSecurityScanEnvironment(variables ...string) []string {
	return append(environmentWithoutPtahVariables(), variables...)
}

// bundledAdvisories selects the advisory snapshots built into the binary, so
// a run makes no request and its result does not depend on the network.
const bundledAdvisories = "PTAH_ADVISORY_SOURCE=bundled"

// TestCompatSecurityScan_RoutesToTheNativeScanE2E runs the shipped
// ptah-compat with PTAH_ATLAS_SECURITY_SCAN against the MySQL server CI
// starts. The JSON report is the native scan's: the server version it names
// is the one the server reports when read directly, and it assesses every
// advisory of the bundled MySQL snapshot. PTAH_FAIL_ON=none keeps the
// permanent partial-coverage gap from deciding the exit code, and
// PTAH_ADVISORY_SOURCE reaches the forwarded scan: the report reads the
// bundled snapshot by choice, with no fallback, where the default source would
// fetch the snapshot or report why it could not.
func TestCompatSecurityScan_RoutesToTheNativeScanE2E(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(c, dbtarget.MySQL)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	banner := vulnerability.CollectIdentity(conn).ServerBanner
	snapshot, err := vulnerability.ReadVersionedSnapshot("mysql", "")
	c.Assert(err, qt.IsNil)

	result := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(),
		Env: compatSecurityScanEnvironment("PTAH_ATLAS_SECURITY_SCAN=1", "PTAH_FAIL_ON=none", bundledAdvisories),
	}, "security", "scan", "-u", url, "--format", "{{ json . }}")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stderr, qt.Equals, "")
	var report compatSecurityScanReport
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil)
	c.Assert(report.Targets, qt.HasLen, 1)
	c.Assert(report.Targets[0].ScanError, qt.Equals, "")
	c.Assert(report.Targets[0].Inventory.Product, qt.Equals, "mysql")
	c.Assert(report.Targets[0].Inventory.ServerBanner, qt.Equals, banner)
	c.Assert(report.Targets[0].Source.URL, qt.Equals, snapshot.Source)
	c.Assert(report.Targets[0].Source.Origin.Kind, qt.Equals, "bundled")
	c.Assert(report.Targets[0].Source.Origin.Fallback, qt.Equals, "")
	c.Assert(report.Targets[0].Assessments, qt.HasLen, len(snapshot.Advisories))
}

// TestCompatSecurityScan_ScansEveryURLE2E repeats -u with the MySQL server CI
// starts and a SQLite file: the report has a target for each, in the order
// given. Joined into one native --db-url value, the two URLs would be one URL
// that names neither database.
func TestCompatSecurityScan_ScansEveryURLE2E(t *testing.T) {
	c := qt.New(t)

	result := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(),
		Env: compatSecurityScanEnvironment("PTAH_ATLAS_SECURITY_SCAN=1", "PTAH_FAIL_ON=none", bundledAdvisories),
	}, "security", "scan", "-u", dbtarget.URL(c, dbtarget.MySQL), "--url", "sqlite://app.db", "--format", "{{ json . }}")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	var report compatSecurityScanReport
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil)
	c.Assert(report.Targets, qt.HasLen, 2)
	c.Assert(report.Targets[0].ScanError, qt.Equals, "")
	c.Assert(report.Targets[0].Inventory.Product, qt.Equals, "mysql")
	c.Assert(report.Targets[1].ScanError, qt.Equals, "")
	c.Assert(report.Targets[1].Inventory.Product, qt.Equals, "sqlite")
}

// TestCompatSecurityScan_NativeVariablesReachTheScanE2E runs the table
// report under the native policy variable. A MySQL report is always
// incomplete, so `incomplete` fails and `none` passes: the variable reaches
// the forwarded scan although this surface registers no --fail-on.
func TestCompatSecurityScan_NativeVariablesReachTheScanE2E(t *testing.T) {
	tests := []struct {
		name     string
		failOn   string
		wantExit int
	}{
		{name: "an incomplete gate fails", failOn: "incomplete", wantExit: 1},
		{name: "no gate passes", failOn: "none", wantExit: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := clirun.Run(c, clirun.Compat, clirun.Options{
				Dir: c.TempDir(),
				Env: compatSecurityScanEnvironment("PTAH_ATLAS_SECURITY_SCAN=1", "PTAH_FAIL_ON="+tt.failOn, bundledAdvisories),
			}, "security", "scan", "--url", dbtarget.URL(c, dbtarget.MySQL))

			c.Assert(result.ExitCode, qt.Equals, tt.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "Product: mysql\n")
		})
	}
}

// TestCompatSecurityScan_DefaultIsAnUnknownCommandE2E runs the same scan
// with the variable unset: the shipped binary answers as the community binary
// does and prints no report.
func TestCompatSecurityScan_DefaultIsAnUnknownCommandE2E(t *testing.T) {
	c := qt.New(t)

	result := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(),
		Env: compatSecurityScanEnvironment("PTAH_FAIL_ON=none"),
	}, "security", "scan", "-u", dbtarget.URL(c, dbtarget.MySQL), "--format", "{{ json . }}")

	c.Assert(result.ExitCode, qt.Equals, 1)
	c.Assert(result.Stdout, qt.Equals, "")
	c.Assert(result.Stderr, qt.Equals, "Error: unknown command \"security\" for \"atlas\"\nRun 'atlas --help' for usage.\n")
}

// TestCompatSecurityScan_StrictRefusesTheVariableE2E runs the scan under the
// strict profile with the variable enabled: the binary refuses the variable
// by name before any command runs.
func TestCompatSecurityScan_StrictRefusesTheVariableE2E(t *testing.T) {
	c := qt.New(t)

	result := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(),
		Env: compatSecurityScanEnvironment("PTAH_ATLAS_STRICT_COMPAT=1", "PTAH_ATLAS_SECURITY_SCAN=1"),
	}, "security", "scan", "-u", dbtarget.URL(c, dbtarget.MySQL))

	c.Assert(result.ExitCode, qt.Equals, 1)
	c.Assert(result.Stdout, qt.Equals, "")
	c.Assert(result.Stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_SECURITY_SCAN\n")
}
