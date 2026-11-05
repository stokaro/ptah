//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// What an HCL schema inspection cannot carry is reported once per run, with
// how many details it concerns, on both binaries, as the run reports its
// other warnings about many objects. Written one line per object, a database
// with a few roles that hold grants printed a line for each grant whose
// grantor HCL cannot name, and under PTAH_LOG_FORMAT=json those lines were
// plain text between JSON records.

// grantsDatabase is a scratch database on the PostgreSQL test server holding a
// table and the two roles granted on it, each with a grantor HCL cannot name,
// as every grant the server records has one. The roles are server objects, so
// they are dropped after the database.
func grantsDatabase(c *qt.C) (url string, roles []string) {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	suffix := time.Now().UnixNano() % 1_000_000_000
	roles = []string{fmt.Sprintf("ptah_warn_reader_%d", suffix), fmt.Sprintf("ptah_warn_writer_%d", suffix)}
	c.Cleanup(func() {
		for _, role := range roles {
			_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	for _, role := range roles {
		_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN")
		c.Assert(err, qt.IsNil)
	}
	url = databaseBuiltFrom(c, "CREATE TABLE notes (id integer);"+
		" GRANT SELECT ON notes TO "+quoteE2EIdent(roles[0])+";"+
		" GRANT INSERT ON notes TO "+quoteE2EIdent(roles[1])+";")
	return url, roles
}

// hclLossSummary matches the one warning a run writes for the details HCL
// leaves out.
const hclLossSummary = `^warning: \d+ details of the inspected schema cannot be represented in HCL,` +
	` and the document leaves them out; PTAH_LOG_LEVEL=debug lists them$`

// TestSchemaInspectReportsHCLLossesOnceE2E runs schema inspect on both
// binaries against a database whose grants carry grantors: the details HCL
// leaves out are one warning, and on the Atlas-compatible surface the
// extension block it omits for the community CLI keeps its own line, with the
// variable that restores it.
func TestSchemaInspectReportsHCLLossesOnceE2E(t *testing.T) {
	c := qt.New(t)
	url, _ := grantsDatabase(c)

	tests := []struct {
		name   string
		target clirun.Target
		args   []string
		want   []string
	}{
		{
			name:   "ptah",
			target: clirun.Ptah,
			args:   []string{"schema", "inspect", "--db-url", url},
			want:   []string{hclLossSummary},
		},
		{
			name:   "ptah-compat",
			target: clirun.Compat,
			args:   []string{"schema", "inspect", "-u", url},
			want: []string{
				`^warning: extensions\.plpgsql: omitted from Atlas-compatible schema inspect output: .*` +
					`; set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models$`,
				hclLossSummary,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := clirun.Run(c, test.target, clirun.Options{Dir: c.TempDir()}, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
			warnings := warningLines(got.Stderr)
			c.Assert(warnings, qt.HasLen, len(test.want), qt.Commentf("stderr:\n%s", got.Stderr))
			for i, want := range test.want {
				c.Assert(warnings[i], qt.Matches, want)
			}
		})
	}
}

// TestSchemaInspectListsHCLLossesAsJSONRecordsE2E runs the same inspection
// with PTAH_LOG_FORMAT=json and PTAH_LOG_LEVEL=debug: no warning is a text
// line, the details are listed one record each at debug level, the grants of
// both roles among them, and the warning is one record counting exactly the
// details listed.
func TestSchemaInspectListsHCLLossesAsJSONRecordsE2E(t *testing.T) {
	c := qt.New(t)
	url, roles := grantsDatabase(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{
		Dir: c.TempDir(),
		Env: append(os.Environ(), "PTAH_LOG_FORMAT=json", "PTAH_LOG_LEVEL=debug"),
	}, "schema", "inspect", "--db-url", url)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	listed, warnings := hclLossRecords(c, got.Stderr)
	c.Assert(warningLines(got.Stderr), qt.HasLen, 0)
	c.Assert(listed, qt.Contains, "grants."+roles[0])
	c.Assert(listed, qt.Contains, "grants."+roles[1])
	c.Assert(slices.Compact(slices.Sorted(slices.Values(listed))), qt.HasLen, len(listed))
	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0]["count"], qt.Equals, float64(len(listed)))
}

// hclLossRecords reads the JSON records of stderr and returns the paths of the
// HCL losses listed at debug level and the warnings about them. A line that is
// not a record is left to the caller: a server holding roles the inspected
// schema does not refer to gets a note about them in text.
func hclLossRecords(c *qt.C, stderr string) (listed []string, warnings []map[string]any) {
	c.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(stderr), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var record map[string]any
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("%s", line))
		if record["category"] != "hcl-loss" {
			continue
		}
		switch record["level"] {
		case "DEBUG":
			path, _ := record["path"].(string)
			listed = append(listed, path)
		case "WARN":
			warnings = append(warnings, record)
		}
	}
	return listed, warnings
}
