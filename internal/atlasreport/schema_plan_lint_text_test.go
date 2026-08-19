package atlasreport_test

import (
	"bytes"
	"regexp"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// planLintAnalysis analyzes one block of SQL the way a plan file's SQL is
// analyzed: one Atlas-layout source, the compatibility profile, no version
// selection.
func planLintAnalysis(c *qt.C, sql string) migrationlint.Analysis {
	c.Helper()
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"20060102150405_plan.sql": {Data: []byte(sql)},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		Dialect:       "postgres",
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// planLintElapsedLine matches the report's elapsed-duration line, the one line
// whose content depends on how long the machine took. Dropping it is what lets
// the assertions below compare the whole document rather than pick at it with
// substring checks, which would pass on a report missing everything they do not
// name.
var planLintElapsedLine = regexp.MustCompile(`(?m)^ {2}-- \d+(\.\d+)?(ns|µs|ms|s|m[0-9.]+s)\n`)

// writePlanLintReport renders a report with the elapsed line removed.
func writePlanLintReport(c *qt.C, sql string) string {
	c.Helper()
	analysis := planLintAnalysis(c, sql)
	var out bytes.Buffer
	c.Assert(atlasreport.WriteSchemaPlanLintText(&out, atlasreport.SchemaPlanLintOptions{
		Analysis: &analysis,
	}), qt.IsNil)
	rendered := out.String()
	c.Assert(planLintElapsedLine.MatchString(rendered), qt.IsTrue,
		qt.Commentf("no elapsed line to strip; the report shape changed:\n%s", rendered))
	return planLintElapsedLine.ReplaceAllString(rendered, "")
}

func TestWriteSchemaPlanLintText_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "a plan with nothing to report",
			sql:  "ALTER TABLE users ADD COLUMN nick text;\n",
			want: "Analyzing planned statements (1 in total):\n" +
				"\n" +
				"  -- no diagnostics found\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1 schema change\n",
		},
		{
			name: "one diagnostic carries a singular fix header",
			sql:  "DROP TABLE users;\n",
			want: "Analyzing planned statements (1 in total):\n" +
				"\n" +
				"  -- destructive changes detected:\n" +
				"    -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"  -- suggested fix:\n" +
				"    -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name: "two diagnostics carry a plural fix header and one group",
			sql:  "DROP TABLE users;\nDROP TABLE orders;\n",
			want: "Analyzing planned statements (2 in total):\n" +
				"\n" +
				"  -- destructive changes detected:\n" +
				"    -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- L2: Dropping table \"orders\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"  -- suggested fixes:\n" +
				"    -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"    -> Add a pre-migration check to ensure table \"orders\" is empty before dropping it\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// Destructive above data-dependent even though the data-dependent
			// statement comes first, which is the group order the migration
			// report already prints and the reason groupDiagnostics ranks
			// rather than following first appearance.
			name: "groups are ordered by analyzer, not by first appearance",
			sql:  "ALTER TABLE users ADD COLUMN nick text NOT NULL;\nDROP TABLE orders;\n",
			want: "Analyzing planned statements (2 in total):\n" +
				"\n" +
				"  -- destructive changes detected:\n" +
				"    -- L2: Dropping table \"orders\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"  -- data dependent changes detected:\n" +
				"    -- L1: Adding a non-nullable \"text\" column \"nick\" will fail in case table" +
				" \"users\" is not\n" +
				"       empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"  -- suggested fix:\n" +
				"    -> Add a pre-migration check to ensure table \"orders\" is empty before dropping it\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// A plan that executes SQL expressing no structural change prints
			// no schema-change line at all rather than "0 schema changes".
			name: "a plan expressing no schema change prints no change line",
			sql:  "INSERT INTO users (id) VALUES (1);\n",
			want: "Analyzing planned statements (1 in total):\n" +
				"\n" +
				"  -- no diagnostics found\n" +
				"\n" +
				"  -------------------------\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := writePlanLintReport(c, test.sql)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestWriteSchemaPlanLintText_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		opts    atlasreport.SchemaPlanLintOptions
		wantErr string
	}{
		{
			name:    "no analysis to render",
			opts:    atlasreport.SchemaPlanLintOptions{},
			wantErr: "plan lint analysis is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteSchemaPlanLintText(&out, test.opts)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
