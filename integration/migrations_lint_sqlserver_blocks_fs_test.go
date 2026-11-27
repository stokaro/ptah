//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrationsLintSplitsATSQLProcedureWhereItEndsFS keeps a transaction,
// TRY/CATCH and END CONVERSATION inside a procedure, then reports the DROP
// TABLE in the batch after it. GO ends the stored definition even when the
// procedure's body has a BEGIN ... END block of its own.
func TestMigrationsLintSplitsATSQLProcedureWhereItEndsFS(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want []string
	}{
		{name: "TRY and CATCH", body: "BEGIN TRY\n    UPDATE t SET c = 1;\n  END TRY\n  BEGIN CATCH\n    SELECT 1;\n  END CATCH;",
			want: []string{"coverage/routine-body@1", "destructive/drop-table@11 users", "rollout/drop-table@11 users"}},
		{name: "a transaction", body: "BEGIN TRAN;\n  UPDATE t SET c = 1;\n  COMMIT;",
			want: []string{"coverage/routine-body@1", "destructive/drop-table@8 users", "rollout/drop-table@8 users"}},
		{name: "a DROP TABLE after END CONVERSATION is body text", body: "END CONVERSATION @h;\n  DROP TABLE audit;",
			want: []string{"coverage/routine-body@1", "destructive/drop-table@7 users", "rollout/drop-table@7 users"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			sql := fmt.Sprintf("CREATE PROCEDURE p AS\nBEGIN\n  %s\nEND;\nGO\nDROP TABLE users;\n", test.body)
			c.Assert(os.WriteFile(filepath.Join(dir, "0001_init.up.sql"), []byte(sql), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, _ := runPtahNativeWithError("migrations", "lint", "--dir", dir, "--dialect", "sqlserver",
				"--format", "json", "--fail-on", "none")

			c.Assert(lintedFindings(c, out), qt.DeepEquals, test.want)
		})
	}
}

// lintedFindings lists the findings of a `migrations lint --format json` run
// as "RULE@line", with the dropped table where the finding names one.
func lintedFindings(c *qt.C, out string) []string {
	c.Helper()
	var report struct {
		Findings []struct {
			Rule    string `json:"rule"`
			Line    int    `json:"line"`
			Context struct {
				Subjects []struct {
					Name string `json:"name"`
				} `json:"subjects"`
			} `json:"context"`
		} `json:"findings"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	listed := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		entry := fmt.Sprintf("%s@%d", finding.Rule, finding.Line)
		for _, subject := range finding.Context.Subjects {
			entry += " " + subject.Name
		}
		listed = append(listed, entry)
	}
	return listed
}
