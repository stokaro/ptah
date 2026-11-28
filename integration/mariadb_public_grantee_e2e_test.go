//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mariaDBPublicEngines are the MariaDB lines CI runs, each reached with an
// account that may grant to PUBLIC and create roles and databases.
var mariaDBPublicEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mariadb-12.3", engine: dbtarget.MariaDBAdmin},
	{name: "mariadb-11.8", engine: dbtarget.MariaDB118},
}

// TestMariaDBPublicCheckpointAppliesE2E leaves a PUBLIC row in MariaDB's
// account tables, which a grant to PUBLIC creates and its revoke keeps, then
// checkpoints a migration directory on a shadow database of that server and
// applies the checkpoint to a fresh database. PUBLIC is the grantee every
// account holds and no role: CREATE ROLE PUBLIC is Error 1959, so a
// checkpoint that declares it cannot be applied.
func TestMariaDBPublicCheckpointAppliesE2E(t *testing.T) {
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, row.engine)
			granted := server.database(c, "public_row")
			for _, statement := range []string{
				"CREATE TABLE " + granteeIdentifier(granted) + ".items (id int PRIMARY KEY)",
				"GRANT SELECT ON " + granteeIdentifier(granted) + ".items TO PUBLIC",
				"REVOKE SELECT ON " + granteeIdentifier(granted) + ".items FROM PUBLIC",
			} {
				_, err := server.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.up.sql"), []byte("CREATE TABLE items (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.down.sql"), []byte("DROP TABLE items;\n"), 0o600), qt.IsNil)
			shadow := server.url("mariadb", server.database(c, "public_shadow"))
			applied := server.database(c, "public_target")
			target := server.url("mariadb", applied)

			checkpoint := runPtahIn(c, "migrations", "checkpoint", "--shadow-db", shadow, "--migrations-dir", dir)
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir)

			c.Assert(checkpoint.ExitCode, qt.Equals, 0, historyComment(checkpoint))
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(server.tables(c, applied), qt.Equals, "items,schema_migrations,schema_migrations_log")
		})
	}
}

// TestMariaDBPublicReachE2E grants a table to PUBLIC, and to PUBLIC a role
// holding the database, on MariaDB, where a login granted nothing reads what
// both give: measured on MariaDB 11.8.9 and 12.3.3, and refused it with Error
// 1142 once the grant is revoked. `schema security` reports PUBLIC as
// reaching both tables, through the role, and never as a role that reaches
// nobody. The role reaches no more than PUBLIC and is not reported.
func TestMariaDBPublicReachE2E(t *testing.T) {
	for _, row := range mariaDBPublicEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, row.engine)
			database := server.database(c, "public_reach")
			suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
			held := "pubheld" + suffix
			c.Cleanup(func() {
				for _, statement := range []string{
					"REVOKE SELECT ON " + granteeIdentifier(database) + ".direct FROM PUBLIC",
					"REVOKE " + granteeIdentifier(held) + " FROM PUBLIC",
					"DROP ROLE IF EXISTS " + granteeIdentifier(held),
				} {
					_, err := server.admin.ExecContext(context.Background(), statement)
					c.Check(err, qt.IsNil, qt.Commentf("%s", statement))
				}
			})
			for _, statement := range []string{
				"CREATE TABLE " + granteeIdentifier(database) + ".direct (id int PRIMARY KEY)",
				"CREATE TABLE " + granteeIdentifier(database) + ".through (id int PRIMARY KEY)",
				"CREATE ROLE " + granteeIdentifier(held),
				"GRANT SELECT ON " + granteeIdentifier(database) + ".* TO " + granteeIdentifier(held),
				"GRANT " + granteeIdentifier(held) + " TO PUBLIC",
				"GRANT SELECT ON " + granteeIdentifier(database) + ".direct TO PUBLIC",
			} {
				_, err := server.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			target := server.url("mariadb", database)

			printed, err := runPtahNativeStdout("db", "read", "--db-url", target)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", printed))
			c.Assert(printed, qt.Not(qt.Contains), "ROLE IF NOT EXISTS `PUBLIC`")
			c.Assert(printed, qt.Contains, "GRANT SELECT ON `"+database+"`.`direct` TO `PUBLIC`;")
			c.Assert(publicFindingLines(c, target, suffix), qt.DeepEquals, []string{
				"roles/read-reach error PUBLIC 2/2 through=[pubheld]",
			})
		})
	}
}

// publicFindingLines runs `ptah schema security` against dbURL and states the
// ROL04 and reach findings about PUBLIC and about the roles whose names end in
// suffix, one line each, with the suffix taken off.
func publicFindingLines(c *qt.C, dbURL, suffix string) []string {
	c.Helper()
	stdout, err := runPtahNativeStdout("schema", "security", "--db-url", dbURL, "--format", "json", "--fail-on", "none",
		"--reach-error", "75")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
	var report reachReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	findings := slices.DeleteFunc(report.Findings, func(finding reachReportFinding) bool {
		return !strings.EqualFold(finding.Subject.Name, "PUBLIC") && !strings.HasSuffix(finding.Subject.Name, suffix) ||
			finding.Code != "ROL04" && !strings.HasPrefix(finding.Code, "roles/")
	})
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		line := finding.Code + " " + finding.Severity + " " + finding.Subject.Name + reachText(finding.Detail.Reach)
		lines = append(lines, strings.ReplaceAll(line, suffix, ""))
	}
	return lines
}

// reachText states a finding's reach after its subject, and nothing for a
// finding that states none.
func reachText(reach *reachReportDetail) string {
	if reach == nil {
		return ""
	}
	return fmt.Sprintf(" %d/%d through=%v", reach.Reached, reach.Relations, reach.Through)
}
