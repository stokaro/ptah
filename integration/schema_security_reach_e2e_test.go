//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// reachReport is the part of the `schema security` JSON report a reach
// finding fills.
type reachReport struct {
	Findings []reachReportFinding `json:"findings"`
}

type reachReportFinding struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Subject  struct {
		Name string `json:"name"`
	} `json:"subject"`
	Detail struct {
		Reach *struct {
			Reached   int      `json:"reached"`
			Relations int      `json:"relations"`
			Through   []string `json:"through"`
			Sources   []string `json:"sources"`
		} `json:"reach"`
	} `json:"detail"`
}

// reachFindingLines runs `ptah schema security` against dbURL and states, one
// line each, the reach findings about the roles whose names end in suffix. The
// run lowers the error threshold to 75, since a reach is never an error by
// default and the lines would show one severity only.
// Roles are server-wide, so the run also measures every other test's roles;
// the suffix keeps this test's own, and is taken off so the lines read the
// same on every run.
func reachFindingLines(c *qt.C, dbURL, suffix string) []string {
	c.Helper()
	stdout, err := runPtahNativeStdout("schema", "security", "--db-url", dbURL, "--format", "json", "--fail-on", "none",
		"--reach-error", "75")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
	var report reachReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	findings := slices.DeleteFunc(report.Findings, func(finding reachReportFinding) bool {
		return finding.Detail.Reach == nil || !strings.HasSuffix(finding.Subject.Name, suffix)
	})
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		reach := finding.Detail.Reach
		line := fmt.Sprintf("%s %s %s %d/%d through=%v sources=%v", finding.Code, finding.Severity,
			finding.Subject.Name, reach.Reached, reach.Relations, reach.Through, reach.Sources)
		lines = append(lines, strings.ReplaceAll(line, suffix, ""))
	}
	return lines
}

// TestSchemaSecurityReachE2E_PostgreSQL builds a role graph over a database of
// four tables and reads back how much of it each role reaches: through its own
// grants, through a role it is a member of, through pg_read_all_data, which no
// grant on any table states, and through owning three of the tables. One role
// holds pg_read_all_data only by switching to it, and a member that can only
// inherit from that role reaches nothing; a privilege on one column reaches
// no table.
func TestSchemaSecurityReachE2E_PostgreSQL(t *testing.T) {
	c := qt.New(t)
	target, admin := scratchReplayDatabase(c)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	reader, member, auditor, writer := "reach_reader"+suffix, "reach_member"+suffix, "reach_auditor"+suffix, "reach_writer"+suffix
	owner, switcher, heir := "reach_owner"+suffix, "reach_switcher"+suffix, "reach_heir"+suffix
	c.Cleanup(func() {
		// This runs before scratchReplayDatabase drops the database, so the
		// privileges that keep the roles from being dropped go first.
		conn, err := dbschema.ConnectToDatabase(context.Background(), target)
		c.Check(err, qt.IsNil)
		_, err = conn.ExecContext(context.Background(), fmt.Sprintf(`DROP OWNED BY %s, %s, %s`, reader, writer, owner))
		c.Check(err, qt.IsNil)
		dbschema.CloseAndWarn(conn)
		_, err = admin.ExecContext(context.Background(),
			fmt.Sprintf(`DROP ROLE IF EXISTS %s, %s, %s, %s, %s, %s, %s`, member, reader, auditor, writer, owner, heir, switcher))
		c.Check(err, qt.IsNil)
	})
	_, err := admin.ExecContext(c.Context(), fmt.Sprintf(`
		CREATE ROLE %[1]s NOLOGIN;
		CREATE ROLE %[2]s NOLOGIN;
		CREATE ROLE %[3]s NOLOGIN;
		CREATE ROLE %[4]s NOLOGIN;
		CREATE ROLE %[5]s NOLOGIN;
		CREATE ROLE %[6]s NOLOGIN;
		CREATE ROLE %[7]s NOLOGIN;
		GRANT %[1]s TO %[2]s;
		GRANT pg_read_all_data TO %[3]s;
		GRANT pg_read_all_data TO %[6]s WITH INHERIT FALSE;
		GRANT %[6]s TO %[7]s WITH SET FALSE;`, reader, member, auditor, writer, owner, switcher, heir))
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), fmt.Sprintf(`
		CREATE TABLE t1 (id int PRIMARY KEY);
		CREATE TABLE t2 (id int PRIMARY KEY);
		CREATE TABLE t3 (id int PRIMARY KEY);
		CREATE TABLE t4 (id int PRIMARY KEY);
		GRANT SELECT ON t1, t2, t3 TO %[1]s;
		GRANT SELECT (id) ON t4 TO %[1]s;
		GRANT INSERT, UPDATE ON t1, t2, t3, t4 TO %[2]s;
		ALTER TABLE t1 OWNER TO %[3]s;
		ALTER TABLE t2 OWNER TO %[3]s;
		ALTER TABLE t3 OWNER TO %[3]s;`, reader, writer, owner))
	c.Assert(err, qt.IsNil)

	c.Assert(reachFindingLines(c, target, suffix), qt.DeepEquals, []string{
		"roles/read-reach error reach_auditor 4/4 through=[] sources=[pg_read_all_data held by reach_auditor]",
		"roles/read-reach warning reach_member 3/4 through=[reach_reader] sources=[]",
		"roles/read-reach warning reach_owner 3/4 through=[] sources=[ownership of 3 relations held by reach_owner]",
		"roles/read-reach warning reach_reader 3/4 through=[] sources=[]",
		"roles/read-reach error reach_switcher 4/4 through=[] sources=[pg_read_all_data held by reach_switcher]",
		"roles/write-reach warning reach_owner 3/4 through=[] sources=[ownership of 3 relations held by reach_owner]",
		"roles/write-reach error reach_writer 4/4 through=[] sources=[]",
	})
}

// TestSchemaSecurityReachE2E_MySQLFamily builds the same graph on MySQL and
// MariaDB, where the access no table grant states is a privilege on every
// database, and a grant on the database reaches each table in it.
func TestSchemaSecurityReachE2E_MySQLFamily(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer admin.Close()
			stamp := time.Now().UnixNano()
			suffix := fmt.Sprintf("_%d", stamp)
			database := fmt.Sprintf("ptah_reach_%d", stamp)
			// MySQL caps a role name at 32 characters, and the suffix takes 20.
			reader, member, global, schemaWriter := "rd"+suffix, "mb"+suffix, "gl"+suffix, "sw"+suffix
			createMySQLDatabase(c, c.Context(), admin, database)
			defer dropMySQLDatabase(c, context.Background(), admin, database)
			defer func() {
				_, dropErr := admin.ExecContext(context.Background(),
					fmt.Sprintf("DROP ROLE IF EXISTS %s, %s, %s, %s", member, reader, global, schemaWriter))
				c.Check(dropErr, qt.IsNil)
			}()
			for _, statement := range []string{
				fmt.Sprintf("CREATE ROLE %s, %s, %s, %s", reader, member, global, schemaWriter),
				fmt.Sprintf("CREATE TABLE `%s`.t1 (id int PRIMARY KEY)", database),
				fmt.Sprintf("CREATE TABLE `%s`.t2 (id int PRIMARY KEY)", database),
				fmt.Sprintf("CREATE TABLE `%s`.t3 (id int PRIMARY KEY)", database),
				fmt.Sprintf("CREATE TABLE `%s`.t4 (id int PRIMARY KEY)", database),
				fmt.Sprintf("GRANT SELECT ON `%s`.t1 TO %s", database, reader),
				fmt.Sprintf("GRANT SELECT ON `%s`.t2 TO %s", database, reader),
				fmt.Sprintf("GRANT SELECT ON `%s`.t3 TO %s", database, reader),
				fmt.Sprintf("GRANT %s TO %s", reader, member),
				fmt.Sprintf("GRANT SELECT ON *.* TO %s", global),
				fmt.Sprintf("GRANT INSERT ON `%s`.* TO %s", database, schemaWriter),
			} {
				_, err = admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			target := replaceMySQLDatabaseName(c, dbtarget.URL(c, test.engine), database)

			c.Assert(reachFindingLines(c, target, suffix), qt.DeepEquals, []string{
				"roles/read-reach error gl 4/4 through=[] sources=[global privileges held by gl]",
				"roles/read-reach warning mb 3/4 through=[rd] sources=[]",
				"roles/read-reach warning rd 3/4 through=[] sources=[]",
				"roles/write-reach error sw 4/4 through=[] sources=[]",
			})
		})
	}
}
