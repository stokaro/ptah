//go:build integration

package integration_test

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

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
		Reach *reachReportDetail `json:"reach"`
	} `json:"detail"`
}

// reachReportDetail is the reach a finding states.
type reachReportDetail struct {
	Reached   int      `json:"reached"`
	Relations int      `json:"relations"`
	Through   []string `json:"through"`
	Sources   []string `json:"sources"`
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
				"roles/read-reach error gl 4/4 through=[] sources=[SELECT ON *.* held by gl]",
				"roles/read-reach warning mb 3/4 through=[rd] sources=[]",
				"roles/read-reach warning rd 3/4 through=[] sources=[]",
				"roles/write-reach error sw 4/4 through=[] sources=[]",
			})
		})
	}
}

// TestSchemaSecurityReachE2E_MySQLFamilyPatternGrants grants on database name
// patterns and reads back how much of the database each role reaches. A
// session of each role says what the server applies: the expected findings
// are built from what it reads and writes in t1 and t2, and the test states
// only which grant a finding names when one is due.
//
// The server applies one database-level row of all a session holds, so a
// pattern counts only where no other row wins. The rows cover a pattern that
// reaches the database for reading and, through a "_" that stands for another
// character, for writing; patterns that need other characters, or the "_"
// escaped; the database's own row beside a pattern, holding INSERT or only
// EVENT, which shadows the pattern; a role whose two granted roles hold the
// pattern and the own row; and two patterns against each other, where MySQL
// applies the one first in byte order and MariaDB the one that matches fewer
// names, so the two engines answer differently.
func TestSchemaSecurityReachE2E_MySQLFamilyPatternGrants(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newPatternReachRun(c, engine.admin, "abxc")
			roles := []patternReachRole{
				{short: "pt", grants: []string{"SELECT ON `%sab%%`.*"}, readSources: "[SELECT ON `prab%`.* held by pt]"},
				{short: "pw", grants: []string{"INSERT ON `%sab_c`.*"}, writeSources: "[INSERT ON `prab_c`.* held by pw]"},
				{short: "pe", grants: []string{"SELECT ON `%sab\\_c`.*"}, readSources: "[SELECT ON `prab\\_c`.* held by pe]"},
				{short: "pn", grants: []string{"SELECT ON `%sxy%%`.*"}, readSources: "[SELECT ON `prxy%`.* held by pn]"},
				{
					short: "qs1", grants: []string{"SELECT ON `%sab%%`.*", "INSERT ON `%sabxc`.*"},
					readSources: "[SELECT ON `prab%`.* held by qs1]", writeSources: "[]",
				},
				{short: "qe", grants: []string{"SELECT ON `%sab%%`.*", "EVENT ON `%sabxc`.*"}, readSources: "[SELECT ON `prab%`.* held by qe]"},
				{short: "qr1", grants: []string{"SELECT ON `%sab%%`.*"}, readSources: "[SELECT ON `prab%`.* held by qr1]"},
				{short: "qr2", grants: []string{"INSERT ON `%sabxc`.*"}, writeSources: "[]"},
				{
					short: "qm", members: []string{"qr1", "qr2"}, through: "[qr1 qr2]",
					readSources: "[SELECT ON `prab%`.* held by qr1]", writeSources: "[]",
				},
				{
					short: "pp1", grants: []string{"SELECT ON `%sa%%`.*", "INSERT ON `%sab%%`.*"},
					readSources: "[SELECT ON `pra%`.* held by pp1]", writeSources: "[INSERT ON `prab%`.* held by pp1]",
				},
				{
					short: "pp2", grants: []string{"SELECT ON `%sab%%c`.*", "INSERT ON `%sa_x%%`.*"},
					readSources: "[SELECT ON `prab%c`.* held by pp2]", writeSources: "[INSERT ON `pra_x%`.* held by pp2]",
				},
			}
			run.grant(c, roles)

			got := run.findings(c)
			slices.Sort(got)

			c.Assert(got, qt.DeepEquals, run.expectedReach(c, roles))
			c.Assert(got, qt.Not(qt.HasLen), 0)
		})
	}
}

// TestSchemaSecurityReachE2E_MySQLFamilyWildcardDatabaseName reads a database
// whose name holds "_", so a grant on the database's own name is a pattern to
// the server too, and the one-row choice decides whether it applies: MySQL
// tries `pra%` first, byte for byte, and MariaDB the own name, which matches
// fewer names. The name with the "_" escaped matches the database alone and
// wins on both. As in the test above, the expected findings are built from
// what each role's session reads and writes.
func TestSchemaSecurityReachE2E_MySQLFamilyWildcardDatabaseName(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newPatternReachRun(c, engine.admin, "a_xc")
			roles := []patternReachRole{
				{
					short: "wu1", grants: []string{"SELECT ON `%sa%%`.*", "INSERT ON `%sa_xc`.*"},
					readSources: "[SELECT ON `pra%`.* held by wu1]", writeSources: "[]",
				},
				{short: "wu2", grants: []string{"INSERT ON `%sa_xc`.*"}, writeSources: "[]"},
				{
					short: "wu3", grants: []string{"SELECT ON `%sa\\_xc`.*", "INSERT ON `%sa_xc`.*"},
					readSources: "[SELECT ON `pra\\_xc`.* held by wu3]", writeSources: "[]",
				},
			}
			run.grant(c, roles)

			got := run.findings(c)
			slices.Sort(got)

			c.Assert(got, qt.DeepEquals, run.expectedReach(c, roles))
			c.Assert(got, qt.Not(qt.HasLen), 0)
		})
	}
}

// TestSchemaSecurityReachE2E_MariaDBEqualPatternsCountEveryRow grants two
// roles the same two patterns of equal weight, `pr%_xc` and `pr_%xc`, one for
// reading and one for writing. MariaDB orders such rows by an unstable sort,
// so a session applies one of them and which one is not known in advance: the
// finding counts both, and each role's session reads both tables or writes
// both, never both and never neither.
func TestSchemaSecurityReachE2E_MariaDBEqualPatternsCountEveryRow(t *testing.T) {
	c := qt.New(t)
	run := newPatternReachRun(c, dbtarget.MariaDBAdmin, "abxc")
	run.grant(c, []patternReachRole{
		{short: "uo1", grants: []string{"SELECT ON `%s%%_xc`.*", "INSERT ON `%s_%%xc`.*"}},
		{short: "uo2", grants: []string{"SELECT ON `%s%%_xc`.*", "INSERT ON `%s_%%xc`.*"}},
	})

	got := run.findings(c)
	slices.Sort(got)
	read1, write1 := run.sessionReach(c, "uo1"+run.suffix)
	read2, write2 := run.sessionReach(c, "uo2"+run.suffix)
	c.Logf("sessions: uo1 reads %d and writes %d, uo2 reads %d and writes %d", read1, write1, read2, write2)

	c.Assert(got, qt.DeepEquals, []string{
		"roles/read-reach error uo1 2/2 through=[] sources=[SELECT ON `pr%_xc`.* held by uo1]",
		"roles/read-reach error uo2 2/2 through=[] sources=[SELECT ON `pr%_xc`.* held by uo2]",
		"roles/write-reach error uo1 2/2 through=[] sources=[INSERT ON `pr_%xc`.* held by uo1]",
		"roles/write-reach error uo2 2/2 through=[] sources=[INSERT ON `pr_%xc`.* held by uo2]",
	})
	eitherRow := [][2]int{{2, 0}, {0, 2}}
	c.Assert(eitherRow, qt.Contains, [2]int{read1, write1})
	c.Assert(eitherRow, qt.Contains, [2]int{read2, write2})
}

// TestSchemaSecurityReachE2E_MySQLPatternGrantThroughAMembership reads a role
// granted a role that holds a grant on a database name pattern: it reaches what
// the pattern reaches, through the role, and the finding names the grant and
// the role that holds it. A session of the member says it reaches it.
//
// MariaDB is left out. Its privilege views list what a role holds through a
// granted role as the role's own row, so the read reports the member as
// holding the pattern grant too, and the finding names it held by both roles.
// Measured on MariaDB 11.8.9 and 12.3.3, SHOW GRANTS lists no such grant for
// the member, and REVOKE of it from the member fails with Error 1141. The reach
// counts the same relations either way.
func TestSchemaSecurityReachE2E_MySQLPatternGrantThroughAMembership(t *testing.T) {
	c := qt.New(t)
	run := newPatternReachRun(c, dbtarget.MySQLAdmin, "abxc")
	roles := []patternReachRole{
		{short: "pt", grants: []string{"SELECT ON `%sab%%`.*"}, readSources: "[SELECT ON `prab%`.* held by pt]"},
		{short: "pm", members: []string{"pt"}, through: "[pt]", readSources: "[SELECT ON `prab%`.* held by pt]"},
	}
	run.grant(c, roles)

	got := run.findings(c)
	slices.Sort(got)

	c.Assert(got, qt.DeepEquals, run.expectedReach(c, roles))
	c.Assert(got, qt.HasLen, 2)
}

// errTableAccessDenied is the error a server answers a statement on a table
// the session holds no privilege on with: ER_TABLEACCESS_DENIED_ERROR.
const errTableAccessDenied = 1142

// patternReachRole is a role of a pattern reach test: the grants it holds,
// each a privilege and its object with %s where the database's prefix goes,
// and the roles granted to it, named as the test names them. through and the
// sources are what a finding states when the role's session reaches every
// table, under the rule the sources belong to; "[]" is the default for
// through.
type patternReachRole struct {
	short                     string
	grants, members           []string
	through                   string
	readSources, writeSources string
}

// patternReachRun is a database on one server, holding tables t1 and t2, and
// the suffix that every role and pattern of the test carries. The patterns
// and the database's name begin with prefix, which holds no "%" or "_", so a
// grant on the database's own name is a pattern to the server only where the
// rest of the name makes it one.
type patternReachRun struct {
	admin    *sql.DB
	engine   dbtarget.Engine
	suffix   string
	prefix   string
	database string
}

// newPatternReachRun creates the database prefix and tail on engine's server.
// It is dropped when the test ends.
func newPatternReachRun(c *qt.C, engine dbtarget.Engine, tail string) patternReachRun {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	suffix := fmt.Sprintf("x%d", time.Now().UnixNano())
	run := patternReachRun{admin: admin, engine: engine, suffix: suffix, prefix: "pr" + suffix}
	run.database = run.prefix + tail
	createMySQLDatabase(c, c.Context(), admin, run.database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), admin, run.database) })
	run.exec(c,
		fmt.Sprintf("CREATE TABLE `%s`.t1 (id int PRIMARY KEY)", run.database),
		fmt.Sprintf("CREATE TABLE `%s`.t2 (id int PRIMARY KEY)", run.database),
	)
	return run
}

// grant creates each of roles and gives it its grants and its granted roles,
// in order, so a role that others are granted comes first. FLUSH PRIVILEGES
// follows, which MariaDB needs before a new session sees what a granted role
// passes on.
func (r patternReachRun) grant(c *qt.C, roles []patternReachRole) {
	c.Helper()
	for _, role := range roles {
		name := r.role(c, role.short)
		for _, grant := range role.grants {
			r.exec(c, "GRANT "+fmt.Sprintf(grant, r.prefix)+" TO "+name)
		}
		for _, member := range role.members {
			r.exec(c, "GRANT "+member+r.suffix+" TO "+name)
		}
	}
	r.exec(c, "FLUSH PRIVILEGES")
}

// role creates a role named name and the suffix, and returns its name. MySQL
// caps a role name at 32 characters, and the suffix takes 20, so the user a
// session creates, the role's name and "_u", fits beside a name of 10. The role is
// dropped when the test ends, and its grants with it.
func (r patternReachRun) role(c *qt.C, name string) string {
	c.Helper()
	role := name + r.suffix
	r.exec(c, "CREATE ROLE "+role)
	c.Cleanup(func() {
		_, err := r.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	return role
}

// exec runs each of statements as the administrator.
func (r patternReachRun) exec(c *qt.C, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := r.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// findings are the reach findings `ptah schema security` reports on the
// database about the test's roles, as [reachFindingLines] states them.
func (r patternReachRun) findings(c *qt.C) []string {
	c.Helper()
	return reachFindingLines(c, replaceMySQLDatabaseName(c, dbtarget.URL(c, r.engine), r.database), r.suffix)
}

// expectedReach is the finding each role's session says is due, sorted as
// sorted findings are: one per rule under which the session reaches both
// tables, which is above the error threshold, and none where it reaches
// neither. A session that reaches one table is a line no finding matches,
// since a database-level row reaches both or neither.
func (r patternReachRun) expectedReach(c *qt.C, roles []patternReachRole) []string {
	c.Helper()
	lines := make([]string, 0, 2*len(roles))
	for _, role := range roles {
		read, write := r.sessionReach(c, role.short+r.suffix)
		through := cmp.Or(role.through, "[]")
		lines = append(lines,
			fmt.Sprintf("roles/read-reach error %s %d/2 through=%s sources=%s", role.short, read, through, role.readSources),
			fmt.Sprintf("roles/write-reach error %s %d/2 through=%s sources=%s", role.short, write, through, role.writeSources),
		)
	}
	lines = slices.DeleteFunc(lines, func(line string) bool { return strings.Contains(line, " 0/2 ") })
	slices.Sort(lines)
	return lines
}

// sessionReach is how many of t1 and t2 a session of role reads and how many
// it inserts into. A statement the server refuses for want of a privilege
// counts as not reached, and any other failure fails the test.
func (r patternReachRun) sessionReach(c *qt.C, role string) (read, write int) {
	c.Helper()
	session := r.session(c, role)
	reached := map[bool]int{true: 1}
	for _, table := range []string{"t1", "t2"} {
		_, err := session.ExecContext(c.Context(), readProbe(r.database, table))
		c.Assert(err == nil || refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%s reading %s: %v", role, table, err))
		read += reached[err == nil]
		_, err = session.ExecContext(c.Context(), writeProbe(r.database, table))
		c.Assert(err == nil || refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%s writing %s: %v", role, table, err))
		write += reached[err == nil]
	}
	return read, write
}

// readProbe reads every row of table in database, which the test created
// under names holding no backtick.
func readProbe(database, table string) string {
	return "SELECT id FROM `" + database + "`.`" + table + "`"
}

// writeProbe inserts one row into table in database, or nothing where the row
// is already there.
func writeProbe(database, table string) string {
	return "INSERT IGNORE INTO `" + database + "`.`" + table + "` VALUES (1)"
}

// refusedForPrivilege reports whether err is the server refusing a statement
// on a table for want of a privilege.
func refusedForPrivilege(err error) bool {
	var refused *mysqldriver.MySQLError
	return errors.As(err, &refused) && refused.Number == errTableAccessDenied
}

// session is a session of a user role is granted to, with the role active, on
// no database: what it may do is what the role gives, as the server reads its
// grants. The user is dropped when the test ends.
func (r patternReachRun) session(c *qt.C, role string) *sql.Conn {
	c.Helper()
	user := role + "_u"
	// #nosec G202 -- the user, the role and the password are names this test generates.
	r.exec(c, "CREATE USER "+user+" IDENTIFIED BY 'reach_member_pw'", "GRANT "+role+" TO "+user)
	c.Cleanup(func() {
		_, err := r.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+user)
		c.Check(err, qt.IsNil)
	})
	config, err := mysqldriver.ParseDSN(dbtarget.DriverDSN(c, r.engine))
	c.Assert(err, qt.IsNil)
	config.User, config.Passwd, config.DBName = user, "reach_member_pw", ""
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	conn, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err = conn.ExecContext(c.Context(), "SET ROLE "+role)
	c.Assert(err, qt.IsNil)
	return conn
}
