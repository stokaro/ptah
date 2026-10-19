package sqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlreach"
)

// Each statement reaches outside the database through a construct only some
// grammars give a meaning, and the dialect's descriptor says this one does:
// the calls it lists in its Reach section, or Oracle's alternative quoting in
// its Lexical section. The refusal names the construct the descriptor names.
func TestScan_RefusesWhatTheDescriptorStates(t *testing.T) {
	const postgresReach = "controls the server or its replication state rather than reading from it, " +
		"and no transaction undoes that"
	postgresControl := sqlreach.Finding{Construct: "PostgreSQL server control function", Reach: postgresReach}
	tests := []struct {
		name      string
		dialect   string
		statement string
		want      sqlreach.Finding
	}{
		{name: "a ClickHouse table function", dialect: "clickhouse",
			statement: "SELECT count() FROM url('http://169.254.169.254/', 'CSV')",
			want: sqlreach.Finding{
				Construct: "ClickHouse remote table function",
				Reach:     "reads over the network or from the host filesystem inside a query",
			}},
		{name: "the Cluster variant of a ClickHouse table function", dialect: "ch",
			statement: "SELECT * FROM s3Cluster('c', 'https://bucket/x')",
			want: sqlreach.Finding{
				Construct: "ClickHouse remote table function",
				Reach:     "reads over the network or from the host filesystem inside a query",
			}},
		{name: "an Oracle network package", dialect: "oracle",
			statement: "SELECT UTL_HTTP.REQUEST('http://169.254.169.254/') FROM dual",
			want: sqlreach.Finding{
				Construct: "Oracle network package",
				Reach:     "sends a request from the database server, which no transaction retracts",
			}},
		{name: "an Oracle network type called directly", dialect: "oracledb",
			statement: "SELECT HTTPURITYPE('http://169.254.169.254/').getclob() FROM dual",
			want: sqlreach.Finding{
				Construct: "Oracle network package",
				Reach:     "sends a request from the database server, which no transaction retracts",
			}},
		{name: "an Oracle server control package", dialect: "oracle",
			statement: "BEGIN DBMS_JOB.SUBMIT(j, 'NULL;'); END;",
			want: sqlreach.Finding{
				Construct: "Oracle server control package",
				Reach:     "changes instance state that outlives the statement, which no transaction undoes",
			}},
		{name: "an Oracle alternative-quoted literal", dialect: "oracle",
			statement: "SELECT q'[it's]' FROM dual",
			want: sqlreach.Finding{
				Construct: "Oracle alternative-quoted literal",
				Reach:     "hides its contents from this scanner, so what the statement runs cannot be judged",
			}},
		{name: "a PostgreSQL control function", dialect: "postgres",
			statement: "SELECT pg_terminate_backend(42)", want: postgresControl},
		{name: "a PostgreSQL replication function", dialect: "postgres",
			statement: "SELECT pg_create_logical_replication_slot('s', 'pgoutput')", want: postgresControl},
		{name: "a control function on CockroachDB", dialect: "cockroachdb",
			statement: "SELECT pg_cancel_backend(42)", want: postgresControl},
		{name: "a control function on YugabyteDB", dialect: "yugabytedb",
			statement: "SELECT set_config('search_path', 'x', false)", want: postgresControl},
		{name: "a replication function on Spanner", dialect: "spanner",
			statement: "SELECT pg_drop_replication_slot('s')", want: postgresControl},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			finding, found, err := sqlreach.Scan(test.statement, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(finding, qt.DeepEquals, test.want)
		})
	}
}

// The same statements on an engine whose descriptor states none of these
// constructs are ordinary: url, UTL_HTTP and pg_terminate_backend are user
// function or package names there, and q is a column.
func TestScan_DescribedConstructsStayOnTheirEngines(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
	}{
		{name: "a ClickHouse table function on PostgreSQL", dialect: "postgres",
			statement: "SELECT count(*) FROM url('http://169.254.169.254/', 'CSV')"},
		{name: "an Oracle network package on MySQL", dialect: "mysql",
			statement: "SELECT UTL_HTTP.REQUEST('x')"},
		{name: "an Oracle server control package on SQL Server", dialect: "mssql",
			statement: "SELECT DBMS_JOB.SUBMIT('x')"},
		{name: "an alternative-quoted literal on PostgreSQL", dialect: "postgres",
			statement: "SELECT q'[x]'"},
		{name: "a PostgreSQL control function on MySQL", dialect: "mysql",
			statement: "SELECT pg_terminate_backend(42)"},
		{name: "a PostgreSQL replication function on ClickHouse", dialect: "clickhouse",
			statement: "SELECT pg_drop_replication_slot('s')"},
		{name: "a ClickHouse table function with no dialect", dialect: "",
			statement: "SELECT * FROM url('http://x', 'CSV')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			finding, found, err := sqlreach.Scan(test.statement, test.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(finding, qt.DeepEquals, sqlreach.Finding{})
		})
	}
}

// TestDialectUsesBackslashEscapes_AgreesWithTheSwitchItReplaces pins the
// default escape reading for every spelling the process registry accepts
// against the switch it replaces, which answered true for MySQL, MariaDB,
// ClickHouse and YDB, and false for every other dialect and for none.
func TestDialectUsesBackslashEscapes_AgreesWithTheSwitchItReplaces(t *testing.T) {
	escaping := map[string]bool{platform.MySQL: true, platform.MariaDB: true, platform.ClickHouse: true, platform.YDB: true}
	for _, spelling := range append(platform.DialectSpellings(), "", "nonesuch") {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			got := sqlreach.DialectUsesBackslashEscapes(spelling)
			c.Assert(got, qt.Equals, escaping[platform.NormalizeDialect(spelling)])
		})
	}
}
