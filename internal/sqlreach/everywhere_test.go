package sqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlreach"
)

// everyDialect is every spelling the process registry accepts, an empty
// dialect and one no engine claims.
func everyDialect() []string {
	return append([]string{"", "nonesuch"}, platform.DialectSpellings()...)
}

// everywhereStatements use one construct of descriptor.Reach.Everywhere
// each, one per construct the built-in engines state, and the construct and
// the words the refusal names, as the rules of this package held them.
var everywhereStatements = []struct {
	statement string
	want      sqlreach.Finding
}{
	{"ATTACH DATABASE '/tmp/x.db' AS x", sqlreach.Finding{Construct: "ATTACH",
		Reach: "attaches another SQLite database file to the session, so the statement can write to databases other than the dev database"}},
	{"DETACH DATABASE x", sqlreach.Finding{Construct: "DETACH",
		Reach: "manipulates the session's attached SQLite databases rather than the dev database schema"}},
	{"VACUUM INTO '/tmp/copy.db'", sqlreach.Finding{Construct: "VACUUM INTO",
		Reach: "writes a copy of the database to an arbitrary path on the host filesystem"}},
	{"PRAGMA temp_store_directory = '/tmp'", sqlreach.Finding{Construct: "PRAGMA temp_store_directory",
		Reach: "redirects SQLite temporary storage to an arbitrary directory on the host filesystem"}},
	{"PRAGMA main.data_store_directory = '/tmp'", sqlreach.Finding{Construct: "PRAGMA data_store_directory",
		Reach: "redirects SQLite database storage to an arbitrary directory on the host filesystem"}},
	{"SELECT load_extension('/tmp/x.so')", sqlreach.Finding{Construct: "load_extension",
		Reach: "loads a native extension module from the host filesystem"}},
	{"LOAD DATA INFILE '/etc/passwd' INTO TABLE t", sqlreach.Finding{Construct: "LOAD DATA INFILE",
		Reach: "reads a file from the database server or the client host"}},
	{"SELECT a FROM t INTO OUTFILE '/tmp/x'", sqlreach.Finding{Construct: "SELECT ... INTO OUTFILE",
		Reach: "writes a file on the database server host"}},
	{"SELECT a FROM t INTO DUMPFILE '/tmp/x'", sqlreach.Finding{Construct: "SELECT ... INTO DUMPFILE",
		Reach: "writes a file on the database server host"}},
	{"SELECT LOAD_FILE('/etc/passwd')", sqlreach.Finding{Construct: "LOAD_FILE",
		Reach: "reads a file on the database server host"}},
	{"CREATE TABLE t (id int) ENGINE=FEDERATED", sqlreach.Finding{Construct: "ENGINE=FEDERATED",
		Reach: "stores the table on another database server reached over the network"}},
	{"CREATE TABLE t (id int) ENGINE = MySQL('h:3306', 'db', 't', 'u', 'p')", sqlreach.Finding{Construct: "ClickHouse remote table engine",
		Reach: "stores or reads the table over the network or from the host filesystem"}},
	{"CREATE SERVER s FOREIGN DATA WRAPPER w", sqlreach.Finding{Construct: "CREATE SERVER",
		Reach: "defines a connection to another database server"}},
	{"INSTALL PLUGIN p SONAME 'p.so'", sqlreach.Finding{Construct: "INSTALL PLUGIN",
		Reach: "loads a server-side plugin, which runs native code on the database server host"}},
	{"INSTALL COMPONENT 'file://component_x'", sqlreach.Finding{Construct: "INSTALL COMPONENT",
		Reach: "loads a server-side component, which runs native code on the database server host"}},
	{"CREATE TABLE t (id int) DATA DIRECTORY = '/tmp'", sqlreach.Finding{Construct: "DATA DIRECTORY",
		Reach: "places table data at an arbitrary path on the database server host"}},
	{"CREATE TABLE t (id int) INDEX DIRECTORY '/tmp'", sqlreach.Finding{Construct: "INDEX DIRECTORY",
		Reach: "places index data at an arbitrary path on the database server host"}},
	{"COPY t FROM PROGRAM 'id'", sqlreach.Finding{Construct: "COPY ... PROGRAM",
		Reach: "executes a shell command on the database server host"}},
	{"COPY t TO '/tmp/out.csv'", sqlreach.Finding{Construct: "COPY with a file path",
		Reach: "reads or writes a file on the database server host"}},
	{"SELECT dblink_exec('host=x', 'DROP TABLE t')", sqlreach.Finding{Construct: "dblink",
		Reach: "opens a connection to another database server"}},
	{"CREATE EXTENSION postgres_fdw", sqlreach.Finding{Construct: "postgres_fdw",
		Reach: "federates data from another database server"}},
	{"CREATE EXTENSION file_fdw", sqlreach.Finding{Construct: "file_fdw",
		Reach: "exposes files on the database server host as tables"}},
	{"SELECT pg_read_file('/etc/passwd')", sqlreach.Finding{Construct: "pg_read_file",
		Reach: "reads a file on the database server host"}},
	{"SELECT pg_read_binary_file('/etc/passwd')", sqlreach.Finding{Construct: "pg_read_binary_file",
		Reach: "reads a file on the database server host"}},
	{"SELECT pg_file_unlink('/tmp/x')", sqlreach.Finding{Construct: "adminpack file function",
		Reach: "writes, renames or removes a file on the database server host"}},
	{"SELECT pg_ls_waldir()", sqlreach.Finding{Construct: "pg_ls_ directory listing",
		Reach: "lists a directory on the database server host"}},
	{"SELECT lo_import('/etc/passwd')", sqlreach.Finding{Construct: "lo_import",
		Reach: "reads a file on the database server host"}},
	{"SELECT lo_export(1, '/tmp/x')", sqlreach.Finding{Construct: "lo_export",
		Reach: "writes a file on the database server host"}},
	{"BULK INSERT t FROM '/tmp/x.csv'", sqlreach.Finding{Construct: "BULK INSERT",
		Reach: "reads a file on the database server host"}},
	{"EXEC xp_cmdshell 'id'", sqlreach.Finding{Construct: "xp_cmdshell",
		Reach: "executes a shell command on the database server host"}},
	{"EXECUTE xp_dirtree '/'", sqlreach.Finding{Construct: "xp_dirtree",
		Reach: "lists directories on the database server host"}},
	{"CALL sp_addlinkedserver('remote')", sqlreach.Finding{Construct: "sp_addlinkedserver",
		Reach: "defines a connection to another database server"}},
	{"SELECT * FROM OPENQUERY(remote, 'SELECT 1')", sqlreach.Finding{Construct: "OPENQUERY",
		Reach: "runs a query on a linked server, which is another database server"}},
	{"SELECT * FROM OPENROWSET(BULK '/tmp/x', SINGLE_CLOB) AS x", sqlreach.Finding{Construct: "OPENROWSET",
		Reach: "reads from another data source or a file on the database server host"}},
	{"SELECT * FROM OPENDATASOURCE('SQLNCLI', 'Data Source=x').db.dbo.t", sqlreach.Finding{Construct: "OPENDATASOURCE",
		Reach: "reads from another data source reached over the network"}},
}

// TestScan_RefusesEveryEngineConstructOnEveryDialect asks every dialect about
// each construct an engine states for every engine: the construct is refused
// whatever dialect the statement was written for, an empty and an unknown one
// included, and the refusal names the construct its engine states.
func TestScan_RefusesEveryEngineConstructOnEveryDialect(t *testing.T) {
	for _, dialect := range everyDialect() {
		for _, test := range everywhereStatements {
			t.Run(dialect+"/"+test.want.Construct, func(t *testing.T) {
				c := qt.New(t)
				finding, found, err := sqlreach.Scan(test.statement, dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				c.Assert(finding, qt.DeepEquals, test.want)
			})
		}
	}
}

// Each statement is one part short of a construct, or has a part in a place
// the part does not read, and every dialect accepts it. A construct whose
// parts are asked one at a time, or a part that matched any token, would
// refuse it.
func TestScan_AcceptsWhatNoConstructMatches(t *testing.T) {
	statements := map[string]string{
		"a statement with no part":                         "SELECT 1",
		"the start of VACUUM INTO without INTO":            "VACUUM",
		"INTO without the start of VACUUM INTO":            "INSERT INTO t VALUES (1)",
		"COPY with no string after FROM or TO":             "COPY t FROM STDIN",
		"a string after FROM in no COPY":                   "SELECT * FROM 'x'",
		"a table named like a call":                        "CREATE TABLE lo_import (id int)",
		"a call prefix as a column":                        "SELECT dblink_status FROM jobs",
		"a procedure name as a column":                     "SELECT xp_cmdshell FROM settings",
		"a pragma name with no PRAGMA":                     "SELECT temp_store_directory FROM settings",
		"INTO OUTFILE naming a table":                      "SELECT * INTO outfile FROM src",
		"an engine name with no ENGINE":                    "SELECT federated FROM engines",
		"a path option name with no path":                  "CREATE INDEX directory ON t (data)",
		"the first keyword of a statement start elsewhere": "SELECT attach FROM files",
	}
	for _, dialect := range everyDialect() {
		for name, statement := range statements {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				finding, found, err := sqlreach.Scan(statement, dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsFalse)
				c.Assert(finding, qt.DeepEquals, sqlreach.Finding{})
			})
		}
	}
}

// A statement that uses constructs of two engines names the one the registry
// asks first, whatever dialect it was written for. SQLite's descriptor comes
// before SQL Server's, so ATTACH is named ahead of xp_cmdshell. PostgreSQL's
// comes first of all, so pg_read_file is named ahead of ATTACH.
func TestScan_NamesTheFirstConstructInRegistryOrder(t *testing.T) {
	tests := []struct{ name, statement, want string }{
		{name: "SQLite before SQL Server", statement: "ATTACH DATABASE '/tmp/x.db' AS x; EXEC xp_cmdshell 'id'", want: "ATTACH"},
		{name: "PostgreSQL before SQLite", statement: "ATTACH DATABASE '/tmp/x.db' AS x; SELECT pg_read_file('/etc/passwd')", want: "pg_read_file"},
	}
	for _, dialect := range []string{platform.SQLite, platform.Postgres, platform.SQLServer, ""} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				finding, found, err := sqlreach.Scan(test.statement, dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				c.Assert(finding.Construct, qt.Equals, test.want)
			})
		}
	}
}
