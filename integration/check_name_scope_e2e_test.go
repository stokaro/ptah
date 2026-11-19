//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemaprep"
)

// checkNameEngine is a server a schema file declaring two CHECKs of one name
// is applied to, with the statement that counts the tables the schemas here
// declare. Ptah's own lock table, which Oracle holds before the comparison,
// is not one of them.
type checkNameEngine struct {
	dialect string
	target  func(c *qt.C) string
	tables  string
}

var (
	checkNameMySQL = checkNameEngine{
		dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "check_names")
			return url
		},
		tables: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('t', 't1', 't2')",
	}
	checkNameMariaDB = checkNameEngine{
		dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "check_names")
			return url
		},
		tables: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('t', 't1', 't2')",
	}
	checkNamePostgres = checkNameEngine{
		dialect: platform.Postgres,
		target: func(c *qt.C) string {
			return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_check_names"), "public")
		},
		tables: "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('t', 't1', 't2')",
	}
	checkNameOracle = checkNameEngine{
		dialect: platform.Oracle,
		target:  oracleScratchURL,
		tables:  "SELECT count(*) FROM user_tables WHERE table_name IN ('T', 'T1', 'T2')",
	}
	// checkNameSQLServerCaseSensitive is a database whose collation tells ck
	// from CK, which the server's default collation does not.
	checkNameSQLServerCaseSensitive = checkNameEngine{
		dialect: platform.SQLServer,
		target:  sqlServerCaseSensitiveDatabase,
		tables:  "SELECT count(*) FROM sys.tables WHERE name IN ('t', 't1', 't2')",
	}
	checkNameSQLServer = checkNameEngine{
		dialect: platform.SQLServer,
		target:  func(c *qt.C) string { return sqlServerDevDatabase(c).url },
		tables:  "SELECT count(*) FROM sys.tables WHERE name IN ('t', 't1', 't2')",
	}
)

// sqlServerCaseSensitiveDatabase is a scratch database on SQL Server created
// with the Latin1_General_100_CS_AS collation.
func sqlServerCaseSensitiveDatabase(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.SQLServer)
	admin := connectDevDialect(c, adminURL)
	name := devDialectScratchName()
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE "+name+" COLLATE Latin1_General_100_CS_AS")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(),
			"ALTER DATABASE "+name+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+name)
		c.Check(dropErr, qt.IsNil)
	})
	return replaceSQLServerDatabase(c, adminURL, name)
}

// writeCheckNameSchema writes schema to a file of its own.
func writeCheckNameSchema(c *qt.C, schema string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(schema), 0o600), qt.IsNil)
	return path
}

// serverRefusal runs each statement of schema on the server at targetURL and
// answers the first line of the first error it returns.
func serverRefusal(c *qt.C, targetURL, dialect, schema string) string {
	c.Helper()
	conn := connectDevDialect(c, targetURL)
	for _, statement := range sqlutil.SplitSQLStatementsForDialect(schema, dialect) {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := conn.ExecContext(c.Context(), statement); err != nil {
			first, _, _ := strings.Cut(err.Error(), "\n")
			return first
		}
	}
	c.Fatalf("the server accepted every statement of:\n%s", schema)
	return ""
}

// A schema declaring two CHECKs of one name where the server keeps one is
// refused before a statement reaches the server, and the refusal ends with
// what the server answers when the statements do reach it: the same text, read
// from the server rather than restated. Without the validation, MySQL,
// PostgreSQL, Oracle and SQL Server build the tables and stop at the ALTER
// TABLE that adds the second CHECK, and a pair in one table on MariaDB and SQL
// Server fails in the table-creation projection as one constraint declared
// twice.
func TestSchemaApplyRefusesASecondCheckOfANameE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine checkNameEngine
		schema string
		where  string
		server string
	}{
		{
			name: "MySQL, two tables in one schema", engine: checkNameMySQL,
			schema: "CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT CK CHECK (a > 0));\n",
			where:  `table "t1" declares CHECK "ck" and table "t2" CHECK "CK", and the server keeps one CHECK of a name per schema`,
			server: `Error 3822 (HY000): Duplicate check constraint name 'CK'.`,
		},
		{
			name: "MariaDB, one table", engine: checkNameMariaDB,
			schema: "CREATE TABLE t (a int, CONSTRAINT dd CHECK (a > 0), CONSTRAINT DD CHECK (a < 9));\n",
			where:  `table "t" declares CHECK "dd" and CHECK "DD", and the server keeps one CHECK of a name per table`,
			server: `Error 1826 (HY000): Duplicate CHECK constraint name 'DD'`,
		},
		{
			name: "PostgreSQL, a column's CHECK and a table's", engine: checkNamePostgres,
			schema: "CREATE TABLE t (a int CONSTRAINT dd CHECK (a > 0), CONSTRAINT dd CHECK (a < 9));\n",
			where:  `table "t" declares CHECK "dd" and CHECK "dd", and the server keeps one CHECK of a name per table`,
			server: `ERROR: check constraint "dd" already exists (SQLSTATE 42710)`,
		},
		{
			name: "Oracle, two tables in one schema", engine: checkNameOracle,
			schema: "CREATE TABLE t1 (a NUMBER(10), CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a NUMBER(10), CONSTRAINT CK CHECK (a > 0));\n",
			where:  `table "t1" declares CHECK "ck" and table "t2" CHECK "CK", and the server keeps one CHECK of a name per schema`,
			server: `ORA-02264: name already used by an existing constraint`,
		},
		{
			name: "SQL Server, two tables in one schema", engine: checkNameSQLServer,
			schema: "CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT ck CHECK (a > 0));\n",
			where:  `table "t1" declares CHECK "ck" and table "t2" CHECK "ck", and the server keeps one CHECK of a name per schema`,
			server: `mssql: There is already an object named 'ck' in the database.`,
		},
		{
			name: "SQL Server, one table", engine: checkNameSQLServer,
			schema: "CREATE TABLE t (a int, CONSTRAINT dd CHECK (a > 0), CONSTRAINT dd CHECK (a < 9));\n",
			where:  `table "t" declares CHECK "dd" and CHECK "dd", and the server keeps one CHECK of a name per schema`,
			server: `mssql: Cannot create, drop, enable, or disable more than one constraint, column, index, or trigger named 'dd' ` +
				`in this context. Duplicate names are not allowed.`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.engine.target(c)
			schema := writeCheckNameSchema(c, test.schema)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName, qt.Commentf("%s", out))
			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(test.where+": "+test.server)+`.*`)
			c.Assert(heldColumns(c, target, test.engine.tables), qt.Equals, "0")
			c.Assert(serverRefusal(c, target, test.engine.dialect, test.schema), qt.Matches, regexp.QuoteMeta(test.server)+`.*`)
		})
	}
}

// The controls: where the server keeps a CHECK name per table, two tables each
// hold a CHECK of one name, and the apply builds both. A validation that read
// MariaDB or PostgreSQL as keeping a name per schema refuses this schema. The
// other rows are pairs a server accepts and a looser comparison would refuse.
func TestSchemaApplyKeepsACheckNamePerTableE2E(t *testing.T) {
	const sameName = "CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT ck CHECK (a < 9));\n"
	tests := []struct {
		name   string
		engine checkNameEngine
		schema string
		checks string
		want   string
	}{
		{
			name: "MariaDB", engine: checkNameMariaDB, schema: sameName, want: "2",
			checks: "SELECT count(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'ck'",
		},
		{
			name: "PostgreSQL", engine: checkNamePostgres, schema: sameName, want: "2",
			checks: "SELECT count(*) FROM pg_constraint WHERE contype = 'c' AND conname = 'ck' AND connamespace = 'public'::regnamespace",
		},
		{
			// The server accepts i beside the dotted capital I, so Ptah, which
			// folds ASCII letters only, does too.
			name: "MariaDB, i beside the dotted capital I", engine: checkNameMariaDB, want: "2",
			schema: "CREATE TABLE t (a int, CONSTRAINT `i` CHECK (a > 0), CONSTRAINT `\u0130` CHECK (a < 9));\n",
			checks: "SELECT count(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = 't'",
		},
		{
			// A case-sensitive collation holds ck and CK apart, and Ptah
			// leaves a difference of case to the server.
			name: "SQL Server, a case-sensitive collation", engine: checkNameSQLServerCaseSensitive, want: "2",
			schema: "CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT CK CHECK (a > 0));\n",
			checks: "SELECT count(*) FROM sys.check_constraints WHERE name IN ('ck', 'CK')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.engine.target(c)
			schema := writeCheckNameSchema(c, test.schema)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(heldColumns(c, target, test.checks), qt.Equals, test.want)
		})
	}
}

// The compatibility surface reaches the same refusal, before the dev database
// is asked how it writes a CHECK: the dev database holds no table afterward,
// and the message ends with the server's error, which the pinned community
// binary prints when it loads the schema on its dev database.
func TestCompatSchemaDiffRefusesASecondCheckOfANameE2E(t *testing.T) {
	c := qt.New(t)
	target := checkNameMySQL.target(c)
	dev := checkNameMySQL.target(c)
	schema := writeCheckNameSchema(c,
		"CREATE TABLE t1 (a int, CONSTRAINT ck CHECK (a > 0));\nCREATE TABLE t2 (a int, CONSTRAINT CK CHECK (a > 0));\n")

	out, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)

	c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName, qt.Commentf("%s", out))
	c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(`: Error 3822 (HY000): Duplicate check constraint name 'CK'.`))
	c.Assert(heldColumns(c, dev, checkNameMySQL.tables), qt.Equals, "0")
}
