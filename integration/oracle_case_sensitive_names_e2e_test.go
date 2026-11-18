//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// Oracle keeps a quoted name exactly and folds an unquoted one to upper case,
// so a table created as "orders" is stored as orders and is a different table
// from ORDERS. A declared `orders` means ORDERS. These tests drive `ptah schema
// apply` against accounts holding such tables, and read the account back from
// user_tables and user_tab_columns, since only the server can say which table a
// statement reached (stokaro/ptah#6243).
//
// Each case runs in an account of its own: a declarative apply plans a DROP for
// every table the declaration leaves out, so a shared account would lose the
// tables of every other test.

// TestOracleCaseSensitiveNamesE2E_HappyPath applies a declaration against an
// account holding a case-sensitive table and reads back what the apply did.
func TestOracleCaseSensitiveNamesE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	tests := []struct {
		name string
		// held is the DDL that sets the account up, run as the account.
		held []string
		// declared is the desired schema file.
		declared string
		// planned is a statement the plan prints, with ACCOUNT standing for the
		// account's name.
		planned []string
		// tables is every table the account holds after the apply.
		tables []string
		// rows is how many rows KEEP_ME holds after the apply.
		rows int
	}{
		{
			name:     "a quoted table is dropped by its quoted name",
			held:     []string{`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE ACCOUNT."orders" PURGE;`},
			tables:   []string{"KEEP_ME"},
		},
		{
			name: "a quoted table and its folded namesake are two tables, each dropped by its own name",
			held: []string{
				`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`,
				`CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE ACCOUNT."orders" PURGE;`, `ACCOUNT.ORDERS PURGE;`},
			tables:   []string{"KEEP_ME"},
		},
		{
			name: "a declared lower-case name still reaches the table created bare",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`INSERT INTO keep_me (id) VALUES (1)`,
				`CREATE TABLE "keep_me" (id NUMBER(10) PRIMARY KEY)`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE ACCOUNT."keep_me" PURGE;`},
			tables:   []string{"KEEP_ME"},
			rows:     1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			account, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
			runAsAccount(c, ctx, conn, test.held)
			schemaPath := writeDeclaredSchema(c, test.declared)

			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			for _, statement := range test.planned {
				c.Assert(stdout, qt.Contains, strings.ReplaceAll(statement, "ACCOUNT", strings.ToUpper(account)))
			}
			c.Assert(accountTables(c, ctx, conn), qt.DeepEquals, test.tables)
			c.Assert(keepMeRows(c, ctx, conn), qt.Equals, test.rows)

			again, againStderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againStderr))
			c.Assert(again, qt.Not(qt.Contains), "DROP")
			c.Assert(again, qt.Not(qt.Contains), "CREATE")
		})
	}
}

// TestOracleCaseSensitiveNamesE2E_FailurePath declares a name whose fold the
// account does not hold while it holds a case-sensitive namesake, and checks
// that the apply refuses before it changes anything.
func TestOracleCaseSensitiveNamesE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	tests := []struct {
		name     string
		held     []string
		declared string
		// refusal is the part of stderr that names both spellings.
		refusal string
		// tables and columns are what the account holds afterwards, unchanged.
		tables  []string
		columns []string
	}{
		{
			name:     "a declared table whose fold only a quoted table matches",
			held:     []string{`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`},
			declared: "CREATE TABLE orders (id NUMBER(10) PRIMARY KEY);\n",
			refusal: `declared table orders names ORDERS on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive table "orders", which the declared name does not address`,
			tables:  []string{"orders"},
			columns: []string{"orders.ID"},
		},
		{
			name:     "a declared column whose fold only a quoted column matches",
			held:     []string{`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, "note" VARCHAR2(20))`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, note VARCHAR2(20));\n",
			refusal: `declared column keep_me.note names KEEP_ME.NOTE on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive column KEEP_ME."note", which the declared name does not address`,
			tables:  []string{"KEEP_ME"},
			columns: []string{"KEEP_ME.ID", "KEEP_ME.note"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
			runAsAccount(c, ctx, conn, test.held)
			schemaPath := writeDeclaredSchema(c, test.declared)

			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Contains, test.refusal)
			c.Assert(stdout, qt.Not(qt.Contains), "DROP")
			c.Assert(accountTables(c, ctx, conn), qt.DeepEquals, test.tables)
			c.Assert(accountColumns(c, ctx, conn), qt.DeepEquals, test.columns)
		})
	}
}

// oracleCaseSensitiveAccount creates an Oracle account for one test, drops it
// with everything it owns when the test ends, and returns its name, its URL and
// a connection to it.
func oracleCaseSensitiveAccount(c *qt.C, ctx context.Context) (account, dbURL string, conn *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	account = fmt.Sprintf("ptah_case_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	c.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(dropCtx, "DROP USER "+account+" CASCADE")
		c.Check(dropErr, qt.IsNil)
	})
	runAsAccount(c, ctx, admin, []string{
		"CREATE USER " + account + " IDENTIFIED BY ptah_password",
		"ALTER USER " + account + " QUOTA UNLIMITED ON USERS",
		"GRANT CREATE SESSION, CREATE TABLE TO " + account,
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	dbURL = parsed.String()
	conn, err = dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return account, dbURL, conn
}

func runAsAccount(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

func writeDeclaredSchema(c *qt.C, declared string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(declared), 0o600), qt.IsNil)
	return path
}

// ptahOwnTables leaves out the tables an apply creates for itself: its lock
// table and its run log.
const ptahOwnTables = "table_name NOT IN ('PTAH_LOCKS', 'ptah_schema_apply_log')"

// accountTables lists the tables the account holds, as user_tables stores
// their names, leaving out the apply's own.
func accountTables(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	return accountNames(c, ctx, conn,
		"SELECT table_name FROM user_tables WHERE "+ptahOwnTables+" ORDER BY table_name")
}

// accountColumns lists the columns of the account's tables as table.column,
// in the stored spelling of each.
func accountColumns(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	return accountNames(c, ctx, conn,
		"SELECT table_name || '.' || column_name FROM user_tab_columns "+
			"WHERE "+ptahOwnTables+" ORDER BY table_name, column_name")
}

func accountNames(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, query string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// keepMeRows counts the rows of KEEP_ME, the table a declared keep_me names.
func keepMeRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM keep_me").Scan(&count), qt.IsNil)
	return count
}
