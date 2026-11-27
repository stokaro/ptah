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
// every object the declaration leaves out, so a shared account would lose the
// objects of every other test.

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
		// planned is a statement the plan prints. The drop of a folded
		// namesake is matched from its name on: the plan's existence guards
		// compare names without case, so that drop may carry IF EXISTS.
		planned []string
		// objects is every table, view, sequence and trigger the account holds
		// after the apply, as type and stored name.
		objects []string
		// columns is every column of the account's tables after the apply.
		columns []string
		// rows is how many rows KEEP_ME holds after the apply.
		rows int
	}{
		{
			name:     "a quoted table is dropped by its quoted name",
			held:     []string{`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE "orders" PURGE;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a quoted table and its folded namesake are two tables, each dropped by its own name",
			held: []string{
				`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`,
				`CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE "orders" PURGE;`, ` ORDERS PURGE;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a declared lower-case name still reaches the table created bare",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`INSERT INTO keep_me (id) VALUES (1)`,
				`CREATE TABLE "keep_me" (id NUMBER(10) PRIMARY KEY)`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP TABLE "keep_me" PURGE;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
			rows:     1,
		},
		{
			name: "a quoted view is dropped by its quoted name",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE VIEW "v1" AS SELECT id FROM keep_me`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP VIEW "v1";`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a quoted view and its folded namesake are two views, each dropped by its own name",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE VIEW "v1" AS SELECT id FROM keep_me`,
				`CREATE VIEW v1 AS SELECT id FROM keep_me`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP VIEW "v1";`, ` V1;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a quoted sequence is dropped by its quoted name",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE SEQUENCE "s1"`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP SEQUENCE "s1";`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a quoted sequence and its folded namesake are two sequences, each dropped by its own name",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE SEQUENCE "s1"`,
				`CREATE SEQUENCE s1`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP SEQUENCE "s1";`, `S1;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			// The Oracle read does not report triggers, so the comparison names
			// none and neither trigger may go.
			name: "a quoted trigger and its folded namesake are both left alone",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE TRIGGER "t1" BEFORE INSERT ON keep_me FOR EACH ROW BEGIN NULL; END;`,
				`CREATE TRIGGER t1 BEFORE INSERT ON keep_me FOR EACH ROW BEGIN NULL; END;`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			objects:  []string{"TABLE KEEP_ME", "TRIGGER T1", "TRIGGER t1"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			name: "a quoted column holding a dollar sign and its folded namesake are two columns",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, "a$b" NUMBER(10), a$b NUMBER(10))`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n",
			planned:  []string{`DROP COLUMN "a$b";`, `DROP COLUMN A$B;`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID"},
		},
		{
			// A SQL file that quotes a camelCase column names it quoted in its
			// CHECK, DEFAULT and virtual column. The expressions are accepted,
			// the unnamed CHECK takes customers_Status_check, which the server
			// stores as CUSTOMERS_STATUS_CHECK, and the key on "cust_id" takes
			// fk_orders_cust_id, so the second plan finds every constraint
			// where the first one put it.
			name: "a quoted column named quoted in a CHECK, a DEFAULT and a virtual column",
			held: []string{`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n" +
				`CREATE TABLE "customers" ("id" NUMBER(10) PRIMARY KEY, ` +
				`"Status" VARCHAR2(5) DEFAULT 'a' CHECK ("Status" IN ('a','b')), ` +
				`"Twice" NUMBER(10) GENERATED ALWAYS AS ("id" * 2) VIRTUAL);` + "\n" +
				`CREATE TABLE orders (id NUMBER(10) PRIMARY KEY, "cust_id" NUMBER(10) REFERENCES "customers" ("id"));` + "\n",
			planned: []string{
				`CONSTRAINT customers_Status_check CHECK ("Status" IN ('a','b'))`,
				`GENERATED ALWAYS AS ("id" * 2) VIRTUAL`,
				`ADD CONSTRAINT fk_orders_cust_id FOREIGN KEY ("cust_id") REFERENCES "customers" ("id")`,
			},
			objects: []string{"TABLE KEEP_ME", "TABLE ORDERS", "TABLE customers"},
			columns: []string{"KEEP_ME.ID", "ORDERS.ID", "ORDERS.cust_id", "customers.Status", "customers.Twice", "customers.id"},
		},
		{
			name:     "a quoted column starting with an underscore is created quoted",
			held:     []string{`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`},
			declared: `CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, "_A" NUMBER(10));` + "\n",
			planned:  []string{`ADD ("_A" NUMBER(10))`},
			objects:  []string{"TABLE KEEP_ME"},
			columns:  []string{"KEEP_ME.ID", "KEEP_ME._A"},
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
				c.Assert(stdout, qt.Contains, statement)
			}
			// The plan names no object of the account with the account, so a
			// file written from it runs in the account that applies it.
			c.Assert(strings.ToUpper(stdout), qt.Not(qt.Contains), strings.ToUpper(account)+".")
			c.Assert(accountObjects(c, ctx, conn), qt.DeepEquals, test.objects)
			c.Assert(accountColumns(c, ctx, conn), qt.DeepEquals, test.columns)
			c.Assert(keepMeRows(c, ctx, conn), qt.Equals, test.rows)

			again, againStderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againStderr))
			c.Assert(again, qt.Contains, "Schema is synced")
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
		// objects and columns are what the account holds afterwards, unchanged.
		objects []string
		columns []string
	}{
		{
			name:     "a declared table whose fold only a quoted table matches",
			held:     []string{`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`},
			declared: "CREATE TABLE orders (id NUMBER(10) PRIMARY KEY);\n",
			refusal: `declared table orders names ORDERS on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive table "orders", which the declared name does not address`,
			objects: []string{"TABLE orders"},
			columns: []string{"orders.ID"},
		},
		{
			name:     "a declared column whose fold only a quoted column matches",
			held:     []string{`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, "note" VARCHAR2(20))`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, note VARCHAR2(20));\n",
			refusal: `declared column keep_me.note names KEEP_ME.NOTE on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive column KEEP_ME."note", which the declared name does not address`,
			objects: []string{"TABLE KEEP_ME"},
			columns: []string{"KEEP_ME.ID", "KEEP_ME.note"},
		},
		{
			name:     "a declared column holding a dollar sign whose fold only a quoted column matches",
			held:     []string{`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, "a$b" NUMBER(10))`},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY, a$b NUMBER(10));\n",
			refusal: `declared column keep_me.a$b names KEEP_ME.A$B on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive column KEEP_ME."a$b", which the declared name does not address`,
			objects: []string{"TABLE KEEP_ME"},
			columns: []string{"KEEP_ME.ID", "KEEP_ME.a$b"},
		},
		{
			name: "a declared view whose fold only a quoted view matches",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE VIEW "v1" AS SELECT id FROM keep_me`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\nCREATE VIEW v1 AS SELECT id FROM keep_me;\n",
			refusal: `declared view v1 names V1 on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive view "v1", which the declared name does not address`,
			objects: []string{"TABLE KEEP_ME", "VIEW v1"},
			columns: []string{"KEEP_ME.ID", "v1.ID"},
		},
		{
			name: "a declared sequence whose fold only a quoted sequence matches",
			held: []string{
				`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
				`CREATE SEQUENCE "s1"`,
			},
			declared: "CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\nCREATE SEQUENCE s1;\n",
			refusal: `declared sequence s1 names S1 on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive sequence "s1", which the declared name does not address`,
			objects: []string{"SEQUENCE s1", "TABLE KEEP_ME"},
			columns: []string{"KEEP_ME.ID"},
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
			c.Assert(accountObjects(c, ctx, conn), qt.DeepEquals, test.objects)
			c.Assert(accountColumns(c, ctx, conn), qt.DeepEquals, test.columns)
		})
	}
}

// TestOracleCaseSensitiveNamesE2E_ReadAsDesired takes the desired schema from
// a read of the database itself, on both binaries. A read names a table created
// quoted by its quoted spelling, so the desired side and the current side are
// the same schema and nothing is planned. Without that spelling the desired
// side holds a bare orders, which means ORDERS: the quoted table is refused as
// a namesake of ORDERS, and the pair of a quoted table and its folded namesake
// reads as two declarations of one table.
func TestOracleCaseSensitiveNamesE2E_ReadAsDesired(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	native := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), native)
	compat := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), compat)
	quoted := []string{
		`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`,
	}
	pair := []string{
		`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)`,
	}

	tests := []struct {
		name string
		held []string
		// compat selects ptah-compat rather than ptah.
		compat bool
		// args is the command, with URL standing for the account's URL.
		args []string
		// want is what stdout says when nothing is planned.
		want string
	}{
		{name: "native diff of a quoted table against itself", held: quoted,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL"}, want: "No changes found"},
		{name: "compat diff of a quoted table against itself", held: quoted, compat: true,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL"}, want: "No changes found"},
		{name: "native apply of a quoted table to itself", held: quoted,
			args: []string{"schema", "apply", "--db-url", "URL", "--to", "URL", "--dry-run"}, want: "Schema is synced"},
		{name: "compat apply of a quoted table to itself", held: quoted, compat: true,
			args: []string{"schema", "apply", "--url", "URL", "--to", "URL", "--dry-run"}, want: "Schema is synced"},
		{name: "native diff of a namesake pair against itself", held: pair,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL"}, want: "No changes found"},
		{name: "compat diff of a namesake pair against itself", held: pair, compat: true,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL"}, want: "No changes found"},
		{name: "native apply of a namesake pair to itself", held: pair,
			args: []string{"schema", "apply", "--db-url", "URL", "--to", "URL", "--dry-run"}, want: "Schema is synced"},
		{name: "compat apply of a namesake pair to itself", held: pair, compat: true,
			args: []string{"schema", "apply", "--url", "URL", "--to", "URL", "--dry-run"}, want: "Schema is synced"},
		{name: "native diff of a namesake pair excluding the quoted table", held: pair,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL", "--exclude", "orders"}, want: "No changes found"},
		{name: "compat diff of a namesake pair excluding the quoted table", held: pair, compat: true,
			args: []string{"schema", "diff", "--from", "URL", "--to", "URL", "--exclude", "orders"}, want: "No changes found"},
		{name: "native apply of a namesake pair excluding the quoted table", held: pair,
			args: []string{"schema", "apply", "--db-url", "URL", "--to", "URL", "--exclude", "orders", "--dry-run"}, want: "Schema is synced"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
			runAsAccount(c, ctx, conn, test.held)
			binary := map[bool]string{false: native, true: compat}[test.compat]
			args := make([]string, 0, len(test.args))
			for _, arg := range test.args {
				args = append(args, strings.ReplaceAll(arg, "URL", dbURL))
			}

			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, test.want)
			c.Assert(stdout, qt.Not(qt.Contains), "DROP")
			c.Assert(stdout, qt.Not(qt.Contains), "CREATE")
		})
	}
}

// TestOracleCaseSensitiveNamesE2E_InspectReplays inspects an account holding a
// quoted table, its folded namesake, a third table and a camelCase table an
// ORM would create as SQL, compares the file with the account, and replays it
// into a fresh account. Written bare, the quoted table is `CREATE TABLE
// orders`, which creates ORDERS: the file names one table twice, and the
// replay cannot recreate the account. The camelCase table's CHECK, DEFAULT and
// virtual column name its columns quoted, and the inspection writes them so.
func TestOracleCaseSensitiveNamesE2E_InspectReplays(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	source, sourceURL, sourceConn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, sourceConn, []string{
		`CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE "Cust" ("Id" NUMBER(10) PRIMARY KEY, "Status" VARCHAR2(5) DEFAULT 'a', ` +
			`"Twice" NUMBER(10) GENERATED ALWAYS AS ("Id" * 2) VIRTUAL, CONSTRAINT "ck_st" CHECK ("Status" IN ('a','b')))`,
	})

	inspected, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "inspect", "--db-url", sourceURL, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(inspected, qt.Contains, `CREATE TABLE "orders" (`)
	c.Assert(inspected, qt.Contains, `CREATE TABLE ORDERS (`)
	c.Assert(inspected, qt.Not(qt.Contains), strings.ToUpper(source)+".")
	c.Assert(inspected, qt.Contains, `CONSTRAINT "ck_st" CHECK ("Status" IN ('a','b'))`)

	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", sourceURL, "--schema-file", writeDeclaredSchema(c, inspected))
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "No schema differences detected")

	// The file names no table with the account it was read from, so it
	// replays into a fresh account as it was written.
	_, replicaURL, replicaConn := oracleCaseSensitiveAccount(c, ctx)
	stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", replicaURL, "--schema-file", writeDeclaredSchema(c, inspected), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

	c.Assert(accountTables(c, ctx, replicaConn), qt.DeepEquals, []string{"Cust", "KEEP_ME", "ORDERS", "orders"})
	c.Assert(accountNames(c, ctx, replicaConn,
		`SELECT constraint_name || ' ' || search_condition_vc FROM user_constraints `+
			`WHERE table_name = 'Cust' AND constraint_type = 'C' AND constraint_name NOT LIKE 'SYS%'`),
		qt.DeepEquals, []string{`ck_st "Status" IN ('a','b')`})
	c.Assert(accountNames(c, ctx, replicaConn,
		`SELECT column_name || ' ' || data_default_vc FROM user_tab_cols `+
			`WHERE table_name = 'Cust' AND data_default_vc IS NOT NULL ORDER BY column_name`),
		qt.DeepEquals, []string{`Status 'a'`, `Twice "Id"*2`})
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
		"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE TRIGGER TO " + account,
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

// accountObjects lists the tables, views, sequences and triggers the account
// holds as type and stored name, leaving out the apply's own tables and the
// sequences Oracle keeps for identity columns.
func accountObjects(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	return accountNames(c, ctx, conn,
		"SELECT object_type || ' ' || object_name FROM user_objects "+
			"WHERE object_type IN ('TABLE', 'VIEW', 'SEQUENCE', 'TRIGGER') "+
			"AND object_name NOT IN ('PTAH_LOCKS', 'ptah_schema_apply_log') AND object_name NOT LIKE 'ISEQ$$%' "+
			"ORDER BY object_type, object_name")
}

// accountTables lists the tables the account holds, as user_tables stores
// their names, leaving out the apply's own.
func accountTables(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	return accountNames(c, ctx, conn,
		"SELECT table_name FROM user_tables WHERE "+ptahOwnTables+" ORDER BY table_name")
}

// accountColumns lists the columns of the account's tables and views as
// table.column, in the stored spelling of each.
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

// TestOracleBareDeclaredNameSelectorE2E_HappyPath plans a declaration that
// writes its names bare and in lower case against an account holding ORDERS,
// with a selector naming the stored ORDERS. A bare name is stored in upper
// case, so the selector leaves the declared orders and the account's ORDERS
// alike, and the plan is empty. Matched as written, the declared orders stays
// in the plan and the account's ORDERS does not, so the plan creates it.
func TestOracleBareDeclaredNameSelectorE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		"CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)",
		"CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)",
	})
	schemaPath := writeDeclaredSchema(c,
		"CREATE TABLE orders (id NUMBER(10) PRIMARY KEY);\nCREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n")

	for _, selector := range [][]string{{"--exclude", "ORDERS"}, {"--include", "ORDERS"}} {
		t.Run(selector[0], func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, append([]string{
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run",
			}, selector...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestOracleBareDeclaredNameSelectorE2E_FailurePath plans the same
// declaration with the selector orders, which names a table created quoted.
// Neither side holds one, so schema apply refuses the selection, names the
// stored ORDERS as the spelling that names a table, and plans nothing.
// Matched as written, the selector leaves the declared orders and keeps the
// account's ORDERS, and the plan drops it.
func TestOracleBareDeclaredNameSelectorE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		"CREATE TABLE orders (id NUMBER(10) PRIMARY KEY)",
		"CREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY)",
	})
	schemaPath := writeDeclaredSchema(c,
		"CREATE TABLE orders (id NUMBER(10) PRIMARY KEY);\nCREATE TABLE keep_me (id NUMBER(10) PRIMARY KEY);\n")

	stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run", "--exclude", "orders")

	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Contains, `the --exclude selection matched no objects: "orders" (the stored name is "ORDERS")`)
	c.Assert(stdout, qt.Not(qt.Contains), "DROP TABLE")
}
