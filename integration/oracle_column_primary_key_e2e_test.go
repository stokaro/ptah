//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// A primary key a column declares is the table's key on Oracle, and a MODIFY
// that restates the column's type leaves the key as it was. These tests drive
// the built binary against accounts whose tables gain, lose, move and replace
// their keys, and read the keys and nullability back from the catalog, since
// only the server can say whether a key changed.
//
// Each case runs in an account of its own, so one apply's drops do not reach
// the tables of another.

// oracleKeyColumnsQuery lists each primary key as its table, its stored name
// where the declaration chose one (SYS for a name Oracle made up) and its
// columns in key order.
const oracleKeyColumnsQuery = `SELECT c.table_name || ' P ' || DECODE(c.generated, 'USER NAME', c.constraint_name, 'SYS') || ' ' || ` +
	`LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) ` +
	`FROM user_constraints c JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name ` +
	`WHERE c.constraint_type = 'P' AND c.table_name NOT LIKE 'PTAH%' AND c.table_name NOT LIKE 'ptah%' AND c.table_name NOT LIKE 'SCHEMA_MIGRATIONS%' ` +
	`AND c.table_name NOT LIKE 'schema_migrations%' ` +
	`GROUP BY c.table_name, c.generated, c.constraint_name ORDER BY 1`

// oracleNullabilityQuery lists each column of the account's tables with its
// nullability as the catalog reports it.
const oracleNullabilityQuery = `SELECT table_name || '.' || column_name || ' ' || nullable FROM user_tab_columns ` +
	`WHERE table_name NOT LIKE 'PTAH%' AND table_name NOT LIKE 'ptah%' AND table_name NOT LIKE 'SCHEMA_MIGRATIONS%' ` +
	`AND table_name NOT LIKE 'schema_migrations%' ORDER BY 1`

// oracleColumnKeyCases are a table held by the account and a declaration of
// it that changes the table's key. keys and nullability are what the account
// holds after the apply, named by the server where the plan adds the key.
var oracleColumnKeyCases = []struct {
	name     string
	held     []string
	declared string
	// planned is matched against the plan, with (?s) so one pattern can pin
	// the order of two statements.
	planned     string
	keys        []string
	nullability []string
}{
	{
		name:        "a NOT NULL column gains the key",
		held:        []string{`CREATE TABLE t (id NUMBER(10) NOT NULL)`},
		declared:    "CREATE TABLE t (id NUMBER(10) PRIMARY KEY);\n",
		planned:     `ADD CONSTRAINT t_pkey PRIMARY KEY \(id\)`,
		keys:        []string{"T P T_PKEY ID"},
		nullability: []string{"T.ID N"},
	},
	{
		name:        "a nullable column gains the key",
		held:        []string{`CREATE TABLE t (id NUMBER(10))`},
		declared:    "CREATE TABLE t (id NUMBER(10) PRIMARY KEY);\n",
		planned:     `(?s)MODIFY \(id NUMBER\(10\) NOT NULL\);.*ADD CONSTRAINT t_pkey PRIMARY KEY \(id\)`,
		keys:        []string{"T P T_PKEY ID"},
		nullability: []string{"T.ID N"},
	},
	{
		name:        "a column NOT NULL through its key loses the key and is declared NOT NULL",
		held:        []string{`CREATE TABLE t (id NUMBER(10) PRIMARY KEY)`},
		declared:    "CREATE TABLE t (id NUMBER(10) NOT NULL);\n",
		planned:     `(?s)MODIFY \(id NUMBER\(10\) NOT NULL\);.*DROP PRIMARY KEY`,
		keys:        make([]string, 0),
		nullability: []string{"T.ID N"},
	},
	{
		name:        "a column NOT NULL through its key loses the key and is declared nullable",
		held:        []string{`CREATE TABLE t (id NUMBER(10) PRIMARY KEY)`},
		declared:    "CREATE TABLE t (id NUMBER(10));\n",
		planned:     `DROP PRIMARY KEY`,
		keys:        make([]string, 0),
		nullability: []string{"T.ID Y"},
	},
	{
		name:        "a column with its own NOT NULL loses the key and is declared NOT NULL",
		held:        []string{`CREATE TABLE t (id NUMBER(10) NOT NULL PRIMARY KEY)`},
		declared:    "CREATE TABLE t (id NUMBER(10) NOT NULL);\n",
		planned:     `DROP PRIMARY KEY`,
		keys:        make([]string, 0),
		nullability: []string{"T.ID N"},
	},
	{
		name:        "a column with its own NOT NULL loses the key and is declared nullable",
		held:        []string{`CREATE TABLE t (id NUMBER(10) NOT NULL PRIMARY KEY)`},
		declared:    "CREATE TABLE t (id NUMBER(10));\n",
		planned:     `(?s)DROP PRIMARY KEY.*MODIFY \(id NUMBER\(10\) NULL\)`,
		keys:        make([]string, 0),
		nullability: []string{"T.ID Y"},
	},
	{
		name:        "the key moves from one column to another",
		held:        []string{`CREATE TABLE t (a NUMBER(10) PRIMARY KEY, b NUMBER(10) NOT NULL)`},
		declared:    "CREATE TABLE t (a NUMBER(10) NOT NULL, b NUMBER(10) PRIMARY KEY);\n",
		planned:     `(?s)MODIFY \(a NUMBER\(10\) NOT NULL\);.*DROP PRIMARY KEY.*ADD CONSTRAINT t_pkey PRIMARY KEY \(b\)`,
		keys:        []string{"T P T_PKEY B"},
		nullability: []string{"T.A N", "T.B N"},
	},
	{
		name:        "a named key is replaced by one under another name over other columns",
		held:        []string{`CREATE TABLE t (a NUMBER(10), b NUMBER(10), CONSTRAINT pk_old PRIMARY KEY (a))`},
		declared:    "CREATE TABLE t (a NUMBER(10) NOT NULL, b NUMBER(10) NOT NULL, CONSTRAINT pk_new PRIMARY KEY (b));\n",
		planned:     `(?s)DROP CONSTRAINT PK_OLD;.*ADD CONSTRAINT pk_new PRIMARY KEY \(b\)`,
		keys:        []string{"T P PK_NEW B"},
		nullability: []string{"T.A N", "T.B N"},
	},
	{
		name:        "a named key is dropped and its column is declared nullable",
		held:        []string{`CREATE TABLE t (a NUMBER(10), b NUMBER(10), CONSTRAINT pk_old PRIMARY KEY (a))`},
		declared:    "CREATE TABLE t (a NUMBER(10), b NUMBER(10));\n",
		planned:     `DROP CONSTRAINT PK_OLD`,
		keys:        make([]string, 0),
		nullability: []string{"T.A Y", "T.B Y"},
	},
}

// TestOracleColumnPrimaryKeyE2E_ApplyChangesTheKey applies each declaration
// and reads the keys and nullability back. The second plan is empty, so one
// apply reaches the declared state.
func TestOracleColumnPrimaryKeyE2E_ApplyChangesTheKey(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	for _, test := range oracleColumnKeyCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
			runAsAccount(c, ctx, conn, test.held)
			schemaPath := writeDeclaredSchema(c, test.declared)

			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Matches, "(?s).*"+test.planned+".*")
			c.Assert(accountNames(c, ctx, conn, oracleKeyColumnsQuery), qt.DeepEquals, test.keys)
			c.Assert(accountNames(c, ctx, conn, oracleNullabilityQuery), qt.DeepEquals, test.nullability)

			again, againStderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againStderr))
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestOracleColumnPrimaryKeyE2E_DownRestoresTheKey generates a migration for
// each declaration, runs it up and then down to version 0, and reads the keys
// and nullability back after each. The down file gives back the key columns
// and the nullability the account held. A key Oracle named comes back under
// the name the plan gives a key it adds, so the keys are compared by column.
func TestOracleColumnPrimaryKeyE2E_DownRestoresTheKey(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	const keyedColumns = `SELECT c.table_name || ' P ' || LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) ` +
		`FROM user_constraints c JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name ` +
		`WHERE c.constraint_type = 'P' AND c.table_name = 'T' GROUP BY c.table_name, c.constraint_name ORDER BY 1`

	for _, test := range oracleColumnKeyCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
			runAsAccount(c, ctx, conn, test.held)
			keysBefore := accountNames(c, ctx, conn, keyedColumns)
			nullabilityBefore := accountNames(c, ctx, conn, oracleNullabilityQuery)
			work := c.TempDir()
			dir := filepath.Join(work, "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			schemaPath := writeDeclaredSchema(c, test.declared)

			generated, stderr, err := runCLIProcess(ctx, work, binary, "migrations", "generate",
				"--db-url", dbURL, "--schema-file", schemaPath, "--migrations-dir", dir, "--name", "keys")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated, stderr))
			up, stderr, err := runCLIProcess(ctx, work, binary, "migrations", "up",
				"--db-url", dbURL, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", up, stderr))
			c.Assert(accountNames(c, ctx, conn, oracleNullabilityQuery), qt.DeepEquals, test.nullability)

			down, stderr, err := runCLIProcess(ctx, work, binary, "migrations", "down",
				"--db-url", dbURL, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", down, stderr))
			c.Assert(accountNames(c, ctx, conn, keyedColumns), qt.DeepEquals, keysBefore)
			c.Assert(accountNames(c, ctx, conn, oracleNullabilityQuery), qt.DeepEquals, nullabilityBefore)
		})
	}
}
