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

// Oracle names a foreign key the author left unnamed SYS_C<n>, and holds one
// foreign key per column list and reference: a second one over the same
// columns answers ORA-02275. A declaration that leaves a key unnamed is
// satisfied by the key the server named, and a declaration that names or
// changes it replaces it with the old key dropped first. These tests drive
// the built binary and read the foreign keys back from user_constraints, the
// only place the server says which name a key has and who chose it.

const (
	oracleParentTables = "CREATE TABLE p (a NUMBER(10), b NUMBER(10), PRIMARY KEY (a, b))"
	oracleCodeTable    = "CREATE TABLE p2 (code VARCHAR2(20) PRIMARY KEY)"
	declaredParent     = "CREATE TABLE p (a NUMBER(10), b NUMBER(10), PRIMARY KEY (a, b));\n"
	declaredCodeTable  = "CREATE TABLE p2 (code VARCHAR2(20) PRIMARY KEY);\n"
)

// unnamedForeignKeyKeys lists each foreign key as table, who chose its name,
// its delete rule and, for a name the author chose, the name.
const unnamedForeignKeyKeys = "SELECT table_name || ' ' || generated || ' ' || delete_rule || " +
	"CASE WHEN generated = 'USER NAME' THEN ' ' || constraint_name END " +
	"FROM user_constraints WHERE constraint_type = 'R' ORDER BY table_name"

// TestOracleUnnamedForeignKeyE2E_HappyPath applies a declaration over a table
// whose foreign key the server named, and reads the account back.
func TestOracleUnnamedForeignKeyE2E_HappyPath(t *testing.T) {
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
		// firstRun is a part of the output of the first apply.
		firstRun string
		// keys is what user_constraints says afterwards.
		keys []string
	}{
		{
			name: "a composite key left unnamed in the table",
			held: []string{oracleParentTables,
				"CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), FOREIGN KEY (x, y) REFERENCES p (a, b))"},
			declared: declaredParent + "CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), " +
				"FOREIGN KEY (x, y) REFERENCES p (a, b));\n",
			firstRun: "Schema is synced",
			keys:     []string{"CH GENERATED NAME NO ACTION"},
		},
		{
			name: "a single-column key written inline on both sides",
			held: []string{oracleCodeTable,
				"CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20) REFERENCES p2 (code))"},
			declared: declaredCodeTable + "CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20) REFERENCES p2 (code));\n",
			firstRun: "Schema is synced",
			keys:     []string{"C2 GENERATED NAME NO ACTION"},
		},
		{
			name: "an inline key declared in the table",
			held: []string{oracleCodeTable,
				"CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20) REFERENCES p2 (code))"},
			declared: declaredCodeTable + "CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20), " +
				"FOREIGN KEY (pc) REFERENCES p2 (code));\n",
			firstRun: "Schema is synced",
			keys:     []string{"C2 GENERATED NAME NO ACTION"},
		},
		{
			name: "a table-level key declared inline",
			held: []string{oracleCodeTable,
				"CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20), FOREIGN KEY (pc) REFERENCES p2 (code))"},
			declared: declaredCodeTable + "CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20) REFERENCES p2 (code));\n",
			firstRun: "Schema is synced",
			keys:     []string{"C2 GENERATED NAME NO ACTION"},
		},
		{
			name: "a declared name replaces the name the server chose",
			held: []string{oracleParentTables,
				"CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), FOREIGN KEY (x, y) REFERENCES p (a, b))"},
			declared: declaredParent + "CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), " +
				"CONSTRAINT fk_mine FOREIGN KEY (x, y) REFERENCES p (a, b));\n",
			firstRun: "DROP CONSTRAINT",
			keys:     []string{"CH USER NAME NO ACTION FK_MINE"},
		},
		{
			name: "a changed delete rule replaces the key the server named",
			held: []string{oracleParentTables,
				"CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), FOREIGN KEY (x, y) REFERENCES p (a, b))"},
			declared: declaredParent + "CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), " +
				"FOREIGN KEY (x, y) REFERENCES p (a, b) ON DELETE CASCADE);\n",
			firstRun: "DROP CONSTRAINT",
			keys:     []string{"CH USER NAME CASCADE FK_CH_X_Y"},
		},
		{
			name: "a changed delete rule on a single-column key",
			held: []string{oracleCodeTable,
				"CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, pc VARCHAR2(20) REFERENCES p2 (code))"},
			declared: declaredCodeTable + "CREATE TABLE c2 (id NUMBER(10) PRIMARY KEY, " +
				"pc VARCHAR2(20) REFERENCES p2 (code) ON DELETE SET NULL);\n",
			firstRun: "DROP CONSTRAINT",
			keys:     []string{"C2 USER NAME SET NULL FK_C2_PC"},
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

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, test.firstRun)
			c.Assert(accountNames(c, ctx, conn, unnamedForeignKeyKeys), qt.DeepEquals, test.keys)

			again, againStderr, err := runCLIProcess(ctx, c.TempDir(), binary,
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, againStderr))
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// TestOracleUnnamedForeignKeyE2E_Rollback replaces the key the server named
// with one that has another delete rule through a generated migration, then
// rolls it back. Both directions replace a key with one over the same columns
// and reference, so each has to drop the old key before it adds the new one.
func TestOracleUnnamedForeignKeyE2E_Rollback(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		oracleParentTables,
		"CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, x NUMBER(10), y NUMBER(10), FOREIGN KEY (x, y) REFERENCES p (a, b))",
	})
	schemaPath := writeDeclaredSchema(c, declaredParent+"CREATE TABLE ch (id NUMBER(10) PRIMARY KEY, "+
		"x NUMBER(10), y NUMBER(10), FOREIGN KEY (x, y) REFERENCES p (a, b) ON DELETE CASCADE);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)

	generated, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"migrations", "generate", "--db-url", dbURL, "--schema-file", schemaPath, "--migrations-dir", dir, "--name", "cascade")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated, stderr))
	up, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"migrations", "up", "--db-url", dbURL, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", up, stderr))
	c.Assert(accountNames(c, ctx, conn, unnamedForeignKeyKeys), qt.DeepEquals, []string{"CH USER NAME CASCADE FK_CH_X_Y"})

	down, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"migrations", "down", "--db-url", dbURL, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", down, stderr))
	// The rollback restores the key under the name the server gave it, which
	// the migration recorded; the delete rule is the old one.
	keys := accountNames(c, ctx, conn, unnamedForeignKeyKeys)
	c.Assert(keys, qt.HasLen, 1)
	c.Assert(keys[0], qt.Matches, `CH USER NAME NO ACTION SYS_C[0-9]+`)
}
