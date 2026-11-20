//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestOracleColumnUniqueChangeE2E applies a file that changes the keys of
// columns in an account, and reads the keys back. A column that gains its own
// UNIQUE gets one, which Oracle names; a column that loses the key Oracle named
// loses it; a key the account named and the file no longer declares is
// dropped once; and an unnamed key over two columns, which Oracle names
// SYS_C<n>, is the key the file declares. The second plan is then empty.
//
// Without the change the first two are a MODIFY that restates the type and
// leaves the key as it was, so the plan never converges; the columns of the
// named two-column key read as unique each, so their MODIFYs never converge
// either; and the unnamed two-column key is added again, which Oracle refuses
// with ORA-02261.
func TestOracleColumnUniqueChangeE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE gains (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20))`,
		`CREATE TABLE loses (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20) UNIQUE)`,
		`CREATE TABLE widens (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20))`,
		`CREATE TABLE named_pair (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), CONSTRAINT named_pair_ab UNIQUE (a, b))`,
		`CREATE TABLE named_gone (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), CONSTRAINT named_gone_a UNIQUE (a))`,
		`CREATE TABLE unnamed_pair (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), UNIQUE (a, b))`,
	})
	schemaPath := writeDeclaredSchema(c, `CREATE TABLE gains (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20) UNIQUE);
CREATE TABLE loses (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20));
CREATE TABLE widens (id NUMBER(10) PRIMARY KEY, a VARCHAR2(40) UNIQUE);
CREATE TABLE named_pair (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), CONSTRAINT named_pair_ab UNIQUE (a, b));
CREATE TABLE named_gone (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20));
CREATE TABLE unnamed_pair (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), UNIQUE (a, b));
`)
	keys := `SELECT c.table_name || ' ' || c.generated || ' ' || ` +
		`LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) ` +
		`FROM user_constraints c JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name ` +
		`WHERE c.constraint_type = 'U' GROUP BY c.table_name, c.generated, c.constraint_name ORDER BY 1`
	unnamed := `SELECT constraint_name FROM user_constraints WHERE table_name = 'UNNAMED_PAIR' AND constraint_type = 'U'`
	before := accountNames(c, ctx, conn, unnamed)

	applied, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, stderr))
	c.Assert(accountNames(c, ctx, conn, keys), qt.DeepEquals, []string{
		"GAINS GENERATED NAME A",
		"NAMED_PAIR USER NAME A,B",
		"UNNAMED_PAIR GENERATED NAME A,B",
		"WIDENS GENERATED NAME A",
	})
	c.Assert(accountNames(c, ctx, conn, unnamed), qt.DeepEquals, before)

	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Contains, "Schema is synced")
}
