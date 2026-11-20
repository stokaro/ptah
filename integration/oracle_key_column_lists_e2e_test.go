//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// oracleKeysQuery reads the primary and unique keys of the tables the key
// tests build, one row per key: the table, the type, the name the account gave
// it or SYS where Oracle named it, and its columns in order.
const oracleKeysQuery = `SELECT c.table_name || ' ' || c.constraint_type || ' ' || ` +
	`DECODE(c.generated, 'USER NAME', c.constraint_name, 'SYS') || ' ' || ` +
	`LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) ` +
	`FROM user_constraints c JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name ` +
	`WHERE c.constraint_type IN ('P', 'U') AND c.table_name IN ('KL', 'KM', 'KN', 'KQ', 'KS') ` +
	`GROUP BY c.table_name, c.constraint_type, c.generated, c.constraint_name ORDER BY 1`

// oracleForeignKeysQuery reads the foreign keys of KF and KG, one row per
// foreign key: its table, its name, and the key it references, by name or SYS
// where Oracle named that key.
const oracleForeignKeysQuery = `SELECT c.table_name || ' ' || c.constraint_name || ' ' || ` +
	`DECODE(r.generated, 'USER NAME', r.constraint_name, 'SYS') ` +
	`FROM user_constraints c JOIN user_constraints r ON r.constraint_name = c.r_constraint_name ` +
	`WHERE c.constraint_type = 'R' AND c.table_name IN ('KF', 'KG') ORDER BY 1`

// oracleLongTableName is a table name of 126 bytes, so <table>_pkey is longer
// than the 128 bytes Oracle takes.
var oracleLongTableName = strings.Repeat("l", 126)

// TestOracleKeyOverHeldColumnsE2E generates a migration that replaces keys
// with others over the same ordered column lists, runs it, and rolls it back.
// Oracle holds one key per list and answers the second with ORA-02261, so each
// old key is dropped before its replacement is added: a named key, a named
// key over two columns, the key Oracle named for a column alone, a UNIQUE a
// primary key replaces, and the key Oracle named for a column that a named
// primary key replaces. The primary keys keep their declared names, which the
// rollback drops them by, and the rollback gives each column its own key back
// after the primary key over it is gone. The foreign keys over KL and KM leave
// before the keys they reference and come back over the new ones, since Oracle
// answers the drop of a referenced key with ORA-02273. A primary key added to
// a table with a 126-byte name gets a name cut to 128 bytes.
//
// Without the order the up file stops at the first ADD with ORA-02261. Without
// the name the down file stops at `DROP CONSTRAINT ks_pk` with ORA-02443.
func TestOracleKeyOverHeldColumnsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE kl (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20) UNIQUE)`,
		`CREATE TABLE km (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), CONSTRAINT km_k1 UNIQUE (a))`,
		`CREATE TABLE kn (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), CONSTRAINT kn_k1 UNIQUE (a, b))`,
		`CREATE TABLE ks (id NUMBER(10) NOT NULL, CONSTRAINT ks_u UNIQUE (id))`,
		`CREATE TABLE kq (a NUMBER(10) NOT NULL UNIQUE)`,
		`CREATE TABLE kf (r VARCHAR2(20), CONSTRAINT kf_fk FOREIGN KEY (r) REFERENCES kl (a))`,
		`CREATE TABLE kg (r VARCHAR2(20), CONSTRAINT kg_fk FOREIGN KEY (r) REFERENCES km (a))`,
		`CREATE TABLE ` + oracleLongTableName + ` (id NUMBER(10) NOT NULL)`,
	})
	built := []string{
		"KL P SYS ID", "KL U SYS A", "KM P SYS ID", "KM U KM_K1 A", "KN P SYS ID", "KN U KN_K1 A,B", "KQ U SYS A", "KS U KS_U ID",
	}
	builtForeignKeys := []string{"KF KF_FK SYS", "KG KG_FK KM_K1"}
	longKeyQuery := `SELECT generated || ' ' || LENGTHB(constraint_name) FROM user_constraints ` +
		`WHERE constraint_type = 'P' AND table_name = '` + strings.ToUpper(oracleLongTableName) + `'`
	c.Assert(accountNames(c, ctx, conn, oracleKeysQuery), qt.DeepEquals, built)
	c.Assert(accountNames(c, ctx, conn, oracleForeignKeysQuery), qt.DeepEquals, builtForeignKeys)
	schemaPath := writeDeclaredSchema(c, `CREATE TABLE kl (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), CONSTRAINT kl_k2 UNIQUE (a));
CREATE TABLE km (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), CONSTRAINT km_k2 UNIQUE (a));
CREATE TABLE kn (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20), b VARCHAR2(20), CONSTRAINT kn_k2 UNIQUE (a, b));
CREATE TABLE ks (id NUMBER(10) NOT NULL, CONSTRAINT ks_pk PRIMARY KEY (id));
CREATE TABLE kq (a NUMBER(10) NOT NULL, CONSTRAINT kq_pk PRIMARY KEY (a));
CREATE TABLE kf (r VARCHAR2(20), CONSTRAINT kf_fk FOREIGN KEY (r) REFERENCES kl (a));
CREATE TABLE kg (r VARCHAR2(20), CONSTRAINT kg_fk FOREIGN KEY (r) REFERENCES km (a));
CREATE TABLE `+oracleLongTableName+` (id NUMBER(10) NOT NULL, PRIMARY KEY (id));
`)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)

	generated, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, "migrations", "generate",
		"--db-url", dbURL, "--schema-file", schemaPath, "--migrations-dir", dir, "--name", "keys")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated, stderr))
	up, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, "migrations", "up",
		"--db-url", dbURL, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", up, stderr))
	c.Assert(accountNames(c, ctx, conn, oracleKeysQuery), qt.DeepEquals, []string{
		"KL P SYS ID", "KL U KL_K2 A", "KM P SYS ID", "KM U KM_K2 A", "KN P SYS ID", "KN U KN_K2 A,B", "KQ P KQ_PK A", "KS P KS_PK ID",
	})
	c.Assert(accountNames(c, ctx, conn, oracleForeignKeysQuery), qt.DeepEquals, []string{"KF KF_FK KL_K2", "KG KG_FK KM_K2"})
	c.Assert(accountNames(c, ctx, conn, longKeyQuery), qt.DeepEquals, []string{"USER NAME 128"})
	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Contains, "Schema is synced")

	down, stderr, err := runCLIProcess(ctx, c.TempDir(), binary, "migrations", "down",
		"--db-url", dbURL, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", down, stderr))
	c.Assert(accountNames(c, ctx, conn, oracleKeysQuery), qt.DeepEquals, built)
	c.Assert(accountNames(c, ctx, conn, oracleForeignKeysQuery), qt.DeepEquals, builtForeignKeys)
	c.Assert(accountNames(c, ctx, conn, longKeyQuery), qt.HasLen, 0)
}

// TestOracleSecondKeyOverColumnsE2E is refused before any statement runs: the
// file declares a column primary and unique, which are two keys over one
// column, and Oracle answers the second with ORA-02261 partway through the
// plan. The table the file adds first is not created.
func TestOracleSecondKeyOverColumnsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	schemaPath := writeDeclaredSchema(c, `CREATE TABLE ka (id NUMBER(10) PRIMARY KEY);
CREATE TABLE kp (a NUMBER(10) PRIMARY KEY UNIQUE);
`)

	applied, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")

	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, stderr))
	c.Assert(stderr, qt.Contains, `two keys claim the same columns: table "kp" declares the primary key and column "a" as unique, both over (a): `+
		"ORA-02261: The unique or primary key already exists in the table.")
	c.Assert(accountNames(c, ctx, conn, `SELECT table_name FROM user_tables WHERE table_name IN ('KA', 'KP')`), qt.HasLen, 0)
}
