//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestOracleUnnamedCheckE2E applies a file holding an unnamed table-level
// CHECK, which Oracle names itself SYS_C<n> and keeps as written in
// user_constraints.search_condition. The second plan and a comparison of the
// file with the account then find nothing to do, and the constraint the first
// apply created is still the one the account holds. Compared by name, the
// second plan adds the CHECK again and drops SYS_C<n> on every run.
func TestOracleUnnamedCheckE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	schemaPath := writeDeclaredSchema(c,
		"CREATE TABLE things (id NUMBER(10) PRIMARY KEY, qty NUMBER(10), CHECK (id > 0), CONSTRAINT qty_pos CHECK (qty >= 0));\n")
	checks := `SELECT generated || ' ' || search_condition_vc FROM user_constraints ` +
		`WHERE table_name = 'THINGS' AND constraint_type = 'C' AND search_condition_vc NOT LIKE '%IS NOT NULL' ORDER BY 1`
	created := `SELECT constraint_name FROM user_constraints WHERE table_name = 'THINGS' AND search_condition_vc = 'id > 0'`

	stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, []string{"GENERATED NAME id > 0", "USER NAME qty >= 0"})
	first := accountNames(c, ctx, conn, created)

	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Contains, "Schema is synced")

	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", dbURL, "--schema-file", schemaPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "No schema differences detected")
	c.Assert(accountNames(c, ctx, conn, created), qt.DeepEquals, first)
}

// TestOracleForeignKeyDeferralE2E inspects an account holding a foreign key
// Oracle reports NOT DEFERRABLE, with DEFERRED read as IMMEDIATE, and one
// declared DEFERRABLE INITIALLY DEFERRED, and compares the file with the
// account it came from. The file writes the first key without a deferral
// clause, as the server stores it, so the comparison is clean. Read with
// its timing, the first key is written DEFERRABLE INITIALLY IMMEDIATE, and the
// comparison plans to replace a key nobody changed.
func TestOracleForeignKeyDeferralE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	account, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE parent (id NUMBER(10) PRIMARY KEY)`,
		`CREATE TABLE child (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10), ` +
			`CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent (id))`,
		`CREATE TABLE dchild (id NUMBER(10) PRIMARY KEY, parent_id NUMBER(10), ` +
			`CONSTRAINT fk_dchild_parent FOREIGN KEY (parent_id) REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)`,
	})
	c.Assert(accountNames(c, ctx, conn,
		`SELECT constraint_name || ' ' || deferrable || ' ' || deferred FROM user_constraints WHERE constraint_type = 'R' ORDER BY 1`),
		qt.DeepEquals, []string{"FK_CHILD_PARENT NOT DEFERRABLE IMMEDIATE", "FK_DCHILD_PARENT DEFERRABLE DEFERRED"})

	inspected, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "inspect", "--db-url", dbURL, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	schema := strings.ToUpper(account)
	c.Assert(inspected, qt.Contains,
		"ADD CONSTRAINT FK_CHILD_PARENT FOREIGN KEY (PARENT_ID) REFERENCES "+schema+".PARENT (ID);")
	c.Assert(inspected, qt.Contains,
		"ADD CONSTRAINT FK_DCHILD_PARENT FOREIGN KEY (PARENT_ID) REFERENCES "+schema+".PARENT (ID) DEFERRABLE INITIALLY DEFERRED;")

	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", dbURL, "--schema-file", writeDeclaredSchema(c, inspected))
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "No schema differences detected")
}

// TestOracleChangeReportE2E compares a schema-qualified file, as schema
// inspect writes one, with an account holding a quoted table. The report
// names a quoted table by its stored spelling, QRICH."orders", as the DROP
// and the refusals do, rather than quoting the spelling a second time.
func TestOracleChangeReportE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	account, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE "orders" (id NUMBER(10) PRIMARY KEY, "Old" NUMBER(10))`,
	})
	schema := strings.ToUpper(account)
	declared := "CREATE TABLE " + schema + `."orders" (id NUMBER(10) PRIMARY KEY, "New" NUMBER(10));` + "\n" +
		"CREATE TABLE " + schema + `."Items" (id NUMBER(10) PRIMARY KEY);` + "\n"

	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", dbURL, "--schema-file", writeDeclaredSchema(c, declared))

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "tables_added (1): "+schema+`."Items"`)
	c.Assert(compared, qt.Contains, "tables_modified (1): "+schema+`."orders"`)
	c.Assert(compared, qt.Not(qt.Contains), `"""`)
}

// TestOracleCheckDeferralE2E applies plain CHECKs to an account holding the
// same conditions as a deferred named CHECK, a deferred CHECK Oracle named
// and a disabled one, then declares two of them deferrable. Each plan
// replaces the CHECKs whose deferral differs and enables the disabled one in
// place, the account then holds what was declared, and the second apply of
// each file finds nothing to do. The account's deferrable CHECK is written
// with its deferral by schema inspect, and the file compares clean with the
// account. Compared by condition alone, the first plan reports the account
// synced and leaves the three CHECKs deferred and disabled.
func TestOracleCheckDeferralE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE dk (id NUMBER(10) PRIMARY KEY, n NUMBER(10), ` +
			`CONSTRAINT dk_named CHECK (n > 0) DEFERRABLE INITIALLY DEFERRED, ` +
			`CHECK (n < 100) DEFERRABLE INITIALLY DEFERRED, CONSTRAINT dk_off CHECK (n <> 50) DISABLE)`,
	})
	checks := `SELECT search_condition_vc || ' ' || deferrable || ' ' || deferred || ' ' || status || ' ' || validated || ' ' || ` +
		`generated FROM user_constraints ` +
		`WHERE table_name = 'DK' AND constraint_type = 'C' AND search_condition_vc NOT LIKE '%IS NOT NULL' ORDER BY 1`
	steps := []struct {
		declared string
		want     []string
	}{
		{
			declared: "CREATE TABLE dk (id NUMBER(10) PRIMARY KEY, n NUMBER(10), " +
				"CONSTRAINT dk_named CHECK (n > 0), CHECK (n < 100), CONSTRAINT dk_off CHECK (n <> 50));\n",
			want: []string{
				"n < 100 NOT DEFERRABLE IMMEDIATE ENABLED VALIDATED GENERATED NAME",
				"n <> 50 NOT DEFERRABLE IMMEDIATE ENABLED VALIDATED USER NAME",
				"n > 0 NOT DEFERRABLE IMMEDIATE ENABLED VALIDATED USER NAME",
			},
		},
		{
			declared: "CREATE TABLE dk (id NUMBER(10) PRIMARY KEY, n NUMBER(10), " +
				"CONSTRAINT dk_named CHECK (n > 0) DEFERRABLE INITIALLY DEFERRED, CHECK (n < 100) DEFERRABLE, " +
				"CONSTRAINT dk_off CHECK (n <> 50));\n",
			want: []string{
				"n < 100 DEFERRABLE IMMEDIATE ENABLED VALIDATED GENERATED NAME",
				"n <> 50 NOT DEFERRABLE IMMEDIATE ENABLED VALIDATED USER NAME",
				"n > 0 DEFERRABLE DEFERRED ENABLED VALIDATED USER NAME",
			},
		},
	}
	for _, step := range steps {
		schemaPath := writeDeclaredSchema(c, step.declared)

		stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
			"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stdout, qt.Contains, "DROP CONSTRAINT", qt.Commentf("stdout:\n%s", stdout))
		c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, step.want)

		again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
			"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
		c.Assert(again, qt.Contains, "Schema is synced")
	}

	inspected, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "inspect", "--db-url", dbURL, "--format", "sql")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(inspected, qt.Contains, "CONSTRAINT DK_NAMED CHECK (n > 0) DEFERRABLE INITIALLY DEFERRED")

	compared, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "compare", "--db-url", dbURL, "--schema-file", writeDeclaredSchema(c, inspected))
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, stderr))
	c.Assert(compared, qt.Contains, "No schema differences detected")
}

// TestOracleDisabledCheckE2E applies plain CHECKs to an account holding the
// same conditions disabled, as a named CHECK, as a CHECK Oracle named, and as
// one enabled without validating its rows, while a row breaks the named one,
// and a foreign key to an account holding it disabled. The plan enables each
// in place with ENABLE VALIDATE CONSTRAINT and drops none. The row fails the
// named CHECK, so Oracle answers ORA-02293 and the CHECK is still there,
// disabled, as it was. With the row gone the same file applies, every CHECK is
// enabled and validated under the name it had, and the second apply finds
// nothing to do. Dropped and added again, the failed apply leaves the table
// with no CHECK at all, since Oracle commits the drop before the add fails.
func TestOracleDisabledCheckE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE t11 (id NUMBER(10) PRIMARY KEY, a NUMBER(10), b NUMBER(10), d NUMBER(10), ` +
			`CONSTRAINT ck11 CHECK (a > 0) DISABLE, CHECK (b > 0) DISABLE, CONSTRAINT ck13 CHECK (d > 0) ENABLE NOVALIDATE)`,
		`INSERT INTO t11 (id, a, b, d) VALUES (1, -5, 1, 1)`,
		`CREATE TABLE c11 (id NUMBER(10) PRIMARY KEY, t11_id NUMBER(10), ` +
			`CONSTRAINT fk11 FOREIGN KEY (t11_id) REFERENCES t11 (id) DISABLE)`,
	})
	checks := `SELECT search_condition_vc || ' ' || status || ' ' || validated || ' ' || generated || ' ' || ` +
		`CASE generated WHEN 'USER NAME' THEN constraint_name END FROM user_constraints ` +
		`WHERE table_name = 'T11' AND constraint_type = 'C' AND search_condition_vc NOT LIKE '%IS NOT NULL' ORDER BY 1`
	foreignKey := `SELECT constraint_name || ' ' || status || ' ' || validated FROM user_constraints WHERE constraint_type = 'R'`
	schemaPath := writeDeclaredSchema(c, "CREATE TABLE t11 (id NUMBER(10) PRIMARY KEY, a NUMBER(10), b NUMBER(10), d NUMBER(10), "+
		"CONSTRAINT ck11 CHECK (a > 0), CHECK (b > 0), CONSTRAINT ck13 CHECK (d > 0));\n"+
		"CREATE TABLE c11 (id NUMBER(10) PRIMARY KEY, t11_id NUMBER(10), CONSTRAINT fk11 FOREIGN KEY (t11_id) REFERENCES t11 (id));\n")

	stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout+stderr, qt.Contains, "ORA-02293")
	c.Assert(stdout, qt.Contains, "ENABLE VALIDATE CONSTRAINT CK11;")
	c.Assert(stdout, qt.Not(qt.Contains), "DROP CONSTRAINT")
	c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, []string{
		"a > 0 DISABLED NOT VALIDATED USER NAME CK11",
		"b > 0 DISABLED NOT VALIDATED GENERATED NAME ",
		"d > 0 ENABLED NOT VALIDATED USER NAME CK13",
	})
	c.Assert(accountNames(c, ctx, conn, foreignKey), qt.DeepEquals, []string{"FK11 ENABLED VALIDATED"})

	runAsAccount(c, ctx, conn, []string{`DELETE FROM t11 WHERE a <= 0`})
	stdout, stderr, err = runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Not(qt.Contains), "DROP CONSTRAINT")
	c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, []string{
		"a > 0 ENABLED VALIDATED USER NAME CK11",
		"b > 0 ENABLED VALIDATED GENERATED NAME ",
		"d > 0 ENABLED VALIDATED USER NAME CK13",
	})

	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Contains, "Schema is synced")
}

// TestOracleChangedCheckE2E applies a changed condition to a named CHECK and
// to one Oracle named, while a row breaks both new conditions. The plan adds
// each new CHECK before it drops the old one, the named one under a temporary
// name that it renames to the declared name after the drop, so the table
// holds a CHECK at every step. The row refuses the first ADD with ORA-02293,
// and both old CHECKs survive as they were, with no CHECK left under the
// temporary name. With the row gone the same file applies, the table holds the
// new conditions under the declared name, and the second apply finds nothing
// to do. Dropped first, the failed apply leaves the table with no CHECK on
// either column.
func TestOracleChangedCheckE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)
	_, dbURL, conn := oracleCaseSensitiveAccount(c, ctx)
	runAsAccount(c, ctx, conn, []string{
		`CREATE TABLE t12 (id NUMBER(10) PRIMARY KEY, a NUMBER(10), b NUMBER(10), ` +
			`CONSTRAINT ck12 CHECK (a > -10), CHECK (b > -10))`,
		`INSERT INTO t12 (id, a, b) VALUES (1, -5, -5)`,
	})
	checks := `SELECT search_condition_vc || ' ' || status || ' ' || validated || ' ' || generated || ' ' || ` +
		`CASE generated WHEN 'USER NAME' THEN constraint_name END FROM user_constraints ` +
		`WHERE table_name = 'T12' AND constraint_type = 'C' AND search_condition_vc NOT LIKE '%IS NOT NULL' ORDER BY 1`
	schemaPath := writeDeclaredSchema(c, "CREATE TABLE t12 (id NUMBER(10) PRIMARY KEY, a NUMBER(10), b NUMBER(10), "+
		"CONSTRAINT ck12 CHECK (a > 0), CHECK (b > 0));\n")

	stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout+stderr, qt.Contains, "ORA-02293")
	c.Assert(stdout, qt.Matches, `(?s).*ADD CONSTRAINT ptah_[0-9a-f]{16} CHECK \(a > 0\);.*DROP CONSTRAINT CK12;.*RENAME CONSTRAINT ptah_[0-9a-f]{16} TO ck12;.*`)
	c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, []string{
		"a > -10 ENABLED VALIDATED USER NAME CK12",
		"b > -10 ENABLED VALIDATED GENERATED NAME ",
	})

	runAsAccount(c, ctx, conn, []string{`DELETE FROM t12 WHERE a <= 0 OR b <= 0`})
	stdout, stderr, err = runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(accountNames(c, ctx, conn, checks), qt.DeepEquals, []string{
		"a > 0 ENABLED VALIDATED USER NAME CK12",
		"b > 0 ENABLED VALIDATED GENERATED NAME ",
	})

	again, stderr, err := runCLIProcess(ctx, c.TempDir(), binary,
		"schema", "apply", "--db-url", dbURL, "--schema-file", schemaPath, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", again, stderr))
	c.Assert(again, qt.Contains, "Schema is synced")
}
