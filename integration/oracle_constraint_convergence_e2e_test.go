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
