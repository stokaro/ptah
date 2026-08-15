//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemaload"
)

// createRLSSpellingDatabases provisions an empty target database and a separate
// empty dev database, and registers their removal.
//
// They must be two databases rather than two schemas: the dev database is reset
// destructively before the plan is rehearsed on it, and `schema apply` refuses
// outright when --dev-url may address the target.
func createRLSSpellingDatabases(t *testing.T, adminURL string) (targetURL, devURL string) {
	t.Helper()
	c := qt.New(t)
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	targetName := "ptah_rls_target_" + suffix
	devName := "ptah_rls_dev_" + suffix
	conn, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+targetName+" WITH (FORCE)")
		_, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+devName+" WITH (FORCE)")
		dbschema.CloseAndWarn(conn)
	})
	for _, name := range []string{targetName, devName} {
		_, err := conn.ExecContext(context.Background(), "CREATE DATABASE "+name)
		c.Assert(err, qt.IsNil)
	}
	return databaseURL(c, adminURL, targetName), databaseURL(c, adminURL, devName)
}

// databaseURL rewrites the database component of a PostgreSQL URL, keeping
// credentials, host and query parameters.
func databaseURL(c *qt.C, adminURL, database string) string {
	c.Helper()
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	return parsed.String()
}

// rlsPolicyRows reports the relations carrying a row-level security policy in
// the public schema, as `relname/polname` pairs.
func rlsPolicyRows(t *testing.T, dbURL string) []string {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT c.relname || '/' || p.polname
		   FROM pg_policy p
		   JOIN pg_class c ON c.oid = p.polrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE n.nspname = 'public'
		  ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	found := []string{}
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// qualifiedRLSPolicyRows reports every user-schema row-level security policy as
// a `nspname/relname/polname` triple, so a fixture whose relation lives outside
// `public` can say which relation the policy actually landed on.
func qualifiedRLSPolicyRows(t *testing.T, dbURL string) []string {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT n.nspname || '/' || c.relname || '/' || p.polname
		   FROM pg_policy p
		   JOIN pg_class c ON c.oid = p.polrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		  ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	found := []string{}
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// runCompatSchemaApply runs `schema apply` on the compatibility binary's
// command tree and returns its combined output and error.
func runCompatSchemaApply(targetURL, devURL, schemaPath string, extra ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	args := []string{
		"schema", "apply",
		"--url", targetURL,
		"--to", "file://" + schemaPath,
		"--dev-url", devURL,
		"--auto-approve",
	}
	cmd.SetArgs(append(args, extra...))
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaApplyRLSReferenceToUndeclaredRelationRefusedLivePostgres is the
// compatibility floor for a schema file whose row-level security declaration
// names a relation the file does not declare.
//
// `CREATE TABLE "ORDERS"` creates the relation `ORDERS`; `ALTER TABLE orders`
// names `orders`, which is a different relation and does not exist. Measured on
// PostgreSQL 17.10 with `-v ON_ERROR_STOP=1`, psql exits 3 on this file with
// `relation "orders" does not exist`, and the pinned Atlas community v1.3.0
// binary exits 1 with `read state from "schema.sql": ... pq: relation "orders"
// does not exist (42P01)`.
//
// Ptah must not exit 0 where that binary exits 1. Binding the reference to the
// case-preserving declaration instead applied cleanly and put the policy on
// `ORDERS`, leaving the relation the author named with `relrowsecurity = f` and
// no policy at all — a silently relocated access-control declaration.
func TestSchemaApplyRLSReferenceToUndeclaredRelationRefusedLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	targetURL, devURL := createRLSSpellingDatabases(t, adminURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		`CREATE TABLE "ORDERS" (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON orders FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaApply(targetURL, devURL, schemaPath)

	// The rehearsal on the dev database reproduces PostgreSQL's own answer, so
	// the apply is refused and the target keeps no policy on either spelling.
	c.Assert(err, qt.ErrorMatches, `(?s)dev database simulation failed during plan: .*relation "orders" does not exist.*the plan was not applied to the target database`)
	c.Assert(out, qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(rlsPolicyRows(t, targetURL), qt.DeepEquals, []string{})
}

// TestSchemaApplyRLSDeclaredRelationAcceptedLivePostgres is the
// non-interference control: the same surface, the same command, a file
// PostgreSQL accepts. The pinned Atlas community v1.3.0 binary exits 0 on it,
// and so must Ptah — twice, converging on the second run.
//
// Every table here is declared in the case the row-level security declarations
// name, and one of them names it in the other case, which is the fold this
// resolver exists for: an unquoted `ORDERS` reaches the table declared
// `orders`. Measured on PostgreSQL 17.10, replaying the render of this file
// exits 0 with both policies in pg_policy.
func TestSchemaApplyRLSDeclaredRelationAcceptedLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	targetURL, devURL := createRLSSpellingDatabases(t, adminURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
CREATE TABLE shipments (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE ORDERS ENABLE ROW LEVEL SECURITY;
ALTER TABLE shipments ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON ORDERS FOR ALL TO PUBLIC USING (tenant_id = 1);
CREATE POLICY tenant_isolation ON shipments FOR ALL TO PUBLIC USING (tenant_id = 2);
`), 0o600), qt.IsNil)

	firstOut, firstErr := runCompatSchemaApply(targetURL, devURL, schemaPath)
	secondOut, secondErr := runCompatSchemaApply(targetURL, devURL, schemaPath)

	// One policy name per table, both applied, and the second run has nothing
	// left to do.
	c.Assert(firstErr, qt.IsNil, qt.Commentf("%s", firstOut))
	c.Assert(firstOut, qt.Contains, "Schema apply completed successfully.")
	c.Assert(secondErr, qt.IsNil, qt.Commentf("%s", secondOut))
	c.Assert(secondOut, qt.Contains, "Schema is synced, no changes to be made")
	c.Assert(rlsPolicyRows(t, targetURL), qt.DeepEquals, []string{
		"orders/tenant_isolation",
		"shipments/tenant_isolation",
	})
}

// TestSchemaApplyRLSQuotedReferenceRefusedLivePostgres is the other side of the
// quoting boundary, and the one the case fold got wrong.
//
// `CREATE POLICY p ON "ORDERS"` names the relation `ORDERS`, which this file
// does not declare. Measured on PostgreSQL 17.10 with `-v ON_ERROR_STOP=1`,
// psql exits 1 on this exact statement pair with `relation "ORDERS" does not
// exist`.
//
// Ptah folded the reference down to the declaration and rendered
// `CREATE POLICY "p" ON "orders"` instead, which the same server accepts: exit
// 0, one pg_policy row on `public.orders`. The author asked for a policy on
// `ORDERS` and got one on `orders` -- an access-control declaration moved to a
// relation nobody named (stokaro/ptah#1311). Refusing is the only safe answer
// when the alternative is guessing which relation was meant.
func TestSchemaApplyRLSQuotedReferenceRefusedLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	targetURL, devURL := createRLSSpellingDatabases(t, adminURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON "ORDERS" FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	out, err := runCompatSchemaApply(targetURL, devURL, schemaPath)

	c.Assert(err, qt.ErrorMatches, `(?s)dev database simulation failed during plan: .*relation "ORDERS" does not exist.*the plan was not applied to the target database`)
	c.Assert(out, qt.Not(qt.Contains), "Schema apply completed successfully.")
	// Neither spelling is protected, and in particular `orders` did not quietly
	// acquire the policy the file put on `ORDERS`.
	c.Assert(qualifiedRLSPolicyRows(t, targetURL), qt.DeepEquals, []string{})
}

// executeRenderedSchema renders a schema file the way `ptah schema render` does
// and executes every statement against dbURL in order, returning the first
// execution error.
//
// Rendering is not applying, and for an access-control declaration the only
// question worth answering is which relation the statement actually reached. So
// these rows run the DDL and then read pg_policy.
func executeRenderedSchema(t *testing.T, dbURL, schemaPath string) error {
	t.Helper()
	c := qt.New(t)
	database, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{schemaPath}})
	c.Assert(err, qt.IsNil)
	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		if _, execErr := conn.ExecContext(context.Background(), statement); execErr != nil {
			return execErr
		}
	}
	return nil
}

// TestRenderedRLSQualifiedMixedCaseReferenceLandsOnTheNamedRelationLivePostgres
// is the form the whole-string fold could not reach at all.
//
// `"App".ORDERS` has one quoted component and one unquoted one, and PostgreSQL
// answers them separately: the schema keeps its case, the table loses its own.
// Measured on PostgreSQL 17.10, the source file's own policy statement lands on
// `App.orders` with exit 0.
//
// Folding the complete identity asked whether `App.ORDERS` folds to itself. It
// does not, because of the mixed-case schema, so the reference kept
// `App.ORDERS` and the render was answered with `relation "App.ORDERS" does not
// exist` (exit 1, measured) -- a schema file PostgreSQL accepts that Ptah could
// not replay. The render is executed here, and pg_policy is asked which
// relation carries the policy.
//
// This row goes through `schema render` rather than `schema apply` because the
// apply planner emits its schema precondition as raw unquoted SQL
// (`CREATE SCHEMA IF NOT EXISTS App`), which PostgreSQL folds to `app`, so
// every following statement in `"App"` is refused with `schema "App" does not
// exist`. That is a separate pre-existing defect about schema preconditions,
// not about row-level security, and it is not this branch's subject; it is
// recorded rather than worked around.
func TestRenderedRLSQualifiedMixedCaseReferenceLandsOnTheNamedRelationLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	targetURL, _ := createRLSSpellingDatabases(t, adminURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		`CREATE SCHEMA "App";
CREATE TABLE "App".orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE "App".ORDERS ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON "App".ORDERS FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	c.Assert(executeRenderedSchema(t, targetURL, schemaPath), qt.IsNil)

	c.Assert(qualifiedRLSPolicyRows(t, targetURL), qt.DeepEquals, []string{
		"App/orders/tenant_isolation",
	})
}

// TestRenderedRLSQuotedReferenceIsRefusedByPostgres is the executed half of the
// blocker: the render of a file whose policy names `"ORDERS"` must be refused
// by the server exactly as the file is, and must leave `orders` unprotected.
//
// The fold produced `CREATE POLICY "p" ON "orders"`, which the server accepts
// with exit 0 and a pg_policy row on `public.orders` -- the relocation, seen in
// the catalog rather than in a string comparison.
func TestRenderedRLSQuotedReferenceIsRefusedByPostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	targetURL, _ := createRLSSpellingDatabases(t, adminURL)
	schemaPath := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INTEGER);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON "ORDERS" FOR ALL TO PUBLIC USING (tenant_id = 1);
`), 0o600), qt.IsNil)

	err := executeRenderedSchema(t, targetURL, schemaPath)

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "ORDERS" does not exist.*`)
	c.Assert(qualifiedRLSPolicyRows(t, targetURL), qt.DeepEquals, []string{})
}
