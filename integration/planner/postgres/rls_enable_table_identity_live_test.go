//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/pgpolicysource"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// livePostgresURLForRLSEnable gates these rows on the same environment
// variables as the other live PostgreSQL tests.
func livePostgresURLForRLSEnable(t *testing.T) string {
	t.Helper()
	return dbtarget.URL(t, dbtarget.PostgreSQL)
}

// createRLSEnableDatabase provisions one empty database per row and registers
// its removal. The shared development server is dirty, and a row that asks
// pg_class whether row-level security is on needs a relation nothing else
// touched.
func createRLSEnableDatabase(c *qt.C, adminURL string) string {
	c.Helper()
	name := fmt.Sprintf("ptah_rlsenable_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	conn, err := dbschema.ConnectToDatabase(context.Background(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		dbschema.CloseAndWarn(conn)
	})
	_, err = conn.ExecContext(context.Background(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// executeSQL runs every statement in order and fails on the first error.
func executeSQL(c *qt.C, dbURL string, statements []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(context.Background(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// planAndApply plans the diff for PostgreSQL, executes the plan against dbURL
// and returns the statements it ran.
//
// Rendering is not applying. The claim these rows make is about what the
// database enforces afterwards, so the plan is executed and the catalog is
// asked; a string assertion on the SQL cannot distinguish a policy that
// protects rows from one that is inert.
func planAndApply(c *qt.C, dbURL string, diff *difftypes.SchemaDiff, desired *schemamodel.Database) []string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		diff, "postgres",
	)
	c.Assert(err, qt.IsNil)
	c.Logf("planned SQL:\n%s", strings.Join(statements, "\n"))
	executeSQL(c, dbURL, statements)
	return statements
}

// rowSecurityRelations reports every user-schema relation whose
// pg_class.relrowsecurity is true, as `nspname/relname` pairs.
func rowSecurityRelations(c *qt.C, dbURL string) []string {
	c.Helper()
	return queryStrings(c, dbURL,
		`SELECT n.nspname || '/' || c.relname
		   FROM pg_class c
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE c.relrowsecurity
		    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  ORDER BY 1`)
}

// rlsPolicyRelations reports every user-schema row-level security policy as an
// `nspname/relname/polname` triple.
func rlsPolicyRelations(c *qt.C, dbURL string) []string {
	c.Helper()
	return queryStrings(c, dbURL,
		`SELECT n.nspname || '/' || c.relname || '/' || p.polname
		   FROM pg_policy p
		   JOIN pg_class c ON c.oid = p.polrelid
		   JOIN pg_namespace n ON n.oid = c.relnamespace
		  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		  ORDER BY 1`)
}

func queryStrings(c *qt.C, dbURL, query string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), query)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	found := make([]string, 0)
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// ordersSchema returns the target schema for a single `orders` table whose
// policy names the owning table's schema with policySchema, and declares no
// switches, which leaves them to the row-security owner's default.
func ordersSchema(tableSchema, policySchema string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "orders", Schema: tableSchema, StructName: "Order"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Order", Type: "INTEGER", Primary: true},
			{Name: "tenant_id", StructName: "Order", Type: "INTEGER"},
		},
	}
	return withPolicy(db, policySchema, "orders")
}

// withPolicy declares the policy `tenant_id = 1` on table and, as a document
// that names a table's policies and not its switches does, leaves the
// switches to the row-security owner's default.
func withPolicy(db *schemamodel.Database, policySchema, table string) *schemamodel.Database {
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
		pgpolicy.PolicyRef(policySchema, table, "tenant_isolation"),
		pgpolicy.DesiredPolicy{Using: new("tenant_id = 1")}))))
	db.FeatureCoverage = must.Must(pgpolicysource.Claim(must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)), db.FeatureObjects))
	return db
}

// planFromLiveRead reads the database, compares desired with it, and applies
// the plan.
func planFromLiveRead(c *qt.C, dbURL string, desired *schemamodel.Database) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	dbschema.CloseAndWarn(conn)
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, live, "postgres", must.Must(builtin.New())))
	return planAndApply(c, dbURL, diff, desired)
}

// TestPlannerEnablesRowSecurityForANewTableWhoseSpellingDiffersLivePostgres
// pins the row-security owner's answer for a table the plan creates: a
// declaration that names the table's policies and not its switches enables
// row-level security on it, however the policy spells the table. `orders` and
// `public.orders` are one relation under PostgreSQL's rules.
//
// The rows read the catalog rather than the SQL, because "the plan lacks a
// statement" and "the database does not enforce the policy" are different
// claims and only the second one matters (stokaro/ptah#1311).
//
// The last two rows are stokaro/ptah#2048 on a table the plan does not create:
// the declaration leaves the switches as the table has them, whether off or
// on. Enabling a table nothing in this plan creates would deny by default on
// it, and disabling one would turn a security control off.
func TestPlannerEnablesRowSecurityForANewTableWhoseSpellingDiffersLivePostgres(t *testing.T) {
	adminURL := livePostgresURLForRLSEnable(t)
	legacy := func() *schemamodel.Database {
		return withPolicy(&schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "legacy", StructName: "Legacy"}},
			Fields: []schemamodel.Field{
				{Name: "id", StructName: "Legacy", Type: "INTEGER", Primary: true},
				{Name: "tenant_id", StructName: "Legacy", Type: "INTEGER"},
			},
		}, "", "legacy")
	}

	tests := []struct {
		name string
		// seed runs against the fresh database before the plan.
		seed            []string
		desired         *schemamodel.Database
		wantPolicies    []string
		wantRowSecurity []string
	}{
		{
			name:            "the plan creates orders and the policy names public.orders",
			desired:         ordersSchema("", "public"),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			name:            "the plan creates public.orders and the policy names orders",
			desired:         ordersSchema("public", ""),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			name:            "both sides spell the table the same way",
			desired:         ordersSchema("", ""),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			name:            "an existing table with row security off keeps it off",
			seed:            []string{`CREATE TABLE legacy (id INTEGER PRIMARY KEY, tenant_id INTEGER)`},
			desired:         legacy(),
			wantPolicies:    []string{"public/legacy/tenant_isolation"},
			wantRowSecurity: make([]string, 0),
		},
		{
			name: "an existing table with row security on keeps it on",
			seed: []string{
				`CREATE TABLE legacy (id INTEGER PRIMARY KEY, tenant_id INTEGER)`,
				`ALTER TABLE legacy ENABLE ROW LEVEL SECURITY`,
				`CREATE POLICY tenant_isolation ON legacy USING (tenant_id = 1)`,
			},
			desired:         legacy(),
			wantPolicies:    []string{"public/legacy/tenant_isolation"},
			wantRowSecurity: []string{"public/legacy"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := createRLSEnableDatabase(c, adminURL)
			executeSQL(c, dbURL, test.seed)
			planFromLiveRead(c, dbURL, test.desired)
			c.Assert(rlsPolicyRelations(c, dbURL), qt.DeepEquals, test.wantPolicies)
			c.Assert(rowSecurityRelations(c, dbURL), qt.DeepEquals, test.wantRowSecurity)
		})
	}
}

// TestPlannerRowSecurityBindsANonOwnerLivePostgres counts the rows a role that
// does not own the table reads after the plan: the policy admits tenant 1, so
// a plan that left row-level security off, or the policy out, shows every row.
// The owner of a table is exempt unless FORCE is on, which is why the count is
// taken as another role.
func TestPlannerRowSecurityBindsANonOwnerLivePostgres(t *testing.T) {
	adminURL := livePostgresURLForRLSEnable(t)
	c := qt.New(t)
	// The role first, so it is dropped after the database that grants to it.
	reader := createReaderRole(c, adminURL)
	dbURL := createRLSEnableDatabase(c, adminURL)

	planFromLiveRead(c, dbURL, ordersSchema("", ""))
	executeSQL(c, dbURL, []string{
		`INSERT INTO orders VALUES (1, 1), (2, 2), (3, 1)`,
		"GRANT SELECT ON orders TO " + reader,
	})

	c.Assert(visibleOrders(c, dbURL, reader), qt.DeepEquals, []string{"1", "3"})
}

// createReaderRole creates a role that owns nothing, removed when the test
// ends. Roles belong to the cluster, so the name is unique to the run.
func createReaderRole(c *qt.C, adminURL string) string {
	c.Helper()
	reader := fmt.Sprintf("ptah_rls_reader_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	executeSQL(c, adminURL, []string{"CREATE ROLE " + reader})
	c.Cleanup(func() {
		executeSQL(c, adminURL, []string{"DROP ROLE IF EXISTS " + reader})
	})
	return reader
}

// visibleOrders lists the ids of the orders role reads.
func visibleOrders(c *qt.C, dbURL, role string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	tx, err := conn.BeginTx(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(context.Background(), "SET LOCAL ROLE "+role)
	c.Assert(err, qt.IsNil)
	rows, err := tx.QueryContext(context.Background(), "SELECT id::text FROM orders ORDER BY id")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		c.Assert(rows.Scan(&id), qt.IsNil)
		ids = append(ids, id)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return ids
}
