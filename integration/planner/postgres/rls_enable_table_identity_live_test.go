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

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
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
func planAndApply(c *qt.C, dbURL string, diff *types.SchemaDiff, generated *goschema.Database) []string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, "postgres")
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
	found := []string{}
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		found = append(found, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// ordersSchema returns the target schema for a single `orders` table whose
// policy names the owning table with policyTable.
func ordersSchema(tableSchema, policyTable string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "orders", Schema: tableSchema, StructName: "Order"}},
		Fields: []goschema.Field{
			{Name: "id", StructName: "Order", Type: "INTEGER", Primary: true},
			{Name: "tenant_id", StructName: "Order", Type: "INTEGER"},
		},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "tenant_isolation",
			Table:           policyTable,
			PolicyFor:       "ALL",
			ToRoles:         "PUBLIC",
			UsingExpression: "tenant_id = 1",
		}},
	}
}

// TestPlannerEnablesRowSecurityForANewTableWhoseSpellingDiffersLivePostgres is
// the fourth instance of one mistake, asserted where it is observable.
//
// `enableRLSOnTables` decided whether a policy's owning table is new by asking
// `slices.Contains(diff.TablesAdded, policy.Table)` -- a raw string comparison
// -- while `addNewRLSPolicies` resolves the same table through the target's
// identifier semantics (stokaro/ptah#1311, stokaro/ptah#1347). `orders` and
// `public.orders` are one relation under PostgreSQL's rules and two different
// strings, so the plan emitted `CREATE POLICY` and no
// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`.
//
// Measured on PostgreSQL 17.10: the plan applies cleanly, pg_policy carries the
// policy, and pg_class.relrowsecurity for the relation is false. The policy is
// inert -- row-level security is not enforced on a table the author asked to
// protect -- and the plan reported success. That is why these rows read the
// catalog rather than the SQL: "the plan lacks a statement" and "the database
// does not enforce the policy" are different claims and only the second one
// matters.
func TestPlannerEnablesRowSecurityForANewTableWhoseSpellingDiffersLivePostgres(t *testing.T) {
	adminURL := livePostgresURLForRLSEnable(t)

	tests := []struct {
		name string
		// seed runs against the fresh database before the plan, for rows whose
		// subject is a table the diff does not create.
		seed      []string
		diff      *types.SchemaDiff
		generated *goschema.Database
		// wantPolicies and wantRowSecurity are the catalog after the plan ran.
		// A row that expects neither spells `[]string{}` rather than leaving the
		// field out, because qt.DeepEquals separates an empty slice from a nil
		// one and queryStrings never returns nil.
		wantPolicies    []string
		wantRowSecurity []string
	}{
		{
			name: "the diff creates orders and the policy names public.orders",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"orders"},
				RLSPoliciesAdded: []types.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "public.orders"},
				},
			},
			generated:       ordersSchema("", "public.orders"),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			name: "the diff creates public.orders and the policy names orders",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"public.orders"},
				RLSPoliciesAdded: []types.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "orders"},
				},
			},
			generated:       ordersSchema("public", "orders"),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			name: "both sides spell the table the same way",
			diff: &types.SchemaDiff{
				TablesAdded: []string{"orders"},
				RLSPoliciesAdded: []types.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "orders"},
				},
			},
			generated:       ordersSchema("", "orders"),
			wantPolicies:    []string{"public/orders/tenant_isolation"},
			wantRowSecurity: []string{"public/orders"},
		},
		{
			// The control the widened match must not swallow. `legacy` already
			// exists, keeps its declared policy, and is in no diff collection;
			// enabling row-level security on it would deny by default on a table
			// nothing in this plan touches. A fix that enabled every declared
			// policy's table instead of the ones the diff creates fails here.
			name: "a policy on a table the diff does not create leaves it alone",
			seed: []string{
				`CREATE TABLE legacy (id INTEGER PRIMARY KEY, tenant_id INTEGER)`,
			},
			diff: &types.SchemaDiff{
				TablesAdded: []string{"shipments"},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "shipments", StructName: "Shipment"},
					{Name: "legacy", StructName: "Legacy"},
				},
				Fields: []goschema.Field{
					{Name: "id", StructName: "Shipment", Type: "INTEGER", Primary: true},
					{Name: "id", StructName: "Legacy", Type: "INTEGER", Primary: true},
				},
				RLSPolicies: []goschema.RLSPolicy{{
					Name:            "tenant_isolation",
					Table:           "legacy",
					PolicyFor:       "ALL",
					ToRoles:         "PUBLIC",
					UsingExpression: "tenant_id = 1",
				}},
			},
			wantPolicies:    []string{},
			wantRowSecurity: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbURL := createRLSEnableDatabase(c, adminURL)
			executeSQL(c, dbURL, test.seed)
			planAndApply(c, dbURL, test.diff, test.generated)
			c.Assert(rlsPolicyRelations(c, dbURL), qt.DeepEquals, test.wantPolicies)
			c.Assert(rowSecurityRelations(c, dbURL), qt.DeepEquals, test.wantRowSecurity)
		})
	}
}
