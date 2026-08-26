//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestPostgresIndexAndPolicyExpressionsConvergeE2E pins that an index and an
// RLS policy Ptah applied are not planned again on the next run.
//
// Both carry expressions PostgreSQL rewrites on the way in, and the rewrite
// depends on the type of the column the expression names. Measured on 17.11:
//
//	lower(code)  over varchar -> lower(code::text)
//	unit >= 0    over numeric -> (unit >= (0)::numeric)
//	owner = 'x'  over varchar -> ((owner)::text = 'x'::text)
//
// Nothing in the declaration says the column is a varchar, so no rule over its
// text can decide whether the cast appears -- which is why both are put through
// the server instead (stokaro/ptah#2047, stokaro/ptah#2049).
//
// It is live because the rewrite is the server's. A unit test would have to
// hard-code the stored halves, which is the half that was wrong.
func TestPostgresIndexAndPolicyExpressionsConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_expr_converge_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	for _, statement := range []string{
		`CREATE TABLE orders (
			id integer PRIMARY KEY,
			code varchar(40) NOT NULL,
			owner varchar(60) NOT NULL,
			unit numeric(10,2) NOT NULL
		)`,
		`CREATE INDEX idx_lower_code ON orders (lower(code))`,
		`CREATE INDEX idx_partial ON orders (code) WHERE unit >= 0`,
		`ALTER TABLE orders ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY p_owner ON orders FOR SELECT USING (owner = 'x')`,
	} {
		_, err = setupDB.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}

	conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The evidence the rewrite happened at all. Without this a server that
	// stored the text it was given would make the convergence below vacuous.
	c.Assert(storedIndexKey(c, ctx, setupDB, "idx_lower_code"), qt.Contains, "::text")
	c.Assert(storedPolicyQual(c, ctx, setupDB, "p_owner"), qt.Contains, "::text")

	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, expressionDeclaration("lower(code)", "unit >= 0", "owner = 'x'"),
		read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexesAdded, qt.HasLen, 0)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)

	// The controls: a real change to each is still reported, so the
	// convergence above is not a comparison that always agrees.
	changedIndex, err := schemadiff.CompareWithDatabase(
		ctx, conn, expressionDeclaration("upper(code)", "unit >= 0", "owner = 'x'"),
		read, config.DefaultCompareOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(changedIndex.IndexesAdded, qt.Not(qt.HasLen), 0)

	changedPolicy, err := schemadiff.CompareWithDatabase(
		ctx, conn, expressionDeclaration("lower(code)", "unit >= 0", "owner = 'y'"),
		read, config.DefaultCompareOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(changedPolicy.RLSPoliciesModified, qt.HasLen, 1)
}

// expressionDeclaration is the same schema as a description, carrying the
// expressions as they were WRITTEN.
func expressionDeclaration(indexExpr, predicate, using string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "O", Name: "orders"}},
		Fields: []goschema.Field{
			{StructName: "O", Name: "id", Type: "integer", Primary: true},
			{StructName: "O", Name: "code", Type: "varchar(40)"},
			{StructName: "O", Name: "owner", Type: "varchar(60)"},
			{StructName: "O", Name: "unit", Type: "numeric(10,2)"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "O", Name: "idx_lower_code", TableName: "orders",
				Parts: []goschema.IndexPart{{Expr: indexExpr}},
			},
			{
				StructName: "O", Name: "idx_partial", TableName: "orders",
				Fields: []string{"code"}, Condition: predicate,
			},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{{StructName: "O", Table: "orders"}},
		RLSPolicies: []goschema.RLSPolicy{{
			StructName: "O", Name: "p_owner", Table: "orders",
			PolicyFor: "SELECT", ToRoles: "PUBLIC", UsingExpression: using,
		}},
	}
}

// storedIndexKey asks the server how it spells an index's first key.
func storedIndexKey(c *qt.C, ctx context.Context, db *sql.DB, name string) string {
	c.Helper()
	var stored string
	err := db.QueryRowContext(ctx, `
		SELECT pg_get_indexdef(i.indexrelid, 1, true)
		FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`, name).Scan(&stored)
	c.Assert(err, qt.IsNil)
	return stored
}

// storedPolicyQual asks the server how it spells a policy's USING clause.
func storedPolicyQual(c *qt.C, ctx context.Context, db *sql.DB, name string) string {
	c.Helper()
	var stored string
	err := db.QueryRowContext(ctx, `
		SELECT pg_get_expr(p.polqual, p.polrelid)
		FROM pg_policy p WHERE p.polname = $1`, name).Scan(&stored)
	c.Assert(err, qt.IsNil)
	return stored
}
