//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// checkShapes are the CHECK expressions PostgreSQL rewrites on the way in, with
// what it stores. Every pair was measured on 17.11.
//
// The last three are the ones no textual normalizer folds: a cast the parser
// inserts, a BETWEEN it expands into two comparisons, and a cast inside a
// disjunction. Before stokaro/ptah#2044 each of them planned a DROP and an ADD
// on every run, at severity destructive.
var checkShapes = []struct {
	name     string
	column   string
	declared string
}{
	{name: "a comparison the parser only parenthesizes", column: "id bigint", declared: "id > 0"},
	{name: "a null test", column: "note text", declared: "note IS NOT NULL"},
	{name: "a function call", column: "grade text", declared: "length(grade) > 0"},
	{name: "a set membership the parser rewrites", column: "grade2 text", declared: "grade2 IN ('a','b')"},
	{name: "a cast the parser inserts", column: "price numeric(10,2)", declared: "price >= 0"},
	{name: "a BETWEEN the parser expands", column: "score integer", declared: "score BETWEEN 1 AND 10"},
	{name: "a cast inside a disjunction", column: "rank integer", declared: "rank > 0 OR rank = -1"},
}

// TestPostgresCheckExpressionsConvergeE2E pins that a table CHECK Ptah applied
// is not planned again on the next run.
//
// It is live because the rewrite is the server's. A unit test can hold a pair
// of strings; only PostgreSQL can say that `score BETWEEN 1 AND 10` and
// `((score >= 1) AND (score <= 10))` are one constraint, and getting that pair
// wrong in a fixture is how the textual normalizer came to fold some shapes and
// not others.
func TestPostgresCheckExpressionsConvergeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_check_expr_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	scopedURL := replaceDatabaseName(c, dbURL, testDBName)
	setupDB, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	_, err = setupDB.ExecContext(ctx, createCheckTable())
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// Non-vacuity, and the evidence the rewrite happened at all: the server's
	// stored form differs from the declaration for the rows this test is for.
	c.Assert(storedCheckClauses(c, ctx, setupDB), qt.HasLen, len(checkShapes))
	c.Assert(strings.Join(storedCheckClauses(c, ctx, setupDB), " "), qt.Contains, "(0)::numeric")

	declared := checkDeclaration()
	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, declared, read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)

	// The control: a check that really did change is still reported, so the
	// convergence above is not a comparison that always agrees.
	changed := checkDeclaration()
	changed.Constraints[len(changed.Constraints)-1].CheckExpression = "rank > 5 OR rank = -1"
	changedDiff, err := schemadiff.CompareWithDatabase(
		ctx, conn, changed, read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(changedDiff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"ck_6"})
}

// createCheckTable builds one table carrying every shape above.
func createCheckTable() string {
	parts := make([]string, 0, len(checkShapes)*2+1)
	parts = append(parts, "pk integer PRIMARY KEY")
	for _, shape := range checkShapes {
		parts = append(parts, shape.column)
	}
	for i, shape := range checkShapes {
		parts = append(parts, fmt.Sprintf("CONSTRAINT ck_%d CHECK (%s)", i, shape.declared))
	}
	return "CREATE TABLE shapes (" + strings.Join(parts, ", ") + ")"
}

// checkDeclaration is the same table as a description, carrying the expressions
// as they were WRITTEN.
func checkDeclaration() *schemamodel.Database {
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "S", Name: "shapes"}},
		Fields: []schemamodel.Field{{StructName: "S", Name: "pk", Type: "integer", Primary: true}},
	}
	for _, shape := range checkShapes {
		name, columnType, _ := strings.Cut(shape.column, " ")
		declared.Fields = append(declared.Fields, schemamodel.Field{
			StructName: "S", Name: name, Type: columnType, Nullable: true,
		})
	}
	for i, shape := range checkShapes {
		declared.Constraints = append(declared.Constraints, schemamodel.Constraint{
			StructName: "S", Name: fmt.Sprintf("ck_%d", i), Table: "shapes",
			Type: "CHECK", CheckExpression: shape.declared,
		})
	}
	return declared
}

// storedCheckClauses asks the server how it spells every check on the table.
func storedCheckClauses(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT pg_get_expr(conbin, conrelid)
		FROM pg_constraint
		WHERE conrelid = 'shapes'::regclass AND contype = 'c'
		ORDER BY conname`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var clauses []string
	for rows.Next() {
		var clause string
		c.Assert(rows.Scan(&clause), qt.IsNil)
		clauses = append(clauses, clause)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return clauses
}
