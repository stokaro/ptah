//go:build integration

// Live guard for stokaro/ptah#2611: a table constraint's comment reaches the
// server's catalog rather than only the printed plan.
//
// Why a live server. The renderer test beside this one proves the statement is
// PRINTED, and that is exactly what an incomplete fix passed: teaching the
// CREATE TABLE path alone left `schema render` showing the comment and every
// applied database without it, because `schema apply` reaches an existing table
// through ALTER. Only pg_description answers which of the two happened.
//
// The read is deliberately not Ptah's own. No catalog reader fills
// schemamodel.Constraint.Comment, so asking Ptah to read the comment back would
// be asking a reader that cannot see it, and a test built on that would pass
// against a database holding nothing.

package gonative_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// constraintCommentEntities is the reproduction from the issue: one CHECK
// constraint carrying the documented `comment` attribute.
const constraintCommentEntities = `package models

//ptah:schema:table name="ptah2611_orders"
//ptah:schema:constraint name="ck_ptah2611_orders_total" type="CHECK" check="total > 0" comment="a total is positive"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="total" type="BIGINT"
	Total int64
}
`

// TestConstraintCommentReachesTheCatalogIntegration applies the entities and
// asks PostgreSQL what it stored.
func TestConstraintCommentReachesTheCatalogIntegration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(t, dbtarget.PostgreSQL)
	dsn := requireReachableEngine(t, dbtarget.PostgreSQL, "pgx", "PostgreSQL")
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"),
		[]byte(constraintCommentEntities), 0o600), qt.IsNil)
	c.Cleanup(func() { dropConstraintCommentTable(c, dsn) })

	runNativePtah(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")

	c.Assert(constraintCommentInCatalog(c, dsn, "ck_ptah2611_orders_total"),
		qt.Equals, "a total is positive")
}

// TestConstraintCommentApplyStaysIdempotentIntegration is the control that
// keeps the test above from being satisfied by a run that rewrites the schema
// every time.
//
// A comment written on one side and read by neither is the shape that produces
// a database Ptah keeps trying to change: no catalog reader fills the field, so
// if the comparison consulted it, every run would plan the same statement
// again. It does not, and this is what says so.
func TestConstraintCommentApplyStaysIdempotentIntegration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(t, dbtarget.PostgreSQL)
	dsn := requireReachableEngine(t, dbtarget.PostgreSQL, "pgx", "PostgreSQL")
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"),
		[]byte(constraintCommentEntities), 0o600), qt.IsNil)
	c.Cleanup(func() { dropConstraintCommentTable(c, dsn) })

	runNativePtah(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")
	second := runNativePtah(c, "schema", "compare", "--db-url", url, "--root-dir", dir)

	c.Assert(second, qt.Contains, "No schema differences detected")
	c.Assert(constraintCommentInCatalog(c, dsn, "ck_ptah2611_orders_total"),
		qt.Equals, "a total is positive")
}

// constraintCommentInCatalog returns what pg_description holds for one named
// constraint, and the empty string when it holds nothing.
func constraintCommentInCatalog(c *qt.C, dsn, constraint string) string {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var comment sql.NullString
	err = db.QueryRow(
		"SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname = $1",
		constraint).Scan(&comment)
	c.Assert(err, qt.IsNil, qt.Commentf("constraint %q was not created at all", constraint))
	return comment.String
}

// dropConstraintCommentTable removes what this file created, so a shared server
// is left as it was found.
func dropConstraintCommentTable(c *qt.C, dsn string) {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.Logf("cleanup could not open the database: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS ptah2611_orders CASCADE"); err != nil {
		c.Logf("cleanup did not drop the table: %v", err)
	}
}
