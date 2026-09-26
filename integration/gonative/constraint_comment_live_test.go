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
// The read is deliberately not Ptah's own. Ptah reads the comment back to
// compare it (stokaro/ptah#3678), so asking Ptah whether the comment is there
// would ask the code under test to grade itself.

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
// every time. The comparison reads the comment back and compares it, so a
// comment written where the reader does not look would be planned again on
// every run.
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

// orderEntities is the reproduction with attribute written after the
// constraint's other attributes: a comment attribute, or nothing.
func orderEntities(attribute string) string {
	return `package models

//ptah:schema:table name="ptah3678_orders"
//ptah:schema:constraint name="ck_ptah3678_orders_total" type="CHECK" check="total > 0"` + attribute + `
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="total" type="BIGINT"
	Total int64
}
`
}

// TestConstraintCommentChangesInPlaceIntegration applies a declaration, then the
// same declaration with the constraint's comment changed and then removed, and
// asks the server after each (stokaro/ptah#3678). Every apply converges: a
// comparison right after it finds nothing to do.
//
// YugabyteDB stores a constraint's comment too, and the reader's live test reads
// it there. It is not a row here because `schema apply` cannot yet apply
// anything to a YugabyteDB database (stokaro/ptah#3687).
func TestConstraintCommentChangesInPlaceIntegration(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := dbtarget.URL(t, test.engine)
			dsn := requireReachableEngine(t, test.engine, "pgx", test.name)
			dir := t.TempDir()
			c.Cleanup(func() { dropTable(c, dsn, "ptah3678_orders") })

			steps := []struct {
				attribute string
				want      string
			}{
				{attribute: ` comment="a total is positive"`, want: "a total is positive"},
				{attribute: ` comment="a total is above zero"`, want: "a total is above zero"},
				{attribute: "", want: ""},
			}
			for _, step := range steps {
				c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(orderEntities(step.attribute)), 0o600), qt.IsNil)

				runNativePtah(c, "schema", "apply", "--db-url", url, "--root-dir", dir, "--auto-approve")

				c.Assert(constraintCommentInCatalog(c, dsn, "ck_ptah3678_orders_total"), qt.Equals, step.want)
				c.Assert(runNativePtah(c, "schema", "compare", "--db-url", url, "--root-dir", dir),
					qt.Contains, "No schema differences detected")
			}
		})
	}
}

// dropTable removes one table this file created, so a shared server is left as
// it was found.
func dropTable(c *qt.C, dsn, table string) {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.Logf("cleanup could not open the database: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table + " CASCADE"); err != nil {
		c.Logf("cleanup did not drop the table: %v", err)
	}
}
