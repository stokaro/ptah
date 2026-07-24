//go:build integration

package gonative_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestPostgreSQLNextvalDefaultRoundTripIntegration verifies that a column whose
// default draws from a standalone sequence via nextval('seq') survives a
// generate -> apply -> introspect -> compare round-trip cleanly (issue #675).
// PostgreSQL reads the default back as nextval('seq'::regclass) and marks the
// column auto-increment; neither must produce a phantom default or primary-key
// difference, and the actual SERIAL primary key must still be detected.
func TestPostgreSQLNextvalDefaultRoundTripIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.Exec("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public")
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public") }()

	dir := t.TempDir()
	model := `package models

//migrator:schema:sequence name="order_number_seq" as="bigint" start="1000"
type OrderNumberSeq struct{}

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="order_number" type="BIGINT" not_null="true" default_expr="nextval('order_number_seq')"
	OrderNumber int64
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(model), 0o600), qt.IsNil)

	desired, err := goschema.ParseDir(dir)
	c.Assert(err, qt.IsNil)

	stmts, err := renderer.GetOrderedCreateStatements(desired, "postgres")
	c.Assert(err, qt.IsNil)
	for _, stmt := range stmts {
		_, err = db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("statement failed: %s", stmt))
	}

	live, err := postgres.NewPostgreSQLReader(db, "public").ReadSchema()
	c.Assert(err, qt.IsNil)

	roundTrip := schemadiff.CompareWithDialect(desired, live, "postgres")
	c.Assert(roundTrip.HasChanges(), qt.IsFalse, qt.Commentf(
		"nextval-default column must round-trip; tablesModified=%d", len(roundTrip.TablesModified)))
}
