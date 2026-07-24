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

// TestPostgreSQLUserTypesRoundTripIntegration verifies the critical invariant of
// the user-defined-type feature: a generate -> apply -> introspect -> compare
// round-trip is clean, including a domain with a CHECK, a domain over a
// non-canonical base type (VARCHAR), and a composite type with a parameterized
// field type (NUMERIC(10,2)) — the exact shapes that expose the readback
// normalization and comma-in-type parsing pitfalls.
func TestPostgreSQLUserTypesRoundTripIntegration(t *testing.T) {
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

//migrator:schema:domain name="email" type="TEXT" check="VALUE ~ '@'"
type EmailDomain struct{}

//migrator:schema:domain name="pincode" type="VARCHAR(255)" not_null="true"
type PinDomain struct{}

//migrator:schema:composite name="money_amount" fields="amount:NUMERIC(10,2),cur:VARCHAR(3)"
type MoneyType struct{}

//migrator:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
type FloatRange struct{}
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
	c.Assert(live.Domains, qt.HasLen, 2)
	c.Assert(live.Composites, qt.HasLen, 1)
	c.Assert(live.Ranges, qt.HasLen, 1)

	roundTrip := schemadiff.CompareWithDialect(desired, live, "postgres")
	c.Assert(roundTrip.HasChanges(), qt.IsFalse, qt.Commentf(
		"user types must survive apply->introspect->compare; domains+=%v ~=%v composites+=%v ~=%v ranges+=%v",
		roundTrip.DomainsAdded, roundTrip.DomainsModified,
		roundTrip.CompositeTypesAdded, roundTrip.CompositeTypesModified, roundTrip.RangesAdded))

	// A SERIAL column's row type must not be surfaced as a composite type.
	for _, composite := range live.Composites {
		c.Assert(composite.Name, qt.Not(qt.Equals), "money_amount_rowtype")
	}
}
