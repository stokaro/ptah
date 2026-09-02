//go:build integration

package dbschema_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// evolvedEnumMembers is the order the server holds after the fixture's inserts,
// which is neither creation order nor alphabetical. Measured on PostgreSQL 17,
// pg_enum.enumsortorder for these six members reads -1, 0, 1, 1.5, 2, 3, and
// its type is `real`.
var evolvedEnumMembers = []string{"planned", "draft", "queued", "processing", "sent", "archived"}

// TestReadSchema_LiveEnumEvolvedWithBeforeAndAfter covers stokaro/ptah#2719.
//
// enumsortorder is a float4, so ADD VALUE ... BEFORE places a member at a
// FRACTIONAL position between its neighbors and repeated inserts before the
// first member go through zero into the negatives. The reader selected that
// column and scanned it into a Go int purely to discard it, so the first
// fractional position failed the scan and took the whole schema read with it:
//
//	failed to scan enum: sql: Scan error on column index 2,
//	name "enumsortorder": converting driver.Value type float64 ("1.5")
//	to a int: invalid syntax
//
// A unit test can assert the reader no longer asks for the column. Only a live
// server assigns the positions, so only this test proves the shape it produces
// is one Ptah can read -- and it is the half that keeps working if the
// projection is ever restored with a wider scan destination.
//
// The negative and zero positions matter as much as the fraction, in the
// opposite direction: both scan into an int perfectly well, so a fixture built
// only from BEFORE-the-first inserts would have reported this reader healthy.
func TestReadSchema_LiveEnumEvolvedWithBeforeAndAfter(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()

	conn, schemaName := prepareEvolvedEnumFixture(c, ctx)

	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

	c.Assert(err, qt.IsNil)
	c.Assert(enumNames(schema.Enums), qt.DeepEquals, []string{"ptah_delivery_state"})
	c.Assert(schema.Enums[0].Values, qt.DeepEquals, evolvedEnumMembers)
}

// TestReadSchema_LiveEvolvedEnumSerializesIdentically is the fingerprint half of
// the same issue. catalog.Database.Enums serializes as a JSON slice that schema
// fingerprints and a plan artifact's current_schema_digest hash, so a member
// order that moved between two reads of an unchanged database reads as drift.
//
// Dropping the projected column moved the whole ordering promise onto the
// query's ORDER BY, and this asserts the promise is kept across repeats rather
// than assuming a single read that looked right is reproducible.
func TestReadSchema_LiveEvolvedEnumSerializesIdentically(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()

	conn, schemaName := prepareEvolvedEnumFixture(c, ctx)

	// One fixed baseline, taken once. Comparing each read against the one
	// before it passes on an order that drifts a step at a time.
	baseline, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	want, err := json.Marshal(baseline.Enums)
	c.Assert(err, qt.IsNil)
	c.Assert(string(want), qt.Contains, `"processing"`)

	for read := range readsPerCase {
		t.Run(fmt.Sprintf("read %d", read+1), func(t *testing.T) {
			c := qt.New(t)

			schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
			c.Assert(err, qt.IsNil)

			serialized, err := json.Marshal(schema.Enums)
			c.Assert(err, qt.IsNil)
			c.Assert(string(serialized), qt.Equals, string(want))
		})
	}
}

// prepareEvolvedEnumFixture builds one enum whose members were added out of
// order, using both directions and reaching past the first member twice.
//
// The statements are separate because ALTER TYPE ... ADD VALUE cannot run in a
// transaction block on older servers, and each one's position depends on the
// one before it.
func prepareEvolvedEnumFixture(c *qt.C, ctx context.Context) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	schemaName := fmt.Sprintf("ptah_enum_evolution_%d", time.Now().UnixNano())
	dropSchema(c, conn, schemaName)
	c.Cleanup(func() { dropSchema(c, conn, schemaName) })

	statements := []string{
		fmt.Sprintf("CREATE SCHEMA %q", schemaName),
		fmt.Sprintf("CREATE TYPE %q.ptah_delivery_state AS ENUM ('queued', 'sent')", schemaName),
		// 1.5: the fractional position, and the one that failed the scan.
		fmt.Sprintf("ALTER TYPE %q.ptah_delivery_state ADD VALUE 'processing' BEFORE 'sent'", schemaName),
		// 0, then -1: BEFORE the first member walks down through zero.
		fmt.Sprintf("ALTER TYPE %q.ptah_delivery_state ADD VALUE 'draft' BEFORE 'queued'", schemaName),
		fmt.Sprintf("ALTER TYPE %q.ptah_delivery_state ADD VALUE 'planned' BEFORE 'draft'", schemaName),
		// 3: AFTER the last, so the ordering is not only ever prepended to.
		fmt.Sprintf("ALTER TYPE %q.ptah_delivery_state ADD VALUE 'archived' AFTER 'sent'", schemaName),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute %s", statement))
	}
	return conn, schemaName
}
