//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// readsPerCase is how many times each case re-reads the unchanged schema.
//
// One repeat proves nothing here: Go randomizes map iteration per range, so a
// scrambling read agrees with itself often enough to look stable. Eight reads
// of four objects made the defect appear once in the measurement that motivated
// this test, which is the point -- the assertion is on the ORDER MATCHING THE
// QUERY, not on two reads agreeing, so it fails on the first scrambled read
// rather than waiting for two to differ.
const readsPerCase = 8

// TestReadSchema_LiveEnumOrderIsTheQueryOrder covers stokaro/ptah#2712.
//
// readEnumsForSchema asks PostgreSQL for rows ordered by enum name, then
// grouped them in a map and built the result by ranging it. The values inside
// each enum stayed put and the enum TYPES came back in a different order.
//
// It is observable rather than cosmetic: catalog.Database.Enums serializes as a
// JSON slice, schema fingerprints and a plan artifact's current_schema_digest
// hash that serialization, and `schema apply` re-reads the target to verify the
// fingerprint it saved -- so an untouched database could fail its own check.
func TestReadSchema_LiveEnumOrderIsTheQueryOrder(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
	defer cancel()

	conn, schemaName := prepareEnumOrderFixture(c, ctx)
	want := []string{"ptah_order_aa", "ptah_order_mm", "ptah_order_zz"}

	for read := range readsPerCase {
		t.Run(fmt.Sprintf("read %d", read+1), func(t *testing.T) {
			c := qt.New(t)

			schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

			c.Assert(err, qt.IsNil)
			c.Assert(enumNames(schema.Enums), qt.DeepEquals, want)
		})
	}
}

// TestReadSchema_LiveConstraintOrderIsTheQueryOrder covers stokaro/ptah#2709,
// which is the same defect in the MySQL reader: an ORDER BY the grouping map
// discarded.
func TestReadSchema_LiveConstraintOrderIsTheQueryOrder(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQL},
		{name: "MariaDB", engine: dbtarget.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			defer cancel()

			conn, schemaName := prepareConstraintOrderFixture(c, ctx, test.engine)

			// The first read fixes the expectation and every later one has to
			// match it, so a scramble fails whichever read it lands on. It is
			// taken outside the loop because a conditional in a test body is a
			// style violation, and because the two reads are different claims:
			// this one says the fixture is visible at all.
			first, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
			c.Assert(err, qt.IsNil)
			want := uniqueConstraintNames(first.Constraints)
			c.Assert(want, qt.HasLen, 3)

			for read := range readsPerCase - 1 {
				schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
				c.Assert(err, qt.IsNil)
				c.Assert(uniqueConstraintNames(schema.Constraints), qt.DeepEquals, want,
					qt.Commentf("read %d", read+2))
			}
		})
	}
}

func enumNames(enums []catalog.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func uniqueConstraintNames(constraints []catalog.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		names = append(names, constraint.Name)
	}
	return names
}
