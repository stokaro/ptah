package postgres

// White-box testing required: basicConstraintsQuery builds a query string, and
// the property under test is a filter that string carries. Observing it from
// outside the package takes a live PostgreSQL 18, which
// integration/named_not_null_e2e_test.go does.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// TestBasicConstraintsQuery_LeavesANotNullToItsColumn pins the filter that
// keeps a NOT NULL out of the constraint list.
//
// PostgreSQL 18 catalogs each NOT NULL in pg_constraint as contype 'n', and
// information_schema lists it as a CHECK. Read into the constraint list, a
// NOT NULL named by its author is a CHECK with no condition that no schema
// declares, and the comparison plans `DROP CONSTRAINT <name>`, which drops the
// NOT NULL (stokaro/ptah#3927). The name is read with the column instead; see
// notNullConstraintNameExpr. PostgreSQL 17 has no such rows, so the filter is
// the same query on both.
func TestBasicConstraintsQuery_LeavesANotNullToItsColumn(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
	}{
		{name: "PostgreSQL 17", caps: capability.Postgres17()},
		{name: "PostgreSQL 18", caps: capability.Postgres18()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewPostgreSQLReaderWithCapabilities(nil, "public", test.caps)

			query := reader.basicConstraintsQuery()

			c.Assert(query, qt.Contains, "AND pc.contype <> 'n'")
		})
	}
}
