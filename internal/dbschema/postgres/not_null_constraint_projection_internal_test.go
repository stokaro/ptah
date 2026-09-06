package postgres

// White-box testing required: notNullConstraintNameExpr builds one expression of
// a shared query string, and the properties under test are what that string
// contains and what it must never contain. Reaching it from outside the package
// would mean a live PostgreSQL 18.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// TestNotNullConstraintProjection_AsksOnlyWhereTheTargetKeepsAName pins the
// gate.
//
// PostgreSQL 17 and 18 accept the identical `CONSTRAINT c NOT NULL` syntax and
// only 18 keeps the name -- pg_constraint, contype 'n'. Asking 17 would query a
// contype it has no rows for, which costs a scan to learn nothing
// (stokaro/ptah#2161).
func TestNotNullConstraintProjection_AsksOnlyWhereTheTargetKeepsAName(t *testing.T) {
	tests := []struct {
		name  string
		caps  capability.Capabilities
		wants bool
	}{
		{
			name:  "a target that records nothing",
			caps:  capability.Postgres17().With(capability.NamedNotNullConstraints, false),
			wants: false,
		},
		{
			// The control. Without it a reader that never asked would pass the
			// row above, and every name would read empty everywhere.
			name:  "a target that keeps the name",
			caps:  capability.Postgres17().With(capability.NamedNotNullConstraints, true),
			wants: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewPostgreSQLReaderWithCapabilities(nil, "public", test.caps)

			projection := reader.notNullConstraintNameExpr()

			c.Assert(strings.Contains(projection, "pg_constraint"), qt.Equals, test.wants)
			// Either way the column is in the result, because the scan reads it
			// by position.
			c.Assert(projection, qt.Contains, "AS not_null_constraint_name")
		})
	}
}

// TestNotNullConstraintProjection_IsWrittenTheWayTheCatalogAnswers pins three
// choices in the SQL that a passing read would not reveal.
func TestNotNullConstraintProjection_IsWrittenTheWayTheCatalogAnswers(t *testing.T) {
	tests := []struct {
		name     string
		fragment string
		why      string
	}{
		{
			name:     "the NOT NULL contype",
			fragment: "con.contype = 'n'",
			why:      "any other contype names a different constraint on the same column",
		},
		{
			name:     "identity, not containment",
			fragment: "con.conkey = ARRAY[a.attnum]",
			why: "conkey holds one element for a NOT NULL; a containment test would " +
				"match a multi-column constraint that merely includes the column",
		},
		{
			name:     "a subquery rather than a join",
			fragment: "SELECT con.conname",
			why: "the column projection is already several joins deep and PGAdapter " +
				"refuses a query past twenty of them outright",
		},
	}

	projection := NewPostgreSQLReaderWithCapabilities(
		nil, "public", capability.Postgres17().With(capability.NamedNotNullConstraints, true),
	).notNullConstraintNameExpr()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(projection, qt.Contains, test.fragment, qt.Commentf("%s", test.why))
		})
	}
}

// TestNotNullConstraintProjection_DoesNotJoin is the other direction of the
// third row above: naming a subquery is not proof that no join was added
// beside it.
func TestNotNullConstraintProjection_DoesNotJoin(t *testing.T) {
	c := qt.New(t)

	projection := NewPostgreSQLReaderWithCapabilities(
		nil, "public", capability.Postgres17().With(capability.NamedNotNullConstraints, true),
	).notNullConstraintNameExpr()

	c.Assert(strings.ToUpper(projection), qt.Not(qt.Contains), "JOIN")
}
