package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/postgres"
)

// TestParseExcludeConstraintDefinition_Deferral leaves the deferral clauses
// pg_get_constraintdef appends out of the predicate. PostgreSQL 18.6 prints a
// deferrable EXCLUDE as `EXCLUDE USING btree (r WITH =) WHERE ((r > 0))
// DEFERRABLE INITIALLY DEFERRED`; read as it stands, the predicate kept the
// clauses and never matched the declaration.
func TestParseExcludeConstraintDefinition_Deferral(t *testing.T) {
	tests := []struct {
		name         string
		definition   string
		wantElements string
		wantWhere    string
	}{
		{name: "deferrable", definition: "EXCLUDE USING btree (r WITH =) DEFERRABLE", wantElements: "r WITH ="},
		{
			name:         "deferred, with a predicate",
			definition:   "EXCLUDE USING btree (r WITH =) WHERE ((r > 0)) DEFERRABLE INITIALLY DEFERRED",
			wantElements: "r WITH =",
			wantWhere:    "(r > 0)",
		},
		{
			name:         "a predicate naming a column called deferrable",
			definition:   "EXCLUDE USING btree (r WITH =) WHERE ((deferrable > 0))",
			wantElements: "r WITH =",
			wantWhere:    "(deferrable > 0)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := postgres.ParseExcludeConstraintDefinition(test.definition)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.UsingMethod, qt.Equals, "btree")
			c.Assert(parsed.Elements, qt.Equals, test.wantElements)
			c.Assert(parsed.WhereCondition, qt.Equals, test.wantWhere)
		})
	}
}
