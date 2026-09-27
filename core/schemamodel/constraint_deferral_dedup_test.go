package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// Two unnamed constraints that differ only in how they defer are two
// constraints, so deduplication keeps both. PostgreSQL 18.6 builds both of
// `UNIQUE (a) DEFERRABLE, UNIQUE (a)`, and of a deferrable pair that differs
// only in INITIALLY DEFERRED.
func TestDeduplicateConstraints_KeepsConstraintsThatDeferDifferently(t *testing.T) {
	unique := func(deferrable bool, initially string) schemamodel.Constraint {
		return schemamodel.Constraint{StructName: "T", Type: "UNIQUE", Columns: []string{"a"}, Deferrable: deferrable, Initially: initially}
	}
	tests := []struct {
		name          string
		first, second schemamodel.Constraint
	}{
		{name: "one deferrable", first: unique(false, ""), second: unique(true, "")},
		{name: "both deferrable, one deferred", first: unique(true, "immediate"), second: unique(true, "deferred")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables:      []schemamodel.Table{{StructName: "T", Name: "t"}},
				Constraints: []schemamodel.Constraint{test.first, test.second},
			}

			schemamodel.Deduplicate(database)

			c.Assert(database.Constraints, qt.HasLen, 2)
		})
	}
}
