package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/tableidentity"
	"ptah.run/migration/schemadiff/internal/compare"
)

// statedConditionTables is table t prepared with the predicate its
// preparation states for index i.
func statedConditionTables(condition string) map[objectidentity.Key]schemapreparation.Table {
	subject := tableidentity.Subject("", "t", fixtureTarget, identifier.ForDialect(fixtureTarget))
	return map[objectidentity.Key]schemapreparation.Table{subject.Key(): {Subject: subject, IndexConditions: map[string]string{"i": condition}}}
}

// A predicate a table's preparation states is compared exactly with the held
// one, whatever the shared comparison would make of the two texts, and the
// declaration keeps what the source wrote. Without a stated predicate the
// shared comparison folds case, spacing and parentheses as before.
func TestIndexes_PreparedPredicatesAreComparedExactly(t *testing.T) {
	ref := []difftypes.IndexRef{{Name: "i", TableName: "t"}}
	for _, row := range []struct {
		name     string
		declared string
		prepared map[objectidentity.Key]schemapreparation.Table
		held     string
		replaced []difftypes.IndexRef
	}{
		// The shared comparison reads the held cast as another predicate.
		{"a rewritten predicate, prepared as the server holds it", "score > 0", statedConditionTables("(score > '0'::bigint)"),
			"(score > '0'::bigint)", nil},
		{"a held predicate with surrounding white space", "score > 0", statedConditionTables("(score > '0'::bigint)"),
			" (score > '0'::bigint) ", nil},
		// The shared comparison folds these two texts to one predicate.
		{"a prepared spelling that differs from the held one", "a > 0", statedConditionTables("A > 0"), "(a > 0)", ref},
		{"a changed predicate", "score > 1", statedConditionTables("(score > '1'::bigint)"), "(score > '0'::bigint)", ref},
		{"a predicate the held index lacks", "score > 0", statedConditionTables("(score > '0'::bigint)"), "", ref},
		{"a full index against a held predicate", "", statedConditionTables(""), "(score > '0'::bigint)", ref},
		{"nothing prepared, a rewritten predicate", "score > 0", nil, "(score > '0'::bigint)", ref},
		{"nothing prepared, a respelled predicate", "a > 0", nil, "(A > 0)", nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}},
				Indexes: []schemamodel.Index{{Name: "i", TableName: "t", Fields: []string{"a"}, Condition: row.declared}}}
			current := &catalog.Database{Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"a"}, Condition: row.held}}}
			diff := &difftypes.SchemaDiff{}
			compare.IndexesWithSemantics(desired, current, diff, fixtureTarget, identifier.ForDialect(fixtureTarget), nil,
				renamingCaps, schemacomparison.Rules{}, row.prepared)
			c.Assert(diff.IndexesRemoved, qt.DeepEquals, row.replaced)
			c.Assert(desired.Indexes[0].Condition, qt.Equals, row.declared)
		})
	}
}

// A replaced index is created as the source declared it: the diff carries the
// authored predicate, not the prepared spelling.
func TestIndexes_ReplacedIndexKeepsTheAuthoredPredicate(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}},
		Indexes: []schemamodel.Index{{Name: "i", TableName: "t", Fields: []string{"a"}, Condition: "score > 1"}}}
	current := &catalog.Database{Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"a"}, Condition: "(score > '0'::bigint)"}}}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithSemantics(desired, current, diff, fixtureTarget, identifier.ForDialect(fixtureTarget), nil,
		renamingCaps, schemacomparison.Rules{}, statedConditionTables("(score > '1'::bigint)"))

	c.Assert(diff.IndexesAdded, qt.HasLen, 1)
	c.Assert(diff.IndexesAdded[0].Index.Condition, qt.Equals, "score > 1")
}
