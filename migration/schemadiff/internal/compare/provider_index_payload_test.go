package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// coveredIndexes compares one index on dialect whose desired and database
// sides cover the columns given.
func coveredIndexes(dialect string, desired, database []string) *difftypes.SchemaDiff {
	declared := generatedIndex([]schemamodel.IndexPart{{Name: "a"}})
	declared.IncludeColumns = desired
	held := databaseIndex([]catalog.IndexPart{{Name: "a"}})
	held.IncludeColumns = database
	diff := &difftypes.SchemaDiff{}
	compare.IndexesWithDialect(&schemamodel.Database{Indexes: []schemamodel.Index{declared}}, &catalog.Database{Indexes: []catalog.Index{held}},
		diff, dialect, schemacomparison.Rules{})
	return diff
}

// An engine a provider serves that states no answer for
// index_covering_columns compares an index's covered columns: the database
// holds the payload the declaration names, so nothing changes, as YDB's COVER
// columns compared while the built-in engine's preset answered the key.
func TestProviderEngineComparesCoveredColumns_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff := coveredIndexes("fixture-cover-unknown", []string{"b"}, []string{"b"})

	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

// The lists still decide: a payload the database does not hold as declared
// is a change. And a provider that establishes the key false is a target
// that cannot write a payload, so a declared payload is a change whatever
// the database reports, as on a built-in target without the key.
func TestProviderEngineComparesCoveredColumns_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterDefaults(capability.Of(map[capability.Capability]bool{capability.IndexCoveringColumns: false}),
		"fixture-cover-refused"), qt.IsNil)

	for _, row := range []struct {
		name              string
		dialect           string
		desired, database []string
	}{
		{"another payload", "fixture-cover-unknown", []string{"b"}, []string{"c"}},
		{"a payload the database lacks", "fixture-cover-unknown", []string{"b"}, nil},
		{"a provider that cannot write one", "fixture-cover-refused", []string{"b"}, []string{"b"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := coveredIndexes(row.dialect, row.desired, row.database)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 1)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 1)
		})
	}
}
