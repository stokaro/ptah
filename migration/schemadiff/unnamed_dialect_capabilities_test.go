package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// recommentedIndex is an index whose comment the declaration changes.
func recommentedIndex() (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "i", TableName: "t", Fields: []string{"a"}, Comment: "new"}}},
		&catalog.Database{Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"a"}, Comment: "old"}}}
}

// TestCompare_UnnamedDialectAssumesNoTargetCapabilities pins what a comparison
// that names no dialect assumes about its target: nothing. It has no target,
// so it reads no target's capabilities, as it reads no target's identifier
// rules, descriptor facts or comparison rules. An index comment changed in
// place is therefore compared only on a target with
// capability.IndexComments, such as PostgreSQL.
func TestCompare_UnnamedDialectAssumesNoTargetCapabilities(t *testing.T) {
	runtime := must.Must(builtin.New())

	t.Run("no dialect", func(t *testing.T) {
		c := qt.New(t)
		desired, current := recommentedIndex()

		diff, err := schemadiff.Compare(t.Context(), desired, current, runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexCommentsChanged, qt.HasLen, 0)
		c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
		c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	})

	t.Run("a target that comments an index in place", func(t *testing.T) {
		c := qt.New(t)
		desired, current := recommentedIndex()

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{
			{TableName: "t", Name: "i", Current: "old", Desired: "new"},
		})
		c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
		c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	})
}
