package diffpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestSkipIndexDropPreservesOnlyTheRetainedIndexesComment(t *testing.T) {
	cases := []struct {
		name    string
		dialect string
		removed string
		comment string
	}{
		{name: "table scoped", dialect: "ydb", removed: "items", comment: "items"},
		{name: "default schema", dialect: "postgres", removed: "items", comment: "public.items"},
		{name: "case insensitive index", dialect: "mysql", removed: "items", comment: "items"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cleanup := difftypes.IndexCommentChange{TableName: test.comment, Name: "by_id", Current: "keep me"}
			other := difftypes.IndexCommentChange{TableName: "other", Name: "by_id", Current: "old"}
			cleared := difftypes.IndexCommentChange{TableName: test.comment, Name: "clear_me", Current: "old"}
			replaced := difftypes.IndexCommentChange{TableName: test.comment, Name: "rebuilt", Current: "old"}
			diff := &difftypes.SchemaDiff{
				IndexesRemoved:       []difftypes.IndexRef{{TableName: test.removed, Name: "by_id"}, {TableName: test.removed, Name: "rebuilt"}},
				IndexesAdded:         difftypes.IndexChanges{{TableName: test.removed, Index: schemamodel.Index{Name: "rebuilt", Fields: []string{"id"}}}},
				IndexCommentsChanged: []difftypes.IndexCommentChange{cleanup, other, cleared, replaced},
			}
			filtered, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex), test.dialect)
			c.Assert(skipped, qt.HasLen, 1)
			c.Assert(filtered.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{other, cleared, replaced})
			c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{cleanup, other, cleared, replaced})
		})
	}
}
