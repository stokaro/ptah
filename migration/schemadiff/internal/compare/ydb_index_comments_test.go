package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// commentedIndexes returns index "i" on both sides, with the comments given.
func commentedIndexes(desired, current string) (schemamodel.Index, catalog.Index) {
	declared := ydbDeclaredIndex("", false, []string{"a"}, nil)
	declared.Comment = desired
	database := ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)
	database.Comment = current
	return declared, database
}

// An index both sides hold with the same definition keeps its index, and a
// comment that differs is written in place: YDB keeps it as an attribute of
// the table, apart from the index.
func TestIndexes_YDBCommentChangesInPlace(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
	}{
		{name: "a comment declared", desired: "Lookup"},
		{name: "a comment changed", desired: "Lookup by name", current: "Lookup"},
		{name: "a comment no longer declared", current: "Lookup"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, database := commentedIndexes(test.desired, test.current)

			diff := compareYDBIndexes([]schemamodel.Index{declared}, []catalog.Index{database}, capability.YDB262())

			c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{
				{TableName: "t", Name: "i", Current: test.current, Desired: test.desired},
			})
			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// The same comment on both sides is no change, and a target that does not
// keep an index's comment apart from it compares none.
func TestIndexes_YDBCommentUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
		caps    capability.Capabilities
	}{
		{name: "the same comment", desired: "Lookup", current: "Lookup", caps: capability.YDB262()},
		{name: "neither side comments", caps: capability.YDB262()},
		{name: "a target without comment_attributes", desired: "Lookup", current: "Other",
			caps: capability.YDB262().With(capability.CommentAttributes, false)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, database := commentedIndexes(test.desired, test.current)

			diff := compareYDBIndexes([]schemamodel.Index{declared}, []catalog.Index{database}, test.caps)

			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A renamed index takes its comment to its new name, which is a new key on
// the table, so the comment is written even where its text is the same; one
// without a comment on either side has nothing to move.
func TestIndexes_YDBCommentMovesWithARename(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		current string
		want    []difftypes.IndexCommentChange
	}{
		{name: "the same comment", desired: "Lookup", current: "Lookup",
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "j", From: "i", Current: "Lookup", Desired: "Lookup"}}},
		{name: "a changed comment", desired: "New", current: "Old",
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "j", From: "i", Current: "Old", Desired: "New"}}},
		{name: "a dropped comment", current: "Old",
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "j", From: "i", Current: "Old"}}},
		{name: "no comment"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, database := commentedIndexes(test.desired, test.current)

			diff := compareYDBIndexes([]schemamodel.Index{namedIndex(declared, "j")}, []catalog.Index{database}, capability.YDB262())

			c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "t", From: "i", To: "j"}})
			c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// A dropped index leaves its comment on the table, so the plan removes it; a
// rebuilt one does too where the new index has no comment to write over it.
// A new index writes its own comment.
func TestIndexes_YDBCommentOfADroppedIndex(t *testing.T) {
	rebuiltDeclared, rebuiltDatabase := commentedIndexes("", "Old")
	rebuiltDeclared.Unique = true
	rewrittenDeclared, rewrittenDatabase := commentedIndexes("New", "Old")
	rewrittenDeclared.Unique = true
	added, _ := commentedIndexes("New", "")
	tests := []struct {
		name     string
		desired  []schemamodel.Index
		database []catalog.Index
		want     []difftypes.IndexCommentChange
	}{
		{name: "dropped", database: []catalog.Index{rebuiltDatabase},
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "i", Current: "Old"}}},
		{name: "rebuilt without a comment", desired: []schemamodel.Index{rebuiltDeclared}, database: []catalog.Index{rebuiltDatabase},
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "i", Current: "Old"}}},
		{name: "rebuilt with another comment", desired: []schemamodel.Index{rewrittenDeclared}, database: []catalog.Index{rewrittenDatabase},
			want: []difftypes.IndexCommentChange{{TableName: "t", Name: "i", Current: "Old", Desired: "New"}}},
		{name: "added", desired: []schemamodel.Index{added}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compareYDBIndexes(test.desired, test.database, capability.YDB262())
			c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}
