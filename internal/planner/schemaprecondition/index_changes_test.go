package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
)

// TestRefuseIndexChangesInPlace_FailurePath refuses an index rename and an
// index comment written apart from the index for a planner that plans
// neither, naming the index and the planner: planning nothing would report the
// database synced while the index kept its name or its comment.
func TestRefuseIndexChangesInPlace_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "a rename",
			diff:    &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}}},
			wantErr: `unsupported feature: the diff renames index "a" of table "users" to "b", and the postgres planner plans no index rename`,
		},
		{
			name: "an index comment",
			diff: &difftypes.SchemaDiff{IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users", Name: "a", Desired: "x"}}},
			wantErr: `unsupported feature: the diff writes the comment of index "a" of table "users" apart from the index, ` +
				`which only a YDB or a PostgreSQL-family plan does; the postgres planner plans none`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaprecondition.RefuseIndexChangesInPlace("postgres", test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

// TestRefuseIndexChangesInPlace_HappyPath passes a diff carrying neither, and
// no diff at all.
func TestRefuseIndexChangesInPlace_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff", diff: nil},
		{name: "an index added and one removed", diff: &difftypes.SchemaDiff{
			IndexesAdded:   difftypes.IndexChangesFromRefs(difftypes.IndexRef{Name: "b", TableName: "users"}),
			IndexesRemoved: []difftypes.IndexRef{{Name: "a", TableName: "users"}},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseIndexChangesInPlace("postgres", test.diff), qt.IsNil)
		})
	}
}

// TestRefuseIndexRenames_FailurePath refuses an index rename for a planner
// that writes an index's comment in place and renames no index.
func TestRefuseIndexRenames_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "users", From: "a", To: "b"}}}

	err := schemaprecondition.RefuseIndexRenames("postgres", diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches,
		`unsupported feature: the diff renames index "a" of table "users" to "b", and the postgres planner plans no index rename`)
}

// TestRefuseIndexRenames_HappyPath passes an index comment, which such a
// planner writes, and no diff at all.
func TestRefuseIndexRenames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff", diff: nil},
		{name: "an index comment", diff: &difftypes.SchemaDiff{
			IndexCommentsChanged: []difftypes.IndexCommentChange{{TableName: "users", Name: "a", Desired: "x"}},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseIndexRenames("postgres", test.diff), qt.IsNil)
		})
	}
}
