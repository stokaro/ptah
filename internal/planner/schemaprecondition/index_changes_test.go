package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseIndexChangesInPlace_FailurePath refuses an index rename and a
// change of an index's partitioning for a planner that plans neither, naming
// the index and the planner: planning nothing for either would report the
// database synced while the index kept its name or its settings.
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
			name: "a change of partitioning",
			diff: &difftypes.SchemaDiff{IndexPartitioningChanged: []difftypes.IndexPartitioningChange{{TableName: "users", Name: "a"}}},
			wantErr: `unsupported feature: the diff changes the partitioning of index "a" of table "users", which only a YDB plan does; ` +
				`the postgres planner plans none`,
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
