package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// sequenceChange is a diff whose one change is the given change of the
// sequence of items.id.
func sequenceChange(key, change string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "items",
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "id", Changes: map[string]string{key: change}}},
	}}}
}

// TestRefuseSerialSequenceChanges_FailurePath refuses either setting of a
// Serial's sequence for a planner that plans neither.
func TestRefuseSerialSequenceChanges_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a start", diff: sequenceChange("identity_start", "1 -> 100"),
			wantErr: `.*: the diff changes identity_start of column "id" of table "items" \(1 -> 100\), which only a YDB plan does; the postgres planner plans none`},
		{name: "an increment", diff: sequenceChange("identity_increment", "1 -> 5"),
			wantErr: `.*: the diff changes identity_increment of column "id" of table "items" \(1 -> 5\), .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaprecondition.RefuseSerialSequenceChanges("postgres", test.diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRefuseSerialSequenceChanges_HappyPath passes a diff that changes no
// sequence, a column change of another kind included, and no diff at all.
func TestRefuseSerialSequenceChanges_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{name: "an empty diff", diff: &difftypes.SchemaDiff{}},
		{name: "a type change", diff: sequenceChange("type", "INTEGER -> BIGINT")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseSerialSequenceChanges("postgres", test.diff), qt.IsNil)
		})
	}
}
