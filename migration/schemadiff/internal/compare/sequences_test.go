package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestSequences_AddRemove(t *testing.T) {
	tests := []struct {
		name              string
		generated         []goschema.Sequence
		database          []types.DBSequence
		expectedAdded     []string
		expectedRemoved   []string
		expectedModifiedN int
	}{
		{
			name:            "no sequences in either schema",
			generated:       nil,
			database:        nil,
			expectedAdded:   nil,
			expectedRemoved: nil,
		},
		{
			name:            "sequence needs to be added",
			generated:       []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}},
			database:        nil,
			expectedAdded:   []string{"order_seq"},
			expectedRemoved: nil,
		},
		{
			name:            "sequence needs to be removed",
			generated:       nil,
			database:        []types.DBSequence{{Name: "legacy_seq"}},
			expectedAdded:   nil,
			expectedRemoved: []string{"legacy_seq"},
		},
		{
			name:            "schema-qualified sequence matches by qualified name",
			generated:       []goschema.Sequence{{Name: "s", Schema: "app"}},
			database:        []types.DBSequence{{Name: "s", Schema: "app"}},
			expectedAdded:   nil,
			expectedRemoved: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Sequences: tt.generated}
			database := &types.DBSchema{Sequences: tt.database}
			diff := &difftypes.SchemaDiff{}

			compare.Sequences(generated, database, diff)

			c.Assert(diff.SequencesAdded, qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.SequencesRemoved, qt.DeepEquals, tt.expectedRemoved)
		})
	}
}

func TestSequences_ModifiedOnlyComparesDeclaredOptions(t *testing.T) {
	c := qt.New(t)

	// The target declares increment=2 and cycle=true, but leaves cache unset.
	// The database has increment=1, cache=30, cycle=false. Only the declared
	// options that differ (increment, cycle) must show up as changes; the
	// undeclared cache must not churn.
	generated := &goschema.Database{Sequences: []goschema.Sequence{
		{Name: "s", Increment: new(int64(2)), Cycle: true},
	}}
	database := &types.DBSchema{Sequences: []types.DBSequence{
		{Name: "s", Increment: new(int64(1)), Cache: new(int64(30)), Cycle: false},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Sequences(generated, database, diff)

	c.Assert(diff.SequencesAdded, qt.IsNil)
	c.Assert(diff.SequencesRemoved, qt.IsNil)
	c.Assert(diff.SequencesModified, qt.HasLen, 1)
	changes := diff.SequencesModified[0].Changes
	c.Assert(changes["increment"], qt.Equals, "1 -> 2")
	c.Assert(changes["cycle"], qt.Equals, "false -> true")
	_, cacheChanged := changes["cache"]
	c.Assert(cacheChanged, qt.IsFalse, qt.Commentf("undeclared cache option must not be flagged"))
}

// TestGrants_OnSequenceRoundTrip verifies a declared sequence grant matches its
// introspected counterpart (keyed as an ON SEQUENCE grant) and does not churn.
func TestGrants_OnSequenceRoundTrip(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "app_user", Privileges: []string{"USAGE", "SELECT"}, OnSequence: "order_seq"},
		},
	}
	database := &types.DBSchema{
		Grants: []types.DBGrant{
			{Role: "app_user", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
			{Role: "app_user", Privilege: "SELECT", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Grants(generated, database, diff)

	c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("declared sequence grant must match DB, not re-add"))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("introspected sequence grant must not be revoked"))
}

// TestGrants_OnSequenceAddedWhenMissing verifies a declared sequence grant that
// is absent from the database is emitted as an ON SEQUENCE grant (not a
// malformed empty-named TABLE grant).
func TestGrants_OnSequenceAddedWhenMissing(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "app_user", Privileges: []string{"USAGE"}, OnSequence: "order_seq"},
		},
	}
	database := &types.DBSchema{}
	diff := &difftypes.SchemaDiff{}

	compare.Grants(generated, database, diff)

	c.Assert(diff.GrantsAdded, qt.HasLen, 1)
	c.Assert(diff.GrantsAdded[0].ObjectType, qt.Equals, "SEQUENCE")
	c.Assert(diff.GrantsAdded[0].ObjectName, qt.Equals, "order_seq")
}

func TestSequences_UnchangedProducesNoDiff(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{Sequences: []goschema.Sequence{
		{Name: "s", AsType: "bigint", Increment: new(int64(1)), Cache: new(int64(20)), Cycle: true},
	}}
	database := &types.DBSchema{Sequences: []types.DBSequence{
		{Name: "s", DataType: "bigint", Increment: new(int64(1)), Cache: new(int64(20)), Cycle: true},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Sequences(generated, database, diff)

	c.Assert(diff.SequencesAdded, qt.IsNil)
	c.Assert(diff.SequencesRemoved, qt.IsNil)
	c.Assert(diff.SequencesModified, qt.IsNil)
}
