package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestSequences_AddRemove(t *testing.T) {
	tests := []struct {
		name              string
		desired           []schemamodel.Sequence
		database          []catalog.Sequence
		expectedAdded     []string
		expectedRemoved   []string
		expectedModifiedN int
	}{
		{
			name:            "no sequences in either schema",
			desired:         nil,
			database:        nil,
			expectedAdded:   nil,
			expectedRemoved: nil,
		},
		{
			name:            "sequence needs to be added",
			desired:         []schemamodel.Sequence{{Name: "order_seq", AsType: "bigint"}},
			database:        nil,
			expectedAdded:   []string{"order_seq"},
			expectedRemoved: nil,
		},
		{
			name:            "sequence needs to be removed",
			desired:         nil,
			database:        []catalog.Sequence{{Name: "legacy_seq"}},
			expectedAdded:   nil,
			expectedRemoved: []string{"legacy_seq"},
		},
		{
			name:            "schema-qualified sequence matches by qualified name",
			desired:         []schemamodel.Sequence{{Name: "s", Schema: "app"}},
			database:        []catalog.Sequence{{Name: "s", Schema: "app"}},
			expectedAdded:   nil,
			expectedRemoved: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Sequences: tt.desired}
			database := &catalog.Database{Sequences: tt.database}
			diff := &difftypes.SchemaDiff{}

			compare.Sequences(desired, database, diff, compare.CoverageOf(desired, database))

			c.Assert(diff.SequencesAdded.Names(), qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.SequencesRemoved.Names(), qt.DeepEquals, tt.expectedRemoved)
		})
	}
}

func TestSequences_ModifiedOnlyComparesDeclaredOptions(t *testing.T) {
	c := qt.New(t)

	// The target declares increment=2 and cycle=true, but leaves cache unset.
	// The database has increment=1, cache=30, cycle=false. Only the declared
	// options that differ (increment, cycle) must show up as changes; the
	// undeclared cache must not churn.
	desired := &schemamodel.Database{Sequences: []schemamodel.Sequence{
		{Name: "s", Increment: new(int64(2)), Cycle: true},
	}}
	database := &catalog.Database{Sequences: []catalog.Sequence{
		{Name: "s", Increment: new(int64(1)), Cache: new(int64(30)), Cycle: false},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Sequences(desired, database, diff, compare.CoverageOf(desired, database))

	c.Assert(diff.SequencesAdded.Names(), qt.IsNil)
	c.Assert(diff.SequencesRemoved.Names(), qt.IsNil)
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

	desired := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "app_user", Privileges: []string{"USAGE", "SELECT"}, OnSequence: "order_seq"},
		},
	}
	database := &catalog.Database{
		Grants: []catalog.Grant{
			{Role: "app_user", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
			{Role: "app_user", Privilege: "SELECT", ObjectType: "SEQUENCE", ObjectName: "order_seq"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Grants(desired, database, diff)

	c.Assert(diff.GrantsAdded, qt.HasLen, 0, qt.Commentf("declared sequence grant must match DB, not re-add"))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 0, qt.Commentf("introspected sequence grant must not be revoked"))
}

// TestGrants_OnSequenceAddedWhenMissing verifies a declared sequence grant that
// is absent from the database is emitted as an ON SEQUENCE grant (not a
// malformed empty-named TABLE grant).
func TestGrants_OnSequenceAddedWhenMissing(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "app_user", Privileges: []string{"USAGE"}, OnSequence: "order_seq"},
		},
	}
	database := &catalog.Database{}
	diff := &difftypes.SchemaDiff{}

	compare.Grants(desired, database, diff)

	c.Assert(diff.GrantsAdded, qt.HasLen, 1)
	c.Assert(diff.GrantsAdded[0].ObjectType, qt.Equals, "SEQUENCE")
	c.Assert(diff.GrantsAdded[0].ObjectName, qt.Equals, "order_seq")
}

func TestSequences_UnchangedProducesNoDiff(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{Sequences: []schemamodel.Sequence{
		{Name: "s", AsType: "bigint", Increment: new(int64(1)), Cache: new(int64(20)), Cycle: true},
	}}
	database := &catalog.Database{Sequences: []catalog.Sequence{
		{Name: "s", DataType: "bigint", Increment: new(int64(1)), Cache: new(int64(20)), Cycle: true},
	}}
	diff := &difftypes.SchemaDiff{}

	compare.Sequences(desired, database, diff, compare.CoverageOf(desired, database))

	c.Assert(diff.SequencesAdded.Names(), qt.IsNil)
	c.Assert(diff.SequencesRemoved.Names(), qt.IsNil)
	c.Assert(diff.SequencesModified, qt.IsNil)
}
