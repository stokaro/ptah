package facetsplit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/facetsplit"
)

func ownsFamilies(kind schemaext.Kind) bool { return kind == ydbschema.ColumnFamiliesKind }

// splitSchema has a table with a TTL and column families, each bound to YDB,
// and a table with neither.
func splitSchema() *schemamodel.Database {
	facets := must.Must(schemaext.NewFacets(
		&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "ts", Interval: "P1D"}},
		&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}},
	))
	facets = must.Must(must.Must(facets.WithTargetScope(ydbschema.TTLKind, "ydb")).WithTargetScope(ydbschema.ColumnFamiliesKind, "ydb"))
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Facets: facets}, {Name: "plain"}}}
}

// TestSetAside_TakesOnlyTheOwnedFacetsAndRestorePutsThemBack pins the round
// trip: the owned facet leaves its table with its binding, every other facet
// stays, and restoring gives the tables back as they were, with the input
// unchanged throughout.
func TestSetAside_TakesOnlyTheOwnedFacetsAndRestorePutsThemBack(t *testing.T) {
	c := qt.New(t)
	db := splitSchema()

	kept, aside := facetsplit.SetAside(db, ownsFamilies)
	restored, err := facetsplit.Restore(kept, aside)

	c.Assert(kept.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{ydbschema.TTLKind})
	c.Assert(kept.Tables[0].Facets.TargetScope(ydbschema.TTLKind), qt.DeepEquals, []string{"ydb"})
	c.Assert(aside, qt.HasLen, 2)
	c.Assert(aside[0].Kinds(), qt.DeepEquals, []schemaext.Kind{ydbschema.ColumnFamiliesKind})
	c.Assert(aside[1].IsZero(), qt.IsTrue)
	c.Assert(db.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{ydbschema.ColumnFamiliesKind, ydbschema.TTLKind})
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Tables[0].Facets.Equal(db.Tables[0].Facets), qt.IsTrue)
	c.Assert(restored.Tables[0].Facets.TargetScope(ydbschema.ColumnFamiliesKind), qt.DeepEquals, []string{"ydb"})
	c.Assert(restored.Tables[1].Facets.IsZero(), qt.IsTrue)
}

// TestRestore_FailurePath refuses facets that no longer line up with the
// tables they were taken from.
func TestRestore_FailurePath(t *testing.T) {
	c := qt.New(t)
	kept, aside := facetsplit.SetAside(splitSchema(), ownsFamilies)
	kept.Tables = kept.Tables[:1]

	restored, err := facetsplit.Restore(kept, aside)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(restored, qt.IsNil)
}
