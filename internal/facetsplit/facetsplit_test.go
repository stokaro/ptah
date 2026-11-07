package facetsplit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/facetsplit"
)

func ownsRowTTL(kind schemaext.Kind) bool { return kind == crdbschema.RowTTLKind }

// splitSchema has a table with MySQL table options and a CockroachDB
// row-level TTL, each bound to its target, and a table with neither.
func splitSchema() *schemamodel.Database {
	facets := must.Must(schemaext.NewFacets(
		&mysqlschema.DesiredTable{Engine: "InnoDB"},
		&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 day"}},
	))
	facets = must.Must(must.Must(facets.WithTargetScope(mysqlschema.TableKind, "mysql")).WithTargetScope(crdbschema.RowTTLKind, "cockroachdb"))
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Facets: facets}, {Name: "plain"}}}
}

// TestSetAside_TakesOnlyTheOwnedFacetsAndRestorePutsThemBack pins the round
// trip: the owned facet leaves its table with its binding, every other facet
// stays, and restoring gives the tables back as they were, with the input
// unchanged throughout.
func TestSetAside_TakesOnlyTheOwnedFacetsAndRestorePutsThemBack(t *testing.T) {
	c := qt.New(t)
	db := splitSchema()

	kept, aside := facetsplit.SetAside(db, ownsRowTTL)
	restored, err := facetsplit.Restore(kept, aside)

	c.Assert(kept.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{mysqlschema.TableKind})
	c.Assert(kept.Tables[0].Facets.TargetScope(mysqlschema.TableKind), qt.DeepEquals, []string{"mysql"})
	c.Assert(aside, qt.HasLen, 2)
	c.Assert(aside[0].Kinds(), qt.DeepEquals, []schemaext.Kind{crdbschema.RowTTLKind})
	c.Assert(aside[1].IsZero(), qt.IsTrue)
	c.Assert(db.Tables[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{crdbschema.RowTTLKind, mysqlschema.TableKind})
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Tables[0].Facets.Equal(db.Tables[0].Facets), qt.IsTrue)
	c.Assert(restored.Tables[0].Facets.TargetScope(crdbschema.RowTTLKind), qt.DeepEquals, []string{"cockroachdb"})
	c.Assert(restored.Tables[1].Facets.IsZero(), qt.IsTrue)
}

// TestRestore_FailurePath refuses facets that no longer line up with the
// tables they were taken from.
func TestRestore_FailurePath(t *testing.T) {
	c := qt.New(t)
	kept, aside := facetsplit.SetAside(splitSchema(), ownsRowTTL)
	kept.Tables = kept.Tables[:1]

	restored, err := facetsplit.Restore(kept, aside)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(restored, qt.IsNil)
}
