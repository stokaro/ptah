package migrator

// White-box testing required: the boundary a `set` keeps is decided inside the
// unexported delete builder, and version 0 keeps none. No exported-API test on
// any engine this repository runs offline can see the difference, because the
// two spellings -- an absent `version <> ?` clause and one bound to the empty
// string -- delete the same rows everywhere except Oracle, where the empty
// string is NULL. Measured on gvenzl/oracle-free:slim, Oracle Free 23:
//
//	'a' <> ''                     not true
//	'' IS NULL                    true
//	SELECT ... WHERE v <> ''      0 of 2 rows
//
// So a delete whose predicate ANDs that clause removes nothing there and still
// reports every revision removed, because the report is computed in Go.

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrationfile"
)

// newZeroSetMigrator returns a connection-less migrator over two Atlas-format
// migrations, which is all the delete builder reads.
func newZeroSetMigrator(c *qt.C) *Migrator {
	c.Helper()
	m, err := NewFSMigrator(nil, fstest.MapFS{
		"1_users.sql":  {Data: []byte("SELECT 1;\n")},
		"2_orders.sql": {Data: []byte("SELECT 2;\n")},
	}, WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return m.WithRevisionTableFormat(RevisionTableFormatAtlas)
}

func TestDeleteAtlasSetRevisionsAbove_HappyPath(t *testing.T) {
	t.Run("a set below the head keeps the head row", func(t *testing.T) {
		c := qt.New(t)
		m := newZeroSetMigrator(c)

		statement, args := m.deleteAtlasSetRevisionsAbove(nil, nil, 1, m.migrationByVersion(1))

		c.Assert(statement, qt.Contains, "> ? AND version <> ?")
		c.Assert(args, qt.DeepEquals, []any{int64(1), "1"})
	})

	// Version 0 leaves no head row, so the clause that would keep one is
	// absent rather than bound to a value no revision can equal. Restoring the
	// clause with an empty argument passes on every dialect whose empty string
	// is a value and deletes nothing on Oracle.
	t.Run("a set to version 0 keeps no row", func(t *testing.T) {
		c := qt.New(t)
		m := newZeroSetMigrator(c)

		statement, args := m.deleteAtlasSetRevisionsAbove(nil, nil, 0, nil)

		c.Assert(statement, qt.Not(qt.Contains), "version <> ?")
		c.Assert(args, qt.DeepEquals, []any{int64(0)})
	})
}

// A revision the directory does not own is ordered against the head row by the
// comparator the caller installed, and a set to version 0 has no head to order
// it against. Without the boundary arm the builder asks a comparator it was
// never given for an answer it does not need.
func TestUnownedExactAtlasRevisionsAbove_HappyPath(t *testing.T) {
	t.Run("no boundary removes every unowned revision without a comparator", func(t *testing.T) {
		c := qt.New(t)
		provider, err := NewFSMigrationProvider(
			fstest.MapFS{
				"10_target.sql": {Data: []byte("SELECT 10;\n")},
			},
			WithMigrationDirFormat(migrationfile.DirFormatAtlas),
			WithAtlasRevisionVersions(map[int64]string{10: "10"}),
		)
		c.Assert(err, qt.IsNil)
		m := &Migrator{
			migrationProvider:   provider,
			revisionTableFormat: RevisionTableFormatAtlas,
		}

		removed, err := m.unownedExactAtlasRevisionsAbove(
			[]MigrationRevision{{
				Version:         0,
				AtlasVersion:    "20",
				hasAtlasVersion: true,
				AtlasType:       AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied,
				OperatorVersion: revisiontable.SourceIdentityOperatorVersion,
			}},
			nil,
		)

		c.Assert(err, qt.IsNil)
		c.Assert(removed, qt.DeepEquals, []string{"20"})
	})
}

func TestSetRevision_FailurePath(t *testing.T) {
	t.Run("a negative version is refused", func(t *testing.T) {
		c := qt.New(t)
		m := newZeroSetMigrator(c)

		result, err := m.setRevisionLocked(c.Context(), -1)

		c.Assert(err, qt.ErrorMatches, "migration version must not be negative")
		c.Assert(result, qt.DeepEquals, AtlasRevisionSetResult{})
	})
}

// Version 0 removes every row that records a migration, and an Atlas metadata
// row records none. The read usually excludes those rows before the decision
// is made, but with an exact identity map the predicate names
// `.atlas_cloud_identifier` alone rather than the dot-prefixed class, so a
// second metadata row reaches this predicate and nothing else stands between
// it and the delete.
func TestRevisionRemovedByAtlasSet_HappyPath(t *testing.T) {
	t.Run("version 0 removes a migration row that orders to zero", func(t *testing.T) {
		c := qt.New(t)
		revision := MigrationRevision{Version: 0, AtlasVersion: "R", hasAtlasVersion: true}

		removed := revisionRemovedByAtlasSet(revision, nil, 0, nil, nil)

		c.Assert(removed, qt.IsTrue)
	})

	t.Run("version 0 keeps an Atlas metadata row", func(t *testing.T) {
		c := qt.New(t)
		revision := MigrationRevision{
			Version:         0,
			AtlasVersion:    ".atlas_cloud_identifier",
			hasAtlasVersion: true,
		}

		removed := revisionRemovedByAtlasSet(revision, nil, 0, nil, nil)

		c.Assert(removed, qt.IsFalse)
	})

	t.Run("a version above zero is an ordering question again", func(t *testing.T) {
		c := qt.New(t)
		revision := MigrationRevision{Version: 0, AtlasVersion: "R", hasAtlasVersion: true}

		removed := revisionRemovedByAtlasSet(revision, nil, 1, nil, nil)

		c.Assert(removed, qt.IsFalse)
	})
}
