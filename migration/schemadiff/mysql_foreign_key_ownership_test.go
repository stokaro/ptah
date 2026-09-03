package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// ownershipEngines are the two the backing index is transparent on.
var ownershipEngines = []struct {
	name    string
	dialect string
}{
	{name: "mysql", dialect: platform.MySQL},
	{name: "mariadb", dialect: platform.MariaDB},
}

// liveChildren is a catalog holding a foreign key `f` on `a` plus whichever
// indexes a case declares.
func liveChildren(indexes ...catalog.Index) *catalog.Database {
	return &catalog.Database{
		Tables:  []catalog.Table{{Name: "children"}, {Name: "parents"}},
		Indexes: indexes,
		Constraints: []catalog.Constraint{{
			Name: "f", TableName: "children", Type: "FOREIGN KEY",
			ColumnNames: []string{"a"}, ForeignTable: new("parents"),
		}},
	}
}

// desiredChildren is the same table declaring only the named indexes.
func desiredChildren(names ...string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Children", Name: "children"},
			{StructName: "Parents", Name: "parents"},
		},
	}
	for _, name := range names {
		desired.Indexes = append(desired.Indexes, schemamodel.Index{
			StructName: "Children", Name: name, Fields: []string{"a"},
		})
	}
	return desired
}

// removedIndexNames is what a comparison planned to drop.
func removedIndexNames(diff *difftypes.SchemaDiff) []string {
	names := make([]string, 0, len(diff.IndexesRemoved))
	for _, removed := range diff.IndexesRemoved {
		names = append(names, removed.Name)
	}
	return names
}

func compareChildren(c *qt.C, dialect string, current *catalog.Database, desired *schemamodel.Database) []string {
	c.Helper()
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return removedIndexNames(schemadiff.CompareWithOptions(desired, current, opts))
}

// TestCompare_AnIndexSharingAForeignKeysNameIsStillItsOwnObject covers
// stokaro/ptah#2782.
//
// Ownership was inferred from `{table, constraint name}` alone. Measured on
// MySQL 8.4.11 and MariaDB 11.8.9, both accept a table where `cover(a)` backs
// `CONSTRAINT f FOREIGN KEY (a)` while an unrelated `f(b)` sits beside it --
// so the name is shared and the object is not.
//
// Claiming it for the foreign key suppressed it from the comparison entirely:
// removing it from the desired schema reported the target synchronized, with no
// plan, and left a live index nothing would ever manage.
func TestCompare_AnIndexSharingAForeignKeysNameIsStillItsOwnObject(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(
					catalog.Index{Name: "cover", TableName: "children", Columns: []string{"a"}},
					catalog.Index{Name: "f", TableName: "children", Columns: []string{"b"}},
				),
				desiredChildren("cover"))

			c.Assert(removed, qt.DeepEquals, []string{"f"})
		})
	}
}

// TestCompare_TheIndexThatActuallyBacksAForeignKeyIsNotDropped is the inverse
// control, and it is the one a name-blind fix would break.
//
// Here `f(a)` IS the index the engine built for the constraint. It is absent
// from the desired schema for the same reason it always is -- nobody declares
// it -- and planning its removal would emit a DROP the engine refuses, because
// the foreign key still needs it.
func TestCompare_TheIndexThatActuallyBacksAForeignKeyIsNotDropped(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(catalog.Index{Name: "f", TableName: "children", Columns: []string{"a"}}),
				desiredChildren())

			c.Assert(removed, qt.HasLen, 0)
		})
	}
}

// TestCompare_AWiderIndexLedByTheForeignKeysColumnsBacksIt pins that backing is
// a LEADING-column question, not an equality one: `f(a, b)` backs a key on `a`.
func TestCompare_AWiderIndexLedByTheForeignKeysColumnsBacksIt(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(catalog.Index{
					Name: "f", TableName: "children", Columns: []string{"a", "b"},
				}),
				desiredChildren())

			c.Assert(removed, qt.HasLen, 0)
		})
	}
}

// TestCompare_ADifferentlyNamedCoveringIndexIsDroppable is the other half of
// requiring both signals.
//
// `cover(a)` can serve the foreign key, and the engine did not create it -- the
// author did, under a name of their own. It stays an ordinary index, so
// removing it from the desired schema plans its removal -- and so does `f`,
// because an index the engine had no reason to build is the author's whatever
// it is called.
func TestCompare_ADifferentlyNamedCoveringIndexIsDroppable(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(
					catalog.Index{Name: "cover", TableName: "children", Columns: []string{"a"}},
					catalog.Index{Name: "f", TableName: "children", Columns: []string{"a"}},
				),
				desiredChildren())

			// Both. `cover(a)` can serve the key, so the engine built no index
			// of its own and `f` is the author's despite sharing the
			// constraint's name -- measured, `DROP INDEX f` succeeds on both
			// engines with `cover(a)` beside `f(a)`.
			//
			// This assertion read `[]string{"cover"}` until the reader began
			// recording key-part direction (stokaro/ptah#2816). Without it the
			// catalog could not tell an index that covers from one that only
			// looks like it, so `f` was held as the engine's -- right where the
			// engine built it, and conservative where the author did.
			c.Assert(removed, qt.DeepEquals, []string{"cover", "f"})
		})
	}
}

// TestCompare_ASoleCoveringIndexUnderItsOwnNameIsStillTheAuthorsHappyPath is
// what the NAME condition is for, and nothing else here reaches it.
//
// `cover(a)` is the only index on the table and it backs the foreign key, so a
// rule reading columns alone calls it the engine's and suppresses it. It is the
// author's: they named it, and the engine builds its own only where nothing
// covers.
//
// Planning the removal is the right answer even though the engine will refuse
// it. Measured on both: `DROP INDEX cover` answers
// `ERROR 1553 (HY000): Cannot drop index 'cover': needed in a foreign key
// constraint`. A declaration that drops an index its own foreign key needs is a
// mistake, and an engine naming the reason is a better outcome than a
// comparison that reports the target synchronized and leaves the two disagreeing
// for good.
func TestCompare_ASoleCoveringIndexUnderItsOwnNameIsStillTheAuthorsHappyPath(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(catalog.Index{
					Name: "cover", TableName: "children", Columns: []string{"a"},
				}),
				desiredChildren())

			c.Assert(removed, qt.DeepEquals, []string{"cover"})
		})
	}
}

// TestCompare_ADescendingCoverStillMakesASameNamedIndexTheAuthorsHappyPath is
// the case ownership gets wrong when it consults direction.
//
// This row read `{mysql: ["cover"]}` until stokaro/ptah#2822, on a measurement
// that is correct about a different moment. Declaring `cover(a DESC)` and THEN
// adding the foreign key, MySQL 8.4.11 builds its own index and MariaDB 11.8.9
// reuses the cover -- so direction decides what the engine BUILDS.
//
// It does not decide what may be DROPPED, which is what ownership asks. Same
// servers, `cover(a DESC)` and `f(a)` both present with the constraint:
// `DROP INDEX f` succeeds on both, and MySQL keeps the constraint over the
// descending cover afterwards. Holding `f` as the engine's therefore reported
// `InSync` for an author who removed it, and the drop they asked for was one
// the server would have accepted -- the #2782 shape, narrowed.
//
// Both engines now remove both, and the engine's own index is still protected
// by the case below, where nothing else covers the key at all.
func TestCompare_ADescendingCoverStillMakesASameNamedIndexTheAuthorsHappyPath(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(
					catalog.Index{
						Name: "cover", TableName: "children", Columns: []string{"a"},
						Parts: []catalog.IndexPart{{Name: "a", Desc: true}},
					},
					catalog.Index{Name: "f", TableName: "children", Columns: []string{"a"}},
				),
				desiredChildren())

			c.Assert(removed, qt.DeepEquals, []string{"cover", "f"})
		})
	}
}

// TestCompare_ADescendingCoverBeyondTheKeysColumnsStillCovers is a wider cover
// than the key needs, carrying a descending part outside it.
//
// It pinned the LEADING-parts half of the direction rule until
// stokaro/ptah#2822 stopped ownership consulting direction at all -- measured
// on MySQL 8.4.11, `(a, b DESC)` is reused for a key on `a` and `(a DESC, b)`
// is not, which is a true statement about what the engine BUILDS and no longer
// one this decision reads.
//
// The case is kept because it is still worth answering: an index wider than the
// key covers it, and a rule that required an exact column list would hold `f`
// as the engine's and never plan the author's DROP.
func TestCompare_ADescendingCoverBeyondTheKeysColumnsStillCovers(t *testing.T) {
	for _, test := range ownershipEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := compareChildren(c, test.dialect,
				liveChildren(
					catalog.Index{
						Name: "cover", TableName: "children", Columns: []string{"a", "b"},
						Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}},
					},
					catalog.Index{Name: "f", TableName: "children", Columns: []string{"a"}},
				),
				desiredChildren())

			c.Assert(removed, qt.DeepEquals, []string{"cover", "f"})
		})
	}
}
