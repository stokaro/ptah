package migratebaseline

// White-box testing required: baselineVersion and baselineRows are unexported
// correctness primitives whose boundary behavior is not observable through the
// public command constructor without coupling the test to filesystem setup.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

func TestBaselineVersionDefaultsToHighestMigration(t *testing.T) {
	c := qt.New(t)

	version, err := baselineVersion("", []*migrator.Migration{
		migrator.CreateMigrationFromSQL(2, "second", "SELECT 2", "SELECT 2"),
		migrator.CreateMigrationFromSQL(10, "tenth", "SELECT 10", "SELECT 10"),
		migrator.CreateMigrationFromSQL(7, "seventh", "SELECT 7", "SELECT 7"),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(10))
}

func TestBaselineVersionValidatesExplicitValue(t *testing.T) {
	c := qt.New(t)

	version, err := baselineVersion("42", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(42))

	_, err = baselineVersion("0", nil)
	c.Assert(err, qt.ErrorMatches, `invalid baseline version "0"`)

	_, err = baselineVersion("abc", nil)
	c.Assert(err, qt.ErrorMatches, `invalid baseline version "abc"`)
}

func TestBaselineRowsIncludesOnlyVersionsAtOrBelowBaseline(t *testing.T) {
	c := qt.New(t)

	rows := baselineRows(7, []*migrator.Migration{
		migrator.CreateMigrationFromSQL(2, "second", "SELECT 2", "SELECT 2"),
		migrator.CreateMigrationFromSQL(7, "seventh", "SELECT 7", "SELECT 7"),
		migrator.CreateMigrationFromSQL(10, "tenth", "SELECT 10", "SELECT 10"),
	})

	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[0].Version, qt.Equals, int64(2))
	c.Assert(rows[1].Version, qt.Equals, int64(7))
}
