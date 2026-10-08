package migratebaseline

// White-box testing required: baselineVersion, baselineRows and
// entityDriftError are unexported correctness primitives whose boundary
// behavior is not observable through the public command constructor without
// coupling the test to filesystem setup, and for an undecided object to a
// server that refuses a catalog read.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/cli/internal/schemaops"
	"ptah.run/migration/migrator"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
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

// TestEntityDriftErrorRefusesWhatTheReadCouldNotCheck holds the entity
// verification to what a baseline claims: the database already holds what the
// migrations create. A declared role the read was refused the catalog of is
// not shown to be there, so the baseline is refused on it as on a difference,
// and a difference is still reported as one (stokaro/ptah#3844).
func TestEntityDriftErrorRefusesWhatTheReadCouldNotCheck(t *testing.T) {
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	tests := []struct {
		name   string
		result *schemaops.CompareResult
		want   string
	}{
		{
			name:   "a declared role the read could not check",
			result: &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}}},
			want:   "baseline drift verification failed: 1 declared object could not be decided; see the warnings above",
		},
		{
			name: "a difference beside it",
			result: &schemaops.CompareResult{
				Diff:      &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "notes"}}},
				Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}},
			},
			want: `baseline drift verification failed: schema drift detected; findings: .*tables_added.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(entityDriftError(test.result), qt.ErrorMatches, test.want)
		})
	}
}

// TestEntityDriftErrorAcceptsAMatchingDatabase is the control: nothing
// differs and nothing was withheld.
func TestEntityDriftErrorAcceptsAMatchingDatabase(t *testing.T) {
	c := qt.New(t)

	err := entityDriftError(&schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}})

	c.Assert(err, qt.IsNil)
}
