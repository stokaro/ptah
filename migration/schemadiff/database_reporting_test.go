package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompareWithDatabaseReportingUndecidedAdditionsUsesDatabaseDefaults(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		atlasurl.SQLiteURLFromPath(c.TempDir()+"/catalog.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	desired := &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_seq"}},
	}
	current := &types.DBSchema{
		Extensions:   []types.DBExtension{{Name: "plpgsql"}},
		NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
	}

	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		c.Context(), conn, desired, current, nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.Sequence, Name: "order_seq"},
	})
}
