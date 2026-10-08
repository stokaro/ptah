package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/schemadiff"
)

func TestCompareWithDatabaseReportingUndecidedAdditionsUsesDatabaseDefaults(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		atlasurl.SQLiteURLFromPath(c.TempDir()+"/currentCatalog.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	desired := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{Name: "order_seq"}},
	}
	current := &catalog.Database{
		Extensions:   []catalog.Extension{{Name: "plpgsql"}},
		NotDescribed: coverage.Set{}.WithKind(coverage.Sequence),
	}

	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(
		c.Context(), conn, desired, current, nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(diff.ExtensionsRemoved.Names(), qt.HasLen, 0)
	c.Assert(undecided.Common, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.Sequence, Name: "order_seq"},
	})
}
