package postgres

// White-box testing required: readCapabilityGatedObjects is unexported, and the
// record it makes is not reachable through ReadSchema without a live server --
// the exported path returns a whole DBSchema whose other members would have to
// be faked to reach this one field.
//
// A preset without pg_catalog's introspection helpers means this reader cannot
// ask what extensions the server has. It records the kind as not described so
// the comparator withholds rather than concluding one is missing
// (stokaro/ptah#942), and it now records WHY: the answer follows from what the
// target IS rather than from anything read out of it, which is the sentence a
// user needs to stop looking for a privilege to grant (stokaro/ptah#1346).

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

func TestExtensionsAreRecordedAsUnsupportedWhereTheCatalogCannotBeAsked(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want []coverage.Object
	}{
		{
			name: "a target whose preset rules the catalog out",
			caps: capability.Postgres16().With(capability.PostgresCatalogFunctions, false),
			want: []coverage.Object{{
				Kind:       coverage.Extension,
				Reason:     coverage.Unsupported,
				Provenance: coverage.DerivedFromTarget,
			}},
		},
		{
			// The control. A read that CAN look claims full authority over
			// extensions, so the record must not appear at all -- without this
			// row a reader that recorded the limit unconditionally would pass
			// the row above.
			name: "a target that has the catalog",
			caps: capability.Postgres16().With(capability.PostgresCatalogFunctions, true),
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{}, nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps)

			schema := &types.DBSchema{}
			c.Assert(reader.readCapabilityGatedObjects(t.Context(), schema), qt.IsNil)

			c.Assert(schema.NotDescribed.Objects, qt.DeepEquals, test.want)
		})
	}
}
