package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestTableObservationOwnsCapturedIndexDefinitions(t *testing.T) {
	c := qt.New(t)
	table := catalog.Table{Name: "items", Schema: "app"}
	current := &catalog.Database{Indexes: []catalog.Index{{
		Name: "by_id", TableName: "items", Schema: "app", Columns: []string{"id"},
		StorageParams: map[string]string{"fillfactor": "70"}, NullsDistinct: new(false),
	}}}
	observation := difftypes.TableObservationFor(current, table, "postgres", identifier.ForDialect("postgres"))
	c.Assert(observation.Indexes, qt.HasLen, 1)
	current.Indexes[0].Columns[0], current.Indexes[0].StorageParams["fillfactor"] = "changed", "20"
	*current.Indexes[0].NullsDistinct = true
	c.Assert(observation.Indexes[0].Columns, qt.DeepEquals, []string{"id"})
	c.Assert(observation.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"fillfactor": "70"})
	c.Assert(*observation.Indexes[0].NullsDistinct, qt.IsFalse)
}
