package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// persistedTable declares `t (id)`, logged or unlogged.
func persistedTable(unlogged bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, Unlogged: unlogged}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "bigint", Primary: true}},
	}
}

// livePersistedTable is the same table as a reader reports it.
func livePersistedTable(unlogged bool) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Unlogged: unlogged,
		Columns: []catalog.Column{{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true}},
	}}}
}

// TestCompare_Unlogged_HappyPath changes a PostgreSQL table between logged and
// unlogged, in both directions.
func TestCompare_Unlogged_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		desired bool
		current bool
	}{
		{name: "made unlogged", desired: true, current: false},
		{name: "made logged", desired: false, current: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, persistedTable(test.desired), livePersistedTable(test.current))

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].UnloggedChange, qt.DeepEquals,
				&difftypes.UnloggedChange{Current: test.current, Desired: test.desired})
		})
	}
}

// TestCompare_Unlogged_Synced plans nothing where the sides agree, and nothing
// on a target that creates no unlogged table: YugabyteDB takes the keyword and
// creates an ordinary table, so its read reports every table logged, and
// CockroachDB has none.
func TestCompare_Unlogged_Synced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		current bool
	}{
		{name: "PostgreSQL, both unlogged", dialect: platform.Postgres, current: true},
		{name: "YugabyteDB", dialect: platform.YugabyteDB, current: false},
		{name: "CockroachDB", dialect: platform.CockroachDB, current: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, persistedTable(true), livePersistedTable(test.current))

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
