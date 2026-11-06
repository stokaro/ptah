package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// inheritedCopies is what a read returns for a partition and for a plain inheriting
// table: each holds a copy of its parent's check and one check of its own, and the
// partition holds a copy of the parent's index and one index of its own.
func inheritedCopies() *catalog.Database {
	inherited, own := "(id > 0)", "(id < 5)"
	column := catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO"}
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "events", Type: "BASE TABLE", Columns: []catalog.Column{column}},
			{Name: "e1", Type: "BASE TABLE", Columns: []catalog.Column{column}, Parents: []string{"events"},
				Facets: must.Must(schemaext.NewFacets(&pgtable.ObservedPartitionMember{Parent: "events", Bound: "FOR VALUES IN (1)"}))},
			{Name: "child", Type: "BASE TABLE", Columns: []catalog.Column{column}, Parents: []string{"events"}},
		},
		Constraints: []catalog.Constraint{
			{TableName: "events", Name: "positive", Type: "CHECK", CheckClause: &inherited},
			{TableName: "e1", Name: "positive", Type: "CHECK", CheckClause: &inherited, Inherited: true},
			{TableName: "e1", Name: "small", Type: "CHECK", CheckClause: &own},
			{TableName: "child", Name: "positive", Type: "CHECK", CheckClause: &inherited, Inherited: true},
			{TableName: "child", Name: "tiny", Type: "CHECK", CheckClause: &own},
		},
		Indexes: []catalog.Index{
			{Name: "idx_events", TableName: "events", Columns: []string{"id"}},
			{Name: "e1_id_idx", TableName: "e1", Columns: []string{"id"}, PartitionAttached: true},
			{Name: "idx_e1", TableName: "e1", Columns: []string{"id"}},
		},
	}
}

// TestConvert_LeavesOutWhatAPartitionHoldsFromItsParent keeps what a partition
// declares for itself and drops the server's copies, whose replay would answer
// `already exists`. A plain inheriting table has no statement that makes its copies
// for it in the description, so it keeps them.
func TestConvert_LeavesOutWhatAPartitionHoldsFromItsParent(t *testing.T) {
	c := qt.New(t)
	live := inheritedCopies()

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), live, platform.Postgres, must.Must(builtin.New())))

	constraints := make([]string, 0, len(database.Constraints))
	for _, constraint := range database.Constraints {
		constraints = append(constraints, constraint.Table+"."+constraint.Name)
	}
	indexes := make([]string, 0, len(database.Indexes))
	for _, index := range database.Indexes {
		indexes = append(indexes, index.Name)
	}
	c.Assert(constraints, qt.ContentEquals, []string{"events.positive", "e1.small", "child.positive", "child.tiny"})
	c.Assert(indexes, qt.ContentEquals, []string{"idx_events", "idx_e1"})
	c.Assert(live.Constraints, qt.HasLen, 5)
	c.Assert(live.Indexes, qt.HasLen, 3)
}

// TestConvert_KeepsAPartitionsOwnKeysOffItsColumns holds a partition's own key where a
// declaration with no column list can write it. An ordinary table is the control: a
// single-column key stays the column's compact spelling.
func TestConvert_KeepsAPartitionsOwnKeysOffItsColumns(t *testing.T) {
	c := qt.New(t)
	column := func(name string, unique, primary bool) catalog.Column {
		return catalog.Column{Name: name, DataType: "integer", IsNullable: "NO", IsPrimaryKey: primary, IsUnique: unique}
	}
	tables := []catalog.Table{
		{Name: "events", Type: "BASE TABLE", Columns: []catalog.Column{column("id", false, false), column("tag", false, false)}},
		{Name: "e1", Type: "BASE TABLE", Columns: []catalog.Column{column("id", false, true), column("tag", true, false)},
			Facets: must.Must(schemaext.NewFacets(&pgtable.ObservedPartitionMember{Parent: "events", Bound: "DEFAULT"}))},
		{Name: "plain", Type: "BASE TABLE", Columns: []catalog.Column{column("id", false, true), column("tag", true, false)}},
	}
	constraints := []catalog.Constraint{
		{TableName: "e1", Name: "e1_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
		{TableName: "e1", Name: "e1_tag_key", Type: "UNIQUE", ColumnName: "tag", ColumnNames: []string{"tag"}},
		{TableName: "plain", Name: "plain_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
		{TableName: "plain", Name: "plain_tag_key", Type: "UNIQUE", ColumnName: "tag", ColumnNames: []string{"tag"}},
	}

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{Tables: tables, Constraints: constraints}, platform.Postgres, must.Must(builtin.New())))

	keys := make(map[string][]string)
	for _, table := range database.Tables {
		keys[table.Name] = table.PrimaryKey
	}
	c.Assert(keys, qt.DeepEquals, map[string][]string{"events": nil, "e1": {"id"}, "plain": nil})
	named := make([]string, 0, 1)
	for _, constraint := range database.Constraints {
		named = append(named, constraint.Name)
	}
	c.Assert(named, qt.DeepEquals, []string{"e1_tag_key"})
}

// TestConvert_DeclaresThePartitionTheReadFound turns the observation into the
// declaration a document would write, with the columns kept for an export that has
// no spelling for a partition.
func TestConvert_DeclaresThePartitionTheReadFound(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), inheritedCopies(), platform.Postgres, must.Must(builtin.New())))

	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](database.Tables[1].Facets, pgtable.PartitionMemberKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.PartitionMember, qt.DeepEquals, pgtable.PartitionMember{Parent: "events", Bound: "FOR VALUES IN (1)"})
}
