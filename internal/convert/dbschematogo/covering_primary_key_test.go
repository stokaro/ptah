package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// coveringSchema is one table whose primary key covers the given columns and
// carries the given INCLUDE payload.
func coveringSchema(keyColumns, include []string) *catalog.Database {
	columns := make([]catalog.Column, 0, len(keyColumns)+1)
	for _, name := range keyColumns {
		columns = append(columns, catalog.Column{
			Name: name, DataType: "integer", IsNullable: "NO", IsPrimaryKey: true,
		})
	}
	columns = append(columns, catalog.Column{Name: "payload", DataType: "text", IsNullable: "YES"})
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "covering", Type: "BASE TABLE", Columns: columns,
		}},
		Constraints: []catalog.Constraint{{
			TableName:      "covering",
			Name:           "covering_pkey",
			Type:           "PRIMARY KEY",
			ColumnNames:    keyColumns,
			IncludeColumns: include,
		}},
	}
}

// coveringTable returns the converted table the key is on.
func coveringTable(c *qt.C, database *schemamodel.Database) schemamodel.Table {
	c.Helper()
	for _, table := range database.Tables {
		if table.Name == "covering" {
			return table
		}
	}
	c.Fatalf("no covering table in %+v", database.Tables)
	return schemamodel.Table{}
}

// primaryField returns the converted column named, for the flag assertions.
func primaryField(c *qt.C, database *schemamodel.Database, name string) schemamodel.Field {
	c.Helper()
	for _, field := range database.Fields {
		if field.Name == name {
			return field
		}
	}
	c.Fatalf("no %s field in %+v", name, database.Fields)
	return schemamodel.Field{}
}

// TestConvert_KeepsTheIncludePayloadOfACoveringPrimaryKey pins that the payload
// survives the description.
//
// It did not. The reader fills Constraint.IncludeColumns for every constraint
// kind, and the conversion then refuses to carry a primary key as a constraint
// at all -- deliberately, because a primary key is carried as Table.PrimaryKey
// so it renders once -- while the path that does carry primary keys read only
// the key columns. So `PRIMARY KEY (a, b) INCLUDE (payload)` was described as a
// plain `PRIMARY KEY (a, b)`, and applying that description back to the
// database it came from planned a DROP and an ADD that removed the payload from
// the live index, after which the run reported the schema as synced
// (stokaro/ptah#2199).
func TestConvert_KeepsTheIncludePayloadOfACoveringPrimaryKey(t *testing.T) {
	tests := []struct {
		name       string
		keyColumns []string
	}{
		// Two branches, not one shape twice: a single-column key was skipped
		// before it ever reached the map that carries primary keys, so a fix
		// for the composite case alone leaves this one lost.
		{name: "a composite key", keyColumns: []string{"a", "b"}},
		{name: "a single-column key", keyColumns: []string{"a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(coveringSchema(tt.keyColumns, []string{"payload"}), "")

			table := coveringTable(c, database)
			c.Assert(table.PrimaryKey, qt.DeepEquals, tt.keyColumns)
			c.Assert(table.PrimaryKeyInclude, qt.DeepEquals, []string{"payload"})
			// The table-level key and the column flag are two spellings of one
			// key. Both would declare it twice, and the payload can only hang
			// on the first.
			c.Assert(primaryField(c, database, tt.keyColumns[0]).Primary, qt.IsFalse)
		})
	}
}

// TestConvert_LeavesAPlainPrimaryKeyAsItWas is the control.
//
// A key with no payload loses nothing to the spellings that were already in
// use, and changing them would rewrite every description this repository has
// ever produced. A single-column key stays on the column, a composite one stays
// a table-level list, and neither gains an empty include.
func TestConvert_LeavesAPlainPrimaryKeyAsItWas(t *testing.T) {
	tests := []struct {
		name           string
		keyColumns     []string
		wantTableLevel []string
		wantColumnFlag bool
	}{
		{name: "a composite key", keyColumns: []string{"a", "b"}, wantTableLevel: []string{"a", "b"}, wantColumnFlag: false},
		{name: "a single-column key", keyColumns: []string{"a"}, wantTableLevel: nil, wantColumnFlag: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(coveringSchema(tt.keyColumns, nil), "")

			table := coveringTable(c, database)
			c.Assert(table.PrimaryKey, qt.DeepEquals, tt.wantTableLevel)
			c.Assert(table.PrimaryKeyInclude, qt.HasLen, 0)
			c.Assert(primaryField(c, database, tt.keyColumns[0]).Primary, qt.Equals, tt.wantColumnFlag)
		})
	}
}
