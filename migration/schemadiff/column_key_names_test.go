package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// gainingKeyFixture is `c (id, a UNIQUE, b)` declared, and the same table live
// with no key over a. held names a unique index over b that both sides hold,
// or none when empty. missing names a column the live table lacks, so the
// column is added rather than changed, or none when empty.
func gainingKeyFixture(held, missing string) (*schemamodel.Database, *catalog.Database) {
	desired := desiredUniqueTable()
	live := liveUniqueTable()
	if held != "" {
		desired.Indexes = append(desired.Indexes, schemamodel.Index{
			StructName: "C", Name: held, Fields: []string{"b"}, Unique: true,
		})
		live.Indexes = append(live.Indexes, catalog.Index{
			Name: held, TableName: "c", Columns: []string{"b"}, IsUnique: true,
		})
	}
	live.Tables[0].Columns = slices.DeleteFunc(live.Tables[0].Columns, func(column catalog.Column) bool {
		return column.Name == missing
	})
	return desired, live
}

// tableDiffOf answers the diff of the table called name, and the zero diff
// when the comparison left it unmodified.
func tableDiffOf(diff *difftypes.SchemaDiff, name string) difftypes.TableDiff {
	position := slices.IndexFunc(diff.TablesModified, func(table difftypes.TableDiff) bool {
		return table.TableName == name
	})
	if position < 0 {
		return difftypes.TableDiff{}
	}
	return diff.TablesModified[position]
}

// TestCompare_ColumnKeyNames_HappyPath carries the name a column that gains
// its own UNIQUE takes, as the comparison reads it back as the column's key
// (stokaro/ptah#3859). Measured on PostgreSQL 18.6, `ALTER TABLE c ADD COLUMN
// a int UNIQUE` beside a unique index c_a_key over b names the key c_a_key1;
// MySQL 8.4.11 names the key a_2 beside an index a.
func TestCompare_ColumnKeyNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		held    string
		missing string
		want    map[string]string
	}{
		{
			name:    "PostgreSQL, the first name free",
			dialect: platform.Postgres,
			want:    map[string]string{"a": "c_a_key"},
		},
		{
			name:    "PostgreSQL, an index holds the first name",
			dialect: platform.Postgres, held: "c_a_key",
			want: map[string]string{"a": "c_a_key1"},
		},
		{
			name:    "PostgreSQL, a column added beside an index that holds the first name",
			dialect: platform.Postgres, held: "c_a_key", missing: "a",
			want: map[string]string{"a": "c_a_key1"},
		},
		{
			name:    "MySQL, an index holds the first name",
			dialect: platform.MySQL, held: "a",
			want: map[string]string{"a": "a_2"},
		},
		{
			name:    "SQLite, whose naming is not measured",
			dialect: platform.SQLite, held: "c_a_key",
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, live := gainingKeyFixture(test.held, test.missing)

			diff := compareForDialect(test.dialect, desired, live)

			c.Assert(tableDiffOf(diff, "c").ColumnKeyNames, qt.DeepEquals, test.want)
		})
	}
}
