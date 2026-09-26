package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// SQLite never stores NULL in the rowid alias, a single-column INTEGER key on a
// rowid table, whether the column declares NOT NULL or not, while `pragma
// table_info` reports notnull 0 for one spelling and 1 for the other. The
// comparison must not read that flag as a nullability change: the only plan
// SQLite has for one is a full table rebuild, and it changes nothing
// (stokaro/ptah#3685).

// rowidAliasDesired is `widgets (id <type> PRIMARY KEY)` with the given
// nullability on the key column.
func rowidAliasDesired(columnType string, nullable bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widgets"}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: columnType, Primary: true, Nullable: nullable},
		},
	}
}

// rowidAliasCatalog is the same table as SQLite reports it.
func rowidAliasCatalog(columnType, isNullable string, primary bool) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "widgets",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: columnType, IsNullable: isNullable, IsPrimaryKey: primary},
			},
		}},
	}
}

func TestCompareWithDialect_SQLiteRowidAliasNullability_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{
			name:     "declared NOT NULL against a catalog that reports it nullable",
			desired:  rowidAliasDesired("INTEGER", false),
			database: rowidAliasCatalog("INTEGER", "YES", true),
		},
		{
			name:     "declared nullable against a catalog that reports it NOT NULL",
			desired:  rowidAliasDesired("integer", true),
			database: rowidAliasCatalog("INTEGER", "NO", true),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(tt.desired, tt.database, "sqlite")

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
		})
	}
}

// TestCompareWithDialect_SQLiteKeyNullability_FailurePath holds the rule to the
// alias. A TEXT key holds NULL on a rowid table, and a column that becomes the
// key is a change of its own, so each keeps its nullability change.
func TestCompareWithDialect_SQLiteKeyNullability_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{
			name:     "a TEXT key declared NOT NULL",
			desired:  rowidAliasDesired("TEXT", false),
			database: rowidAliasCatalog("TEXT", "YES", true),
		},
		{
			name:     "an INTEGER column that becomes the key",
			desired:  rowidAliasDesired("INTEGER", false),
			database: rowidAliasCatalog("INTEGER", "YES", false),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(tt.desired, tt.database, "sqlite")

			c.Assert(diff.TablesModified, qt.HasLen, 1, qt.Commentf("diff: %#v", diff))
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["nullable"], qt.Equals, "true -> false")
		})
	}
}
