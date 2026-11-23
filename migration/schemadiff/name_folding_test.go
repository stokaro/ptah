package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemaprep"
	"ptah.run/migration/schemadiff"
)

// declaredMySQLTable is a document declaring the table name with one nullable
// integer column, as heldMySQLTable reads it back.
func declaredMySQLTable(name string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: name, Name: name}},
		Fields: []schemamodel.Field{{StructName: name, Name: "note", Type: "INT", Nullable: true}},
	}
}

// heldMySQLTable is a read of a MySQL session holding the table name, as
// declaredMySQLTable declares it.
func heldMySQLTable(name string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: name, Columns: []catalog.Column{
		{Name: "note", DataType: "int", ColumnType: "int", IsNullable: "YES", OrdinalPosition: 1},
	}}}}
}

// mysqlServer is a MySQL session on app whose server compares names as
// folding says.
func mysqlServer(folding catalog.NameFolding) catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "mysql", Schema: "app", NameFolding: folding}
}

// On a server that folds names, a table the document spells in another case
// is the table the read found, and nothing is created or dropped. On a server
// that keeps names apart by case it is another table.
func TestCompareWithDatabaseInfo_NameFolding_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		folding     catalog.NameFolding
		wantAdded   []string
		wantRemoved []string
	}{
		{name: "a server that folds names", folding: catalog.NameFoldingFolds},
		{name: "a server that keeps names apart", folding: catalog.NameFoldingExact, wantAdded: []string{"Items"}, wantRemoved: []string{"items"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declaredMySQLTable("Items"), heldMySQLTable("items"),
				mysqlServer(test.folding), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, test.wantAdded)
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, test.wantRemoved)
		})
	}
}

// A comparison whose server folding no connection read refuses a table the
// document spells in another case than the read, naming the pair, whether it
// was given a server or none.
func TestCompareWithDatabaseInfo_NameFolding_FailurePath(t *testing.T) {
	t.Run("a server built without a connection", func(t *testing.T) {
		c := qt.New(t)

		diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declaredMySQLTable("Items"), heldMySQLTable("items"),
			mysqlServer(catalog.NameFoldingUnknown), nil, must.Must(builtin.New()))

		var unknown *schemaprep.UnknownFoldingError
		c.Assert(err, qt.ErrorAs, &unknown)
		c.Assert(*unknown, qt.Equals, schemaprep.UnknownFoldingError{Kind: "table", Written: "Items", Found: "items"})
		c.Assert(diff, qt.IsNil)
	})
	t.Run("no server", func(t *testing.T) {
		c := qt.New(t)

		diff, err := schemadiff.CompareWithDialect(t.Context(), declaredMySQLTable("Items"), heldMySQLTable("items"), "mysql", must.Must(builtin.New()))

		c.Assert(err, qt.ErrorMatches, `table "Items" and "items" differ only in case, .*`)
		c.Assert(diff, qt.IsNil)
	})
}

// Where folding is unknown, a name spelled as the read spells it is compared
// as before, and a dialect whose servers do not decide folding by a setting
// is not refused at all.
func TestCompareWithDialect_NameFolding_Controls(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		declared    string
		wantAdded   []string
		wantRemoved []string
	}{
		{name: "MySQL, the same spelling", dialect: "mysql", declared: "items"},
		{name: "MySQL, another table", dialect: "mysql", declared: "orders", wantAdded: []string{"orders"}, wantRemoved: []string{"items"}},
		{name: "PostgreSQL, another case", dialect: "postgres", declared: "Items", wantAdded: []string{"Items"}, wantRemoved: []string{"items"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), declaredMySQLTable(test.declared), heldMySQLTable("items"), test.dialect,
				must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, test.wantAdded)
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, test.wantRemoved)
		})
	}
}
