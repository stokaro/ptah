package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/schemadiff"
)

// A foreign key the desired schema declares into a table an exclude selector
// leaves out names a table the server holds and the plan never drops, so it is
// planned once and then compared as the key the server holds
// (stokaro/ptah#6773). integration/excludedschema applies it twice against
// MySQL and MariaDB.

// kidsInto is a desired schema holding kids, whose key kids_parent_fk
// references reference with the delete action onDelete, or none when
// reference is empty.
func kidsInto(reference, onDelete string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Kids", Name: "kids"}},
		Fields: []schemamodel.Field{
			{StructName: "Kids", Name: "id", Type: "INT", Primary: true},
			{StructName: "Kids", Name: "parent_id", Type: "INT", Nullable: true},
		},
	}
	if reference != "" {
		desired.Constraints = []schemamodel.Constraint{{
			StructName: "Kids", Table: "kids", Name: "kids_parent_fk", Type: "FOREIGN KEY",
			Columns: []string{"parent_id"}, ForeignTable: reference, ForeignColumns: []string{"id"}, OnDelete: onDelete,
		}}
	}
	return desired
}

// kidsFieldInto is [kidsInto] with the key declared on its column.
func kidsFieldInto(reference string) *schemamodel.Database {
	desired := kidsInto("", "")
	desired.Fields[1].Foreign, desired.Fields[1].ForeignKeyName = reference+"(id)", "kids_parent_fk"
	return desired
}

// parentsOnly is a read of the database shop holding parents. A read of one
// database names no schema on what it holds.
func parentsOnly() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "parents", Columns: []catalog.Column{
		{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
	}}}}
}

// parentsAndKids is [parentsOnly] with kids, whose key into parents stands on
// the index the server built under the key's name. The key names no schema on
// the table it references, which is in the database read.
func parentsAndKids() *catalog.Database {
	read := parentsOnly()
	read.Tables = append(read.Tables, catalog.Table{Name: "kids", Columns: []catalog.Column{
		{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "parent_id", DataType: "int", ColumnType: "int", IsNullable: "YES"},
	}})
	read.Indexes = []catalog.Index{{TableName: "kids", Name: "kids_parent_fk", Columns: []string{"parent_id"}}}
	read.Constraints = []catalog.Constraint{{
		TableName: "kids", Name: "kids_parent_fk", Type: "FOREIGN KEY", ColumnNames: []string{"parent_id"},
		ForeignTable: new("parents"), ForeignColumns: []string{"id"}, DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
	}}
	return read
}

// compareExcluding compares desired with read after the exclude selector
// exclude, on a connection to the database shop of dialect, as `schema apply
// --exclude` does.
func compareExcluding(c *qt.C, dialect, exclude string, desired *schemamodel.Database, read *catalog.Database) (*difftypes.SchemaDiff, error) {
	runtime := must.Must(builtin.New())
	rules := must.Must(runtime.ComparisonRules(dialect))
	filtered, err := atlasfilter.ScopeDatabase(read, atlasfilter.Scope{
		Selections: runtime.Selections(), Dialect: dialect, DefaultSchema: "shop", Exclude: []string{exclude},
		ForeignKeyIndexes: rules.ForeignKeyIndexes,
	})
	c.Assert(err, qt.IsNil)
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "shop"
	info := catalog.ServerInfo{Dialect: dialect, Schema: "shop", IdentifierSemantics: semantics}
	return schemadiff.CompareWithDatabaseInfo(c.Context(), desired, filtered, info, nil, runtime)
}

// TestCompareWithDatabaseInfo_KeyIntoAnExcludedTable_HappyPath plans kids with
// its key into the excluded parents, however the key names parents. The
// qualified key meets the comparison's refusal of a key into its own schema,
// and the unqualified one the refusal of a key into a table the description
// does not hold.
func TestCompareWithDatabaseInfo_KeyIntoAnExcludedTable_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		desired       *schemamodel.Database
	}{
		{name: "mysql, qualified", dialect: "mysql", desired: kidsInto("shop.parents", "")},
		{name: "mysql, unqualified", dialect: "mysql", desired: kidsInto("parents", "")},
		{name: "mysql, qualified, on the column", dialect: "mysql", desired: kidsFieldInto("shop.parents")},
		{name: "mysql, unqualified, on the column", dialect: "mysql", desired: kidsFieldInto("parents")},
		{name: "mariadb, qualified", dialect: "mariadb", desired: kidsInto("shop.parents", "")},
		{name: "mariadb, unqualified", dialect: "mariadb", desired: kidsInto("parents", "")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareExcluding(c, test.dialect, "parents", test.desired, parentsOnly())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"kids"})
			c.Assert(diff.TablesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_KeyIntoAnExcludedTable_FailurePath still refuses
// a key into a table the selector keeps: the plan would drop parents, which
// the desired schema does not declare.
func TestCompareWithDatabaseInfo_KeyIntoAnExcludedTable_FailurePath(t *testing.T) {
	tests := []struct{ name, reference, wantErr string }{
		{
			name: "qualified", reference: "shop.parents",
			wantErr: `(?s).*constraint "kids_parent_fk" references unknown table "shop.parents": "shop" is the schema of the database compared.*`,
		},
		{name: "unqualified", reference: "parents", wantErr: `(?s).*constraint "kids_parent_fk" references unknown table "parents".*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := parentsOnly()
			read.Tables = append(read.Tables, catalog.Table{Name: "audit", Columns: []catalog.Column{
				{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
			}})

			diff, err := compareExcluding(c, "mysql", "audit", kidsInto(test.reference, ""), read)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_ComparesAKeyIntoAnExcludedTable compares the key
// the server holds with the declared one. A key the declaration states as the
// server holds it is unchanged, and so is the index the key stands on; one the
// declaration leaves out stays out of the plan with its index.
func TestCompareWithDatabaseInfo_ComparesAKeyIntoAnExcludedTable(t *testing.T) {
	tests := []struct {
		name, dialect string
		desired       *schemamodel.Database
	}{
		{name: "mysql, qualified", dialect: "mysql", desired: kidsInto("shop.parents", "")},
		{name: "mysql, unqualified", dialect: "mysql", desired: kidsInto("parents", "")},
		{name: "mysql, on the column", dialect: "mysql", desired: kidsFieldInto("parents")},
		{name: "mariadb, unqualified", dialect: "mariadb", desired: kidsInto("parents", "")},
		{name: "mysql, not declared", dialect: "mysql", desired: kidsInto("", "")},
		{name: "mariadb, not declared", dialect: "mariadb", desired: kidsInto("", "")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareExcluding(c, test.dialect, "parents", test.desired, parentsAndKids())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_ChangesAKeyIntoAnExcludedTable replaces the key
// the server holds when the declaration states it another way: the key
// belongs to kids, which the plan manages.
func TestCompareWithDatabaseInfo_ChangesAKeyIntoAnExcludedTable(t *testing.T) {
	c := qt.New(t)

	diff, err := compareExcluding(c, "mysql", "parents", kidsInto("parents", "CASCADE"), parentsAndKids())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved[0].Name, qt.Equals, "kids_parent_fk")
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Name, qt.Equals, "kids_parent_fk")
}
