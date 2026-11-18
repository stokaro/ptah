package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// Oracle stores an unquoted name in upper case and a quoted one exactly, so a
// read that reports orders has found a table created quoted, a different table
// from ORDERS, which is what a declared `orders` means. These tests compare
// against such a read under the rules a live Oracle read carries
// (identifier.ForCatalog), and against the same rows without them, which is
// what a schema predicted from a declaration is compared under
// (stokaro/ptah#6243).

// oracleRead is the server a live Oracle read reports, connected as APP.
func oracleRead() catalog.ServerInfo {
	semantics := identifier.ForCatalog("oracle")
	semantics.DefaultSchema = "APP"
	return catalog.ServerInfo{Dialect: "oracle", Schema: "APP", IdentifierSemantics: semantics}
}

// heldTable is a table the read reports in APP, with an ID key and the extra
// columns named.
func heldTable(name string, columns ...string) catalog.Table {
	table := catalog.Table{Name: name, Schema: "APP", Type: "TABLE", Columns: []catalog.Column{
		{Name: "ID", DataType: "NUMBER", UDTName: "NUMBER", IsNullable: "NO", IsPrimaryKey: true},
	}}
	for _, column := range columns {
		table.Columns = append(table.Columns, catalog.Column{Name: column, DataType: "NUMBER", UDTName: "NUMBER", IsNullable: "YES"})
	}
	return table
}

// declaredTable declares one table with an id key and the extra columns named.
func declaredTable(name string, columns ...string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: name, Name: name}},
		Fields: []schemamodel.Field{{StructName: name, Name: "id", Type: "NUMBER", Primary: true}},
	}
	for _, column := range columns {
		desired.Fields = append(desired.Fields, schemamodel.Field{StructName: name, Name: column, Type: "NUMBER", Nullable: true})
	}
	return desired
}

func TestCompareWithDatabaseInfo_CaseSensitiveOracleNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		held     []catalog.Table
		removed  []string
		added    []string
		modified []string
	}{
		{
			name:    "a quoted table is removed by its quoted name",
			desired: declaredTable("keep_me"),
			held:    []catalog.Table{heldTable("orders")},
			removed: []string{`APP."orders"`},
			added:   []string{"keep_me"},
		},
		{
			name:    "a quoted table and its folded namesake are two tables",
			desired: declaredTable("keep_me"),
			held:    []catalog.Table{heldTable("orders"), heldTable("ORDERS")},
			removed: []string{`APP."orders"`, "APP.ORDERS"},
			added:   []string{"keep_me"},
		},
		{
			name:    "a declared lower-case name reaches the table created bare",
			desired: declaredTable("keep_me"),
			held:    []catalog.Table{heldTable("KEEP_ME"), heldTable("keep_me")},
			removed: []string{`APP."keep_me"`},
		},
		{
			name:     "a quoted column of a matched table is removed by its quoted name",
			desired:  declaredTable("orders"),
			held:     []catalog.Table{heldTable("ORDERS", "qty")},
			modified: []string{`"qty"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.desired, &catalog.Database{Tables: test.held},
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert([]string(diff.TablesRemoved.Names()), qt.DeepEquals, test.removed)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(removedColumnNames(diff), qt.DeepEquals, test.modified)
		})
	}
}

// TestCompareWithDatabaseInfo_CaseSensitiveOracleNames_PredictedSchema is the
// control: rows that carry no catalog rules come from a declaration, where
// `orders` is written as declared and means ORDERS, so it pairs with the
// declared `orders` rather than being removed.
func TestCompareWithDatabaseInfo_CaseSensitiveOracleNames_PredictedSchema(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")
	semantics.DefaultSchema = "APP"
	predicted := catalog.ServerInfo{Dialect: "oracle", Schema: "APP", IdentifierSemantics: semantics}

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declaredTable("orders"),
		&catalog.Database{Tables: []catalog.Table{heldTable("orders")}}, predicted, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
}

func TestCompareWithDatabaseInfo_CaseSensitiveOracleNames_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		wantErr  string
	}{
		{
			name:     "a declared table whose fold only a quoted table matches",
			desired:  declaredTable("orders"),
			database: &catalog.Database{Tables: []catalog.Table{heldTable("orders")}},
			wantErr: `unsupported feature: declared table orders names ORDERS on oracle, which the database does not hold; ` +
				`the database holds the case-sensitive table "orders", which the declared name does not address\. ` +
				`Planning would drop "orders" and create ORDERS, so rename one of them`,
		},
		{
			name:     "a declared column whose fold only a quoted column matches",
			desired:  declaredTable("orders", "qty"),
			database: &catalog.Database{Tables: []catalog.Table{heldTable("ORDERS", "qty")}},
			wantErr:  `unsupported feature: declared column orders\.qty names ORDERS\.QTY on oracle, .* case-sensitive column ORDERS\."qty", .*`,
		},
		{
			name: "a declared index whose fold only a quoted index matches",
			desired: withIndex(declaredTable("orders", "qty"),
				schemamodel.Index{StructName: "orders", TableName: "orders", Name: "idx_qty", Fields: []string{"qty"}}),
			database: &catalog.Database{
				Tables:  []catalog.Table{heldTable("ORDERS", "QTY")},
				Indexes: []catalog.Index{{Name: "idx_qty", TableName: "ORDERS", Schema: "APP", Columns: []string{"QTY"}}},
			},
			wantErr: `unsupported feature: declared index idx_qty names IDX_QTY on oracle, .* case-sensitive index "idx_qty", .*`,
		},
		{
			name: "a declared constraint whose fold only a quoted constraint matches",
			desired: withConstraint(declaredTable("orders", "qty"),
				schemamodel.Constraint{StructName: "orders", Table: "orders", Name: "ck_qty", Type: "CHECK", CheckExpression: "qty > 0"}),
			database: &catalog.Database{
				Tables:      []catalog.Table{heldTable("ORDERS", "QTY")},
				Constraints: []catalog.Constraint{{Name: "ck_qty", TableName: "ORDERS", Schema: "APP", Type: "CHECK", CheckClause: new("QTY > 0")}},
			},
			wantErr: `unsupported feature: declared constraint ck_qty names CK_QTY on oracle, .* case-sensitive constraint "ck_qty", .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), test.desired, test.database,
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorAs, new(*schemadiff.RefusalError))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func withIndex(desired *schemamodel.Database, index schemamodel.Index) *schemamodel.Database {
	desired.Indexes = append(desired.Indexes, index)
	return desired
}

func withConstraint(desired *schemamodel.Database, constraint schemamodel.Constraint) *schemamodel.Database {
	desired.Constraints = append(desired.Constraints, constraint)
	return desired
}

// removedColumnNames lists the columns the diff removes from any table.
func removedColumnNames(diff *difftypes.SchemaDiff) []string {
	var names []string
	for _, table := range diff.TablesModified {
		names = append(names, table.ColumnsRemoved.Names()...)
	}
	return names
}
