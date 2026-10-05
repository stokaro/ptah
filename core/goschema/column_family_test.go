package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// columnFamilySource is an entity whose struct carries the given annotations
// beside its table directive, and a holder struct that carries others. Each
// line of onStruct ends with a newline, so an empty one leaves the struct's
// doc comment whole.
func columnFamilySource(onStruct, onHolder string) string {
	return `package entities

//ptah:schema:table name="items"
` + onStruct + `type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="body" type="TEXT"
	Body string
}

//ptah:schema:table name="orders" schema="shop"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

type Holder struct {
` + onHolder + `
	_ int
}
`
}

// TestParseSource_ColumnFamily_HappyPath reads YDB column families into the
// table they belong to: the struct's own, or the one named, in the order
// written.
func TestParseSource_ColumnFamily_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		onStruct  string
		onHolder  string
		wantItems []ast.YDBColumnFamilySpec
		wantShop  []ast.YDBColumnFamilySpec
	}{
		{
			name: "on the struct, settings folded as YDB folds them",
			onStruct: `//ptah:schema:columnfamily name="default" compression="LZ4"
//ptah:schema:columnfamily name="cold" data="hdd" cache_mode="IN_MEMORY" fields="body"
`,
			wantItems: []ast.YDBColumnFamilySpec{
				{Name: "default", Compression: "lz4"},
				{Name: "cold", Data: "hdd", CacheMode: "in_memory", Columns: []string{"body"}},
			},
		},
		{
			name:     "on a holder field, naming a table in a directory",
			onHolder: `	//ptah:schema:columnfamily name="cold" table="shop.orders" compression="lz4"`,
			wantShop: []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", columnFamilySource(test.onStruct, test.onHolder))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 2)
			c.Assert(db.Tables[0].YDBColumnFamilies, qt.DeepEquals, test.wantItems)
			c.Assert(db.Tables[1].YDBColumnFamilies, qt.DeepEquals, test.wantShop)
		})
	}
}

// TestParseSource_ColumnFamily_FailurePath refuses a declaration with no
// effect, or one YDB would refuse, where it was written.
func TestParseSource_ColumnFamily_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		onStruct string
		onHolder string
		wantErr  string
	}{
		{name: "no name", onStruct: `//ptah:schema:columnfamily compression="lz4"
`,
			wantErr: `missing required annotation attribute "name" on //ptah:schema:columnfamily at Item`},
		{name: "an unknown attribute", onStruct: `//ptah:schema:columnfamily name="cold" compression_level="3"
`,
			wantErr: `.*compression_level.*`},
		{name: "zstd", onStruct: `//ptah:schema:columnfamily name="cold" compression="zstd"
`,
			wantErr: `invalid compression "zstd": takes off or lz4: YDB keeps zstd for column-oriented tables, .*`},
		{name: "a holder with no table", onHolder: `	//ptah:schema:columnfamily name="cold"`,
			wantErr: `struct Holder maps to no table in this file; .*`},
		{name: "a table not in the file", onHolder: `	//ptah:schema:columnfamily name="cold" table="other"`,
			wantErr: `table "other" is not declared in this file, and a column family is declared beside its table .*`},
		{name: "one name twice", onStruct: `//ptah:schema:columnfamily name="cold"
//ptah:schema:columnfamily name="cold" compression="lz4"
`,
			wantErr: `table "items" declares column family "cold" twice on //ptah:schema:columnfamily at Item`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", columnFamilySource(test.onStruct, test.onHolder))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParseSource_ColumnFamily_RefusesAnInvalidValueAsSuch pins the sentinel
// and the attribute a value the declaration cannot carry is reported with.
func TestParseSource_ColumnFamily_RefusesAnInvalidValueAsSuch(t *testing.T) {
	c := qt.New(t)
	_, err := goschema.ParseSource("items.go",
		columnFamilySource(`//ptah:schema:columnfamily name="cold" cache_mode="hot"
`, ""))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	var parseErr *ptaherr.ParseError
	c.Assert(err, qt.ErrorAs, &parseErr)
	c.Assert(parseErr.Attribute, qt.Equals, "cache_mode")
}
