package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
)

// ydbColumn is one column as the YDB reader reports it: the YDB type name in
// both type fields, and IS_NULLABLE from whether the type is Optional.
func ydbColumn(name, ydbType string, nullable bool) catalog.Column {
	column := catalog.Column{Name: name, DataType: ydbType, ColumnType: ydbType, IsNullable: "NO"}
	if nullable {
		column.IsNullable = "YES"
	}
	return column
}

// ydbTable is a row table in the directory shop, keyed by id, holding one
// column of each YDB type a Go field type depends on.
func ydbTable() *catalog.Database {
	id := ydbColumn("id", "Int64", false)
	id.IsPrimaryKey = true
	id.IsAutoIncrement = true
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name:   "orders",
			Schema: "shop",
			Columns: []catalog.Column{
				id,
				ydbColumn("small", "Int8", false),
				ydbColumn("count", "Uint32", true),
				ydbColumn("ratio", "Float", false),
				ydbColumn("raw", "String", true),
				ydbColumn("name", "Utf8", false),
				ydbColumn("doc", "JsonDocument", true),
				ydbColumn("created", "Timestamp64", false),
				ydbColumn("ttl", "Interval", true),
				ydbColumn("total", "Decimal(22,9)", true),
			},
		}},
		Constraints: []catalog.Constraint{{
			Name: "orders_pkey", TableName: "orders", Schema: "shop", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"},
		}},
	}
}

// introspect runs the conversion `ptah introspect` runs, for a database read
// from dialect, and returns the one generated file.
func introspect(c *qt.C, db *catalog.Database, dialect string) string {
	files, err := goschematogo.Render(dbschematogo.ConvertDBSchemaToGoSchema(db, dialect), goschematogo.Options{
		PackageName: "models",
		SingleFile:  true,
		Dialect:     dialect,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	return string(files[0].Data)
}

// A field read from YDB takes the Go type ydb-go-sdk scans the column into.
// Several YDB names mean another type on the SQL engines -- Int8 is one byte,
// Float is single precision, String is bytes -- and the generated import list
// carries what the types need.
func TestRender_YDBFieldsTakeTheTypeTheDriverScansInto(t *testing.T) {
	c := qt.New(t)
	source := introspect(c, ydbTable(), platform.YDB)

	for _, line := range []string{
		"\t\"github.com/ydb-platform/ydb-go-sdk/v3/table/types\"\n",
		"\t\"time\"\n",
		`//ptah:schema:table name="orders" schema="shop"` + "\n",
		`//ptah:schema:field name="id" type="Int64" not_null="true" primary="true" auto_increment="true"` + "\n\tId int64\n",
		"\tSmall int8\n",
		"\tCount *uint32\n",
		"\tRatio float32\n",
		"\tRaw []byte\n",
		"\tName string\n",
		"\tDoc *string\n",
		"\tCreated time.Time\n",
		"\tTtl *time.Duration\n",
		"\tTotal *types.Decimal\n",
	} {
		c.Assert(strings.Contains(source, line), qt.IsTrue, qt.Commentf("missing %q in:\n%s", line, source))
	}
}

// The same columns read under the SQL engines' spellings come out as those
// engines mean them. This is the control for the test above: it is the
// dialect, and nothing else in the columns, that decides the types there.
func TestRender_WithoutADialectTypeNamesReadAsSQL(t *testing.T) {
	c := qt.New(t)
	source := introspect(c, ydbTable(), "")

	for _, line := range []string{"\tSmall int64\n", "\tRatio float64\n", "\tRaw *string\n", "\tTotal *string\n"} {
		c.Assert(strings.Contains(source, line), qt.IsTrue, qt.Commentf("missing %q in:\n%s", line, source))
	}
	c.Assert(strings.Contains(source, "ydb-go-sdk"), qt.IsFalse)
}

// A YDB key column may be nullable, and no declaration can say so, so the
// conversion refuses it rather than write a key Ptah would rebuild NOT NULL.
// Both a column's own key flag and a table-level key are refused.
func TestRender_FailurePath_YDBNullableKeyColumn(t *testing.T) {
	tests := []struct {
		name string
		db   func() *catalog.Database
		want string
	}{
		{
			name: "single-column key",
			db: func() *catalog.Database {
				key := ydbColumn("k", "Utf8", true)
				key.IsPrimaryKey = true
				return &catalog.Database{
					Tables: []catalog.Table{{Name: "Nullable-Key", Columns: []catalog.Column{key}}},
					Constraints: []catalog.Constraint{{
						Name: "Nullable-Key_pkey", TableName: "Nullable-Key", Type: "PRIMARY KEY",
						ColumnName: "k", ColumnNames: []string{"k"},
					}},
				}
			},
			want: `column "k" of table "Nullable-Key" is a nullable key column, which no Ptah declaration can ` +
				`represent: Ptah writes NOT NULL on every YDB key column`,
		},
		{
			name: "second column of a composite key",
			db: func() *catalog.Database {
				first := ydbColumn("a", "Uint64", false)
				first.IsPrimaryKey = true
				second := ydbColumn("b", "Utf8", true)
				second.IsPrimaryKey = true
				return &catalog.Database{
					Tables: []catalog.Table{{Name: "pairs", Schema: "app", Columns: []catalog.Column{first, second}}},
					Constraints: []catalog.Constraint{{
						Name: "pairs_pkey", TableName: "pairs", Schema: "app", Type: "PRIMARY KEY",
						ColumnName: "a", ColumnNames: []string{"a", "b"},
					}},
				}
			},
			want: `column "b" of table "app.pairs" is a nullable key column, which no Ptah declaration can ` +
				`represent: Ptah writes NOT NULL on every YDB key column`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbschematogo.ConvertDBSchemaToGoSchema(test.db(), platform.YDB)

			files, err := goschematogo.Render(db, goschematogo.Options{Dialect: platform.YDB})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(files, qt.IsNil)
		})
	}
}

// A nullable key column read from another engine is that engine's to answer
// for -- SQLite's rowid alias is one -- and is written as read.
func TestRender_NullableKeyColumnOfAnotherDialect(t *testing.T) {
	c := qt.New(t)
	key := catalog.Column{Name: "id", DataType: "INTEGER", ColumnType: "INTEGER", IsNullable: "YES", IsPrimaryKey: true}
	db := &catalog.Database{Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{key}}}}

	source := introspect(c, db, platform.SQLite)

	c.Assert(strings.Contains(source, `//ptah:schema:field name="id" type="INTEGER" primary="true"`), qt.IsTrue,
		qt.Commentf("in:\n%s", source))
}
