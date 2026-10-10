package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_MySQLSettingsBoundToTheFamily writes the MySQL owner's column
// settings and index options a SQL source binds to MySQL and MariaDB as the
// platform properties of both targets, which one target's property encoding
// cannot write.
func TestRender_MySQLSettingsBoundToTheFamily(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT", Nullable: true,
				Facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "latin1"}))},
		},
		Indexes: []schemamodel.Index{{StructName: "Doc", Name: "ft_bio", Fields: []string{"bio"}, Type: "FULLTEXT", TableName: "docs",
			Facets: must.Must(mysqlschema.WithIndexOptions(schemaext.Facets{}, mysqlschema.DesiredIndex{Parser: "ngram"}))}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, `platform.mariadb.charset="latin1" platform.mysql.charset="latin1"`)
	c.Assert(source, qt.Contains, `platform.mariadb.parser="ngram" platform.mysql.parser="ngram"`)
}
