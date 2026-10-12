package goschematogo_test

import (
	"maps"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
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

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, `platform.mariadb.charset="latin1" platform.mysql.charset="latin1"`)
	c.Assert(source, qt.Contains, `platform.mariadb.parser="ngram" platform.mysql.parser="ngram"`)
}

// mysqlSettingsSource declares every setting the MySQL owner writes into a Go
// annotation: the block-size hint of a table's primary key, of a PRIMARY KEY
// constraint, of an index and of a unique index, a FULLTEXT parser, and
// column settings.
const mysqlSettingsSource = `package entities

//ptah:schema:table name="docs" primary_key="id" primary_key_block_size="8"
type Doc struct {
	//ptah:schema:field name="id" type="BIGINT"
	ID int64
	//ptah:schema:field name="bio" type="TEXT" platform.mysql.charset="latin1"
	//ptah:schema:index name="ft_bio" fields="bio" type="FULLTEXT" key_block_size="4" platform.mysql.parser="ngram"
	Bio string
	//ptah:schema:field name="code" type="VARCHAR(16)"
	//ptah:schema:index name="uq_code" fields="code" unique="true" key_block_size="2"
	Code string
	//ptah:schema:field name="updated_at" type="TIMESTAMP" platform.mysql.on_update="CURRENT_TIMESTAMP"
	UpdatedAt string
}

//ptah:schema:table name="tags"
type Tag struct {
	//ptah:schema:field name="id" type="BIGINT"
	//ptah:schema:constraint name="pk_tags" type="PRIMARY KEY" columns="id" key_block_size="16"
	ID int64
}
`

// decodeForMySQL reads source as a MySQL schema: the Go annotations, then the
// platform properties the MySQL target decodes into the owner's settings.
func decodeForMySQL(c *qt.C, name string, source any) schemamodel.Database {
	c.Helper()
	runtime := builtintest.Runtime()
	parsed, err := goschema.ParseSource(runtime.Annotations(), name, source)
	c.Assert(err, qt.IsNil)
	decoded, err := schemaproperties.Decode(c.Context(), &parsed, "mysql", runtime)
	c.Assert(err, qt.IsNil)
	return *decoded
}

// mysqlSettings are the facets of every declaration that can hold the MySQL
// owner's settings, by declaration.
func mysqlSettings(db schemamodel.Database) map[string]schemaext.Facets {
	facets := make(map[string]schemaext.Facets)
	for _, table := range db.Tables {
		facets["table "+table.Name] = table.Facets
		facets["key of "+table.Name] = table.PrimaryKeyFacets
	}
	for _, field := range db.Fields {
		facets["field "+field.StructName+"."+field.Name] = field.Facets
	}
	for _, index := range db.Indexes {
		facets["index "+index.Name] = index.Facets
	}
	for _, constraint := range db.Constraints {
		facets["constraint "+constraint.Name] = constraint.Facets
	}
	return facets
}

// kindsOf are the kinds each declaration's facets hold, leaving out the
// declarations that hold none.
func kindsOf(facets map[string]schemaext.Facets) map[string][]schemaext.Kind {
	kinds := make(map[string][]schemaext.Kind)
	for declaration, each := range facets {
		kinds[declaration] = each.Kinds()
	}
	maps.DeleteFunc(kinds, func(_ string, held []schemaext.Kind) bool { return len(held) == 0 })
	return kinds
}

// TestRender_MySQLSettingsRoundTrip decodes a MySQL source, exports it with
// the bundled owners, and decodes the export again: every setting reads back
// as it was declared, the block-size hint of a unique index and of a PRIMARY
// KEY constraint among them.
func TestRender_MySQLSettingsRoundTrip(t *testing.T) {
	c := qt.New(t)
	declared := decodeForMySQL(c, "entities.go", mysqlSettingsSource)
	want := mysqlSettings(declared)
	c.Assert(kindsOf(want), qt.DeepEquals, map[string][]schemaext.Kind{
		"key of docs":          {mysqlschema.IndexBlockSizeKind},
		"field Doc.bio":        {mysqlschema.ColumnSettingsKind},
		"field Doc.updated_at": {mysqlschema.ColumnSettingsKind},
		"index ft_bio":         {mysqlschema.IndexKind, mysqlschema.IndexBlockSizeKind},
		"index uq_code":        {mysqlschema.IndexBlockSizeKind},
		"constraint pk_tags":   {mysqlschema.IndexBlockSizeKind},
	})

	files, err := goschematogo.Render(c.Context(), &declared, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	exported := decodeForMySQL(c, files[0].Name, files[0].Data)

	c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:index name="uq_code" fields="code" unique="true" table="docs" key_block_size="2"`+"\n")
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:index name="ft_bio" fields="bio" type="FULLTEXT" table="docs" key_block_size="4" platform.mysql.parser="ngram"`+"\n")
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:constraint name="pk_tags" type="PRIMARY KEY" table="tags" columns="id" key_block_size="16"`+"\n")
	c.Assert(mysqlSettings(exported), qt.DeepEquals, want)
}

// TestRender_AnIndexHintPrecedesYDBPartitioning pins the order of an index's
// attributes that two owners add: the MySQL hint with the index's own
// attributes, then the YDB partitioning this package still writes.
func TestRender_AnIndexHintPrecedesYDBPartitioning(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT"}},
		Indexes: []schemamodel.Index{{StructName: "Doc", Name: "ix_bio", Fields: []string{"bio"}, TableName: "docs",
			Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8},
				&ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 2}}))}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:index name="ix_bio" fields="bio" table="docs" key_block_size="8" auto_partitioning_min_partitions_count="2"`+"\n")
}

// TestRender_WritesTheBlockSizeOfAPrimaryKeyConstraint pins that a PRIMARY KEY
// constraint is recognized as the renderer recognizes one, in any case and
// with surrounding space, so its hint is written rather than refused.
func TestRender_WritesTheBlockSizeOfAPrimaryKeyConstraint(t *testing.T) {
	for _, constraintType := range []string{"PRIMARY KEY", "primary key", " Primary Key "} {
		t.Run(constraintType, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Tag", Name: "tags"}},
				Fields: []schemamodel.Field{{StructName: "Tag", FieldName: "ID", Name: "id", Type: "BIGINT"}},
				Constraints: []schemamodel.Constraint{{StructName: "Tag", Name: "pk_tags", Type: constraintType, Table: "tags", Columns: []string{"id"},
					Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 4))}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, `columns="id" key_block_size="4"`+"\n")
		})
	}
}

// TestRender_RefusesABlockSizeOfAUniqueConstraint pins that the export
// refuses a block-size hint on a UNIQUE constraint rather than drop it: MySQL
// keeps a UNIQUE key's hint on the unique index it is, so no source declares
// one on the constraint, and the renderer refuses it too.
func TestRender_RefusesABlockSizeOfAUniqueConstraint(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Code", Name: "code", Type: "VARCHAR(16)"}},
		Constraints: []schemamodel.Constraint{{StructName: "Doc", Name: "uq_code", Type: "UNIQUE", Table: "docs", Columns: []string{"code"},
			Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 2))}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature facet "ptah.run/mysql/index-block-size"`)
	c.Assert(files, qt.IsNil)
}
