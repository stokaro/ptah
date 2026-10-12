package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// fieldPropertiesSource declares a field with a comment and platform
// properties for two targets.
const fieldPropertiesSource = `package entities

//ptah:schema:table name="docs"
type Doc struct {
	//ptah:schema:field name="bio" type="TEXT" comment="about" platform.mysql.charset="latin1" platform.postgres.storage="plain"
	Bio string
}
`

// TestRender_FieldPropertiesRoundTrip reads a Go source whose field states
// platform properties, exports the model, and reads the export back: the
// field keeps every property. The MySQL-family comment the reader derives
// from the field's comment is not written, since reading the comment derives
// it again.
func TestRender_FieldPropertiesRoundTrip(t *testing.T) {
	c := qt.New(t)
	declared, err := goschema.ParseSource(builtintest.Annotations(), "entities.go", fieldPropertiesSource)
	c.Assert(err, qt.IsNil)
	c.Assert(declared.Fields[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mariadb": {"comment": "about"}, "mysql": {"charset": "latin1", "comment": "about"}, "postgres": {"storage": "plain"},
	})

	files, err := goschematogo.Render(c.Context(), &declared, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	exported, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `comment="about" platform.mysql.charset="latin1" platform.postgres.storage="plain"`+"\n")
	c.Assert(string(files[0].Data), qt.Not(qt.Contains), "platform.mysql.comment")
	c.Assert(string(files[0].Data), qt.Not(qt.Contains), "platform.mariadb.comment")
	c.Assert(exported.Fields[0].Overrides, qt.DeepEquals, declared.Fields[0].Overrides)
}

// TestRender_WritesAFieldPropertyOfAnotherTarget pins that a property for
// another target than the export's stays in the source: the export decodes
// only its own target's properties, and the rest are written as they were
// stated. A MySQL property is written once on every target, from the setting
// the MySQL export decodes it into.
func TestRender_WritesAFieldPropertyOfAnotherTarget(t *testing.T) {
	for _, dialect := range []string{"postgres", "sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT", Nullable: true,
					Overrides: map[string]map[string]string{"mysql": {"charset": "latin1"}}}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(),
				Runtime: builtintest.Runtime(), Dialect: dialect, SingleFile: true})

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Count(string(files[0].Data), `platform.mysql.charset="latin1"`), qt.Equals, 1)
		})
	}
}

// TestRender_RefusesUnrepresentableFieldProperties pins that a field
// property a Go annotation cannot spell is refused by name rather than left
// out.
func TestRender_RefusesUnrepresentableFieldProperties(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]map[string]string
		wantErr    string
	}{
		{"a target with a separator", map[string]map[string]string{"my.sql": {"charset": "latin1"}},
			`unsupported feature: field "Doc.bio" has an unrepresentable Go annotation target "my.sql"`},
		{"a key with a space", map[string]map[string]string{"mysql": {"char set": "latin1"}},
			`unsupported feature: field "Doc.bio" has an unrepresentable Go annotation property "platform.mysql.char set"`},
		{"a value with NUL", map[string]map[string]string{"mysql": {"charset": "latin\x00"}},
			`unsupported feature: field "Doc.bio" has an unrepresentable Go annotation property "platform.mysql.charset"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT", Overrides: test.properties}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_RefusesAnAttributeWrittenTwice pins that a platform property a
// declaration states and an owner's setting that spells the same attribute
// are refused together: read back, one value would replace the other.
func TestRender_RefusesAnAttributeWrittenTwice(t *testing.T) {
	charset := must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "latin1"}))
	parser := must.Must(mysqlschema.WithIndexOptions(schemaext.Facets{}, mysqlschema.DesiredIndex{Parser: "ngram"}))
	mysql := func(key, value string) map[string]map[string]string {
		return map[string]map[string]string{"mysql": {key: value}}
	}
	for _, test := range []struct {
		name    string
		db      *schemamodel.Database
		wantErr string
	}{
		{"a field", &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
			Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT", Facets: charset,
				Overrides: mysql("charset", "utf8mb4")}},
		}, `unsupported feature: the Go annotation of field "bio" of table "docs" would write attribute "platform.mysql.charset" twice: .*`},
		{"an index", &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
			Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT"}},
			Indexes: []schemamodel.Index{{StructName: "Doc", Name: "ft_bio", Fields: []string{"bio"}, Type: "FULLTEXT", TableName: "docs",
				Facets: parser, Overrides: mysql("parser", "ngram")}},
		}, `unsupported feature: the Go annotation of index "ft_bio" of table "docs" would write attribute "platform.mysql.parser" twice: .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(c.Context(), test.db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_WritesFieldPropertiesTheReaderDoesNotDerive pins which comment
// the export leaves out: only a MySQL-family comment that equals the field's
// own non-empty comment, which reading the source back derives. Any other
// property is written.
func TestRender_WritesFieldPropertiesTheReaderDoesNotDerive(t *testing.T) {
	for _, test := range []struct {
		name, comment string
		properties    map[string]map[string]string
		want          string
	}{
		{"an empty comment on an uncommented field", "", map[string]map[string]string{"mysql": {"comment": ""}}, `platform.mysql.comment=""`},
		{"a comment that differs", "about", map[string]map[string]string{"mysql": {"comment": "other"}}, `platform.mysql.comment="other"`},
		{"another target's comment", "about", map[string]map[string]string{"postgres": {"comment": "about"}}, `platform.postgres.comment="about"`},
		{"another property with the comment's value", "about", map[string]map[string]string{"mysql": {"collation": "about"}},
			`platform.mysql.collation="about"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "Bio", Name: "bio", Type: "TEXT", Comment: test.comment,
					Overrides: test.properties}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.want)
		})
	}
}
