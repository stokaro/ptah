package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// declaredBoth returns base twice: once with extension in the common model,
// and once with the same extension as the owner's object.
func declaredBoth(base func() *schemamodel.Database, extension schemamodel.Extension) (common, owned *schemamodel.Database) {
	common = base()
	common.Extensions = []schemamodel.Extension{extension}
	owned = base()
	owned.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref(extension.Name),
		pgextension.DesiredExtension{Schema: extension.Schema, Version: extension.Version, Comment: extension.Comment,
			IfNotExists: extension.IfNotExists, Provides: extension.Provides}))))
	owned.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return common, owned
}

// books declares a table whose column has a type an extension supplies.
func books() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Book", Name: "books"}},
		Fields: []schemamodel.Field{{StructName: "Book", Name: "id", Type: "INTEGER", Primary: true}, {StructName: "Book", Name: "code", Type: "isbn"}},
	}
}

// notes declares a table whose column type is named like the extension that
// supplies it.
func notes() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Note", Name: "notes"}},
		Fields: []schemamodel.Field{{StructName: "Note", Name: "id", Type: "INTEGER", Primary: true}, {StructName: "Note", Name: "body", Type: "citext"}},
	}
}

// numbers declares a GIN index over an integer column, which only btree_gin
// can build, as the catalog resolved it.
func numbers() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "Number", Name: "numbers"}},
		Fields:  []schemamodel.Field{{StructName: "Number", Name: "n", Type: "INTEGER", Primary: true}},
		Indexes: []schemamodel.Index{{StructName: "Number", Name: "numbers_gin", Fields: []string{"n"}, Type: "gin", RequiresExtensions: []string{"btree_gin"}}},
	}
}

// TestRenderInspected_WritesAnOwnedExtensionAsTheCommonOne holds the owner's
// extension to the block, the diagnostics and the coverage record the common
// model's extension gets, on both surfaces: placed, left to the default
// schema, in a system schema, kept on the Atlas-compatible surface by a name
// the document carries, by a member the read reported, or by an index the
// catalog resolved, and left out there when nothing needs it.
func TestRenderInspected_WritesAnOwnedExtensionAsTheCommonOne(t *testing.T) {
	native := atlashclrender.RenderInspected
	atlas := atlashclrender.RenderInspectedForAtlasCLI
	tests := []struct {
		name      string
		render    func(*schemamodel.Database, string, atlashclrender.Options) (atlashclrender.Result, error)
		base      func() *schemamodel.Database
		extension schemamodel.Extension
	}{
		{name: "every value", render: native, base: books,
			extension: schemamodel.Extension{Name: "pg_trgm", Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}},
		{name: "the default schema", render: native, base: books, extension: schemamodel.Extension{Name: "pg_trgm"}},
		{name: "a system schema", render: native, base: books, extension: schemamodel.Extension{Name: "plpgsql", Schema: "pg_catalog"}},
		{name: "kept by its name", render: atlas, base: notes, extension: schemamodel.Extension{Name: "citext"}},
		{name: "kept by a member", render: atlas, base: books, extension: schemamodel.Extension{Name: "isn", Provides: []string{"isbn", "ean13"}}},
		{name: "kept by an index", render: atlas, base: numbers, extension: schemamodel.Extension{Name: "btree_gin"}},
		{name: "left out", render: atlas, base: books, extension: schemamodel.Extension{Name: "pg_trgm", Provides: []string{"similarity"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			common, owned := declaredBoth(test.base, test.extension)

			want, err := test.render(common, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})
			c.Assert(err, qt.IsNil)
			got, err := test.render(owned, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(string(got.Data), qt.Equals, string(want.Data))
			c.Assert(got.Diagnostics, qt.DeepEquals, want.Diagnostics)
			c.Assert(got.NotDescribed, qt.DeepEquals, want.NotDescribed)
		})
	}
}

// TestRenderInspected_WritesTheOwnedExtensionBlock pins the block itself, so
// the comparison above cannot pass by writing nothing for either model.
func TestRenderInspected_WritesTheOwnedExtensionBlock(t *testing.T) {
	c := qt.New(t)
	_, owned := declaredBoth(books, schemamodel.Extension{Name: "pg_trgm", Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true})

	result, err := atlashclrender.RenderInspected(owned, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(string(result.Data), qt.Contains,
		"extension \"pg_trgm\" {\n  schema = schema.ext\n  if_not_exists = true\n  version = \"1.6\"\n  comment = \"trigrams\"\n}\n")
}

func TestRenderInspected_RefusesAnExtensionItCannotWrite(t *testing.T) {
	twice, _ := declaredBoth(books, schemamodel.Extension{Name: "pg_trgm"})
	_, owned := declaredBoth(books, schemamodel.Extension{Name: "pg_trgm"})
	twice.FeatureObjects, twice.FeatureCoverage = owned.FeatureObjects, owned.FeatureCoverage
	observed := books()
	observed.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"),
		pgextension.ObservedExtension{Schema: "public", Version: "1.6"}))))
	tests := []struct {
		name string
		db   *schemamodel.Database
		is   error
		want string
	}{
		{name: "declared by both models", db: twice, is: ptaherr.ErrInvalidSchemaDiff, want: `.*extension "pg_trgm" is declared twice`},
		{name: "an observation", db: observed, is: schemaext.ErrInvalidValue, want: `.*HCL requires desired extension values`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Data, qt.IsNil)
		})
	}
}
