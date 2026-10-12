package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// declaring returns base with the named extension as the owner's object.
func declaring(base func() *schemamodel.Database, name string, extension pgextension.DesiredExtension) *schemamodel.Database {
	db := base()
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref(name), extension))))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return db
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
		Indexes: []schemamodel.Index{{StructName: "Number", Name: "numbers_gin", Fields: []string{"n"}, Type: "gin", Requires: []objectidentity.ID{pgextension.Subject(pgextension.Ref("btree_gin"))}}},
	}
}

// TestRenderInspected_WritesTheOwnedExtensionBlock pins the block the native
// surface writes for an owned extension: placed in its schema, left to the
// default schema, and in a system schema, which is written as a string so the
// document declares no schema the server owns.
func TestRenderInspected_WritesTheOwnedExtensionBlock(t *testing.T) {
	tests := []struct {
		name      string
		extension string
		value     pgextension.DesiredExtension
		want      string
	}{
		{name: "every value", extension: "pg_trgm", value: pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true},
			want: "extension \"pg_trgm\" {\n  schema = schema.ext\n  if_not_exists = true\n  version = \"1.6\"\n  comment = \"trigrams\"\n}\n"},
		{name: "the default schema", extension: "pg_trgm", want: "extension \"pg_trgm\" {\n  schema = schema.public\n}\n"},
		{name: "a system schema", extension: "plpgsql", value: pgextension.DesiredExtension{Schema: "pg_catalog"},
			want: "extension \"plpgsql\" {\n  schema = \"pg_catalog\"\n}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(declaring(books, test.extension, test.value), "public",
				atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
		})
	}
}

// keptBecause is the warning the Atlas-compatible surface gives an extension
// block it keeps although the pinned binary refuses it.
func keptBecause(because, cost string) string {
	return "kept in Atlas-compatible schema inspect output because " + because +
		": the Atlas community CLI refuses a postgres schema file that declares any extension block, so it cannot read" +
		" this document, but omitting the block would " + cost
}

// TestRenderInspectedForAtlasCLI_KeepsAnOwnedExtensionTheDocumentNeeds pins
// why the Atlas-compatible surface keeps an owned extension: a name the
// document carries, a member the read reported, or an index the catalog
// resolved to it.
func TestRenderInspectedForAtlasCLI_KeepsAnOwnedExtensionTheDocumentNeeds(t *testing.T) {
	const named, nothing = "another object in this document depends on it", "leave a reference to an object nothing declares"
	tests := []struct {
		name      string
		base      func() *schemamodel.Database
		extension string
		value     pgextension.DesiredExtension
		want      string
		warning   string
	}{
		{name: "kept by its name", base: notes, extension: "citext", want: "extension \"citext\" {\n  schema = schema.public\n}\n",
			warning: keptBecause(named, nothing)},
		{name: "kept by a member", base: books, extension: "isn", value: pgextension.DesiredExtension{Provides: []string{"isbn", "ean13"}},
			want: "extension \"isn\" {\n  schema = schema.public\n}\n", warning: keptBecause(named, nothing)},
		{name: "kept by an index", base: numbers, extension: "btree_gin", want: "extension \"btree_gin\" {\n  schema = schema.public\n}\n",
			warning: keptBecause("the catalog resolved an index or constraint in this document to an operator class or access method it supplies",
				"leave an index no database could build")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(declaring(test.base, test.extension, test.value), "public",
				atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
			c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
				{Severity: "warning", Path: "extensions." + test.extension, Message: test.warning},
			})
		})
	}
}

// TestRenderInspectedForAtlasCLI_LeavesOutAnOwnedExtensionNothingNeeds is the
// control: an extension whose members the document does not name is left out,
// with the warning that says how to keep it.
func TestRenderInspectedForAtlasCLI_LeavesOutAnOwnedExtensionNothingNeeds(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspectedForAtlasCLI(declaring(books, "pg_trgm", pgextension.DesiredExtension{Provides: []string{"similarity"}}),
		"public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), `extension "pg_trgm"`)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{
		Severity: "warning", Path: "extensions.pg_trgm",
		Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses a postgres schema file" +
			" that declares any extension block, and one of them makes the whole document unreadable to it;" +
			" set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models",
	}})
}

// TestRenderInspected_RefusesAnObservedExtension pins that the document
// writes declarations only: an observation handed to it is refused rather
// than written as if someone had declared it.
func TestRenderInspected_RefusesAnObservedExtension(t *testing.T) {
	c := qt.New(t)
	observed := books()
	observed.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"),
		pgextension.ObservedExtension{Schema: "public", Version: "1.6"}))))

	result, err := atlashclrender.RenderInspected(observed, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*HCL requires desired extension values`)
	c.Assert(result.Data, qt.IsNil)
}
