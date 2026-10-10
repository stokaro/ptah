package annotationmeta_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/ydbsource"
)

func sourceComments(file *ast.File) []*ast.Comment {
	var comments []*ast.Comment
	for _, group := range file.Comments {
		comments = append(comments, group.List...)
	}
	return comments
}

func TestAllowsAttributeValidatesPlatformOverrideShape(t *testing.T) {
	c := qt.New(t)

	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform.mysql.type"), qt.IsTrue)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform.mysql.generated.kind"), qt.IsTrue)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform.mysql"), qt.IsFalse)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform..type"), qt.IsFalse)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform.mysql.type-name"), qt.IsFalse)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:field", "platform.mysql.тип"), qt.IsFalse)
}

func TestAllowsAttribute_AcceptsRetainedPlatformOverrides(t *testing.T) {
	directives := []string{
		"ptah:schema:field",
		"ptah:embedded",
		"ptah:schema:table",
		"ptah:schema:index",
	}

	for _, directive := range directives {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(annotationmeta.Common().AllowsAttribute(directive, "platform.postgres.type"), qt.IsTrue)
		})
	}
}

func TestAllowsAttribute_AcceptsIndexInclude(t *testing.T) {
	c := qt.New(t)

	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:index", "include"), qt.IsTrue)
}

func TestAllowsAttribute_RejectsDroppedCompatibilitySyntax(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		attribute string
	}{
		{
			name:      "field nullable",
			directive: "ptah:schema:field",
			attribute: "nullable",
		},
		{
			name:      "field autoincrement",
			directive: "ptah:schema:field",
			attribute: "autoincrement",
		},
		{
			name:      "field index",
			directive: "ptah:schema:field",
			attribute: "index",
		},
		{
			name:      "embedded not null",
			directive: "ptah:embedded",
			attribute: "not_null",
		},
		{
			name:      "embedded index",
			directive: "ptah:embedded",
			attribute: "index",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(annotationmeta.Common().AllowsAttribute(test.directive, test.attribute), qt.IsFalse)
		})
	}
}

func TestAllowsAttribute_RejectsPlatformOverridesWithoutRuntimeSupport(t *testing.T) {
	directives := []string{
		"ptah:schema:schema",
		"ptah:schema:view",
		"ptah:schema:matview",
		"ptah:schema:trigger",
	}

	for _, directive := range directives {
		t.Run(directive, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(annotationmeta.Common().AllowsAttribute(directive, "platform.postgres.type"), qt.IsFalse)
		})
	}
}

func TestDetachedFileScopesMatchParserSupport(t *testing.T) {
	c := qt.New(t)

	for _, directive := range annotationmeta.Common().Directives() {
		for _, scope := range directive.Scopes {
			if scope != annotationmeta.ScopeFile {
				continue
			}
			c.Assert(directive.Name, qt.Matches, `ptah:schema:rls:(policy|enable)`)
		}
	}
}

func TestAllowsScope_UsesDirectiveMetadata(t *testing.T) {
	c := qt.New(t)

	table, ok := annotationmeta.Common().Lookup("ptah:schema:table")
	c.Assert(ok, qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(table, annotationmeta.ScopeStruct), qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(table, annotationmeta.ScopeFile), qt.IsFalse)

	policy, ok := annotationmeta.Common().Lookup("ptah:schema:rls:policy")
	c.Assert(ok, qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(policy, annotationmeta.ScopeStruct), qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(policy, annotationmeta.ScopeFile), qt.IsTrue)
}

func TestCommentPlacements_ClassifiesParserAndCleanupScopes(t *testing.T) {
	c := qt.New(t)
	file, err := parser.ParseFile(token.NewFileSet(), "model.go", `package models

//ptah:schema:rls:enable table="users"
const marker = 0

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id"
	ID int64

	//ptah:embedded mode="inline"
	Audit
}
`, parser.ParseComments)
	c.Assert(err, qt.IsNil)
	comments := sourceComments(file)
	c.Assert(comments, qt.HasLen, 4)
	placements := annotationmeta.CommentPlacements(file)

	c.Assert(placements[comments[0]], qt.DeepEquals, annotationmeta.Placement{Scope: annotationmeta.ScopeFile})
	c.Assert(placements[comments[1]], qt.DeepEquals, annotationmeta.Placement{
		Scope:      annotationmeta.ScopeStruct,
		StructName: "User",
	})
	c.Assert(placements[comments[2]], qt.DeepEquals, annotationmeta.Placement{
		Scope:      annotationmeta.ScopeField,
		StructName: "User",
		FieldNames: []string{"ID"},
		NamedField: true,
	})
	c.Assert(placements[comments[3]], qt.DeepEquals, annotationmeta.Placement{
		Scope:            annotationmeta.ScopeField,
		StructName:       "User",
		EmbeddedTypeName: "Audit",
	})
}

// TestTableDirectiveSpellsRowTTLAsCockroachDBProperties ties the table
// directive to the parameters the CockroachDB owner decodes.
//
// Row-level TTL is a CockroachDB platform property: a parameter is written as
// platform.cockroachdb.<parameter>, and the bare spelling is not an attribute
// of the directive. stokaro/ptah#1721 once added a parameter to the managed
// set without the directive, so the parameter answered `unknown annotation
// attribute` on the surface most authors use; the owner's list is the one this
// test iterates, so that cannot recur.
func TestTableDirectiveSpellsRowTTLAsCockroachDBProperties(t *testing.T) {
	for _, parameter := range crdbschema.ManagedParameters() {
		t.Run(parameter, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:table", "platform.cockroachdb."+parameter), qt.IsTrue)
			c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:table", parameter), qt.IsFalse)
		})
	}
}

// TestTableDirectiveSpellsRowDeletionAsPlatformProperties is the same tie for
// the row deletion policies of Spanner and YDB: each owner's property is
// written as platform.<dialect>.<property>, and the bare spelling a single
// shared attribute once had is not an attribute of the directive, so it cannot
// reach both targets at once.
func TestTableDirectiveSpellsRowDeletionAsPlatformProperties(t *testing.T) {
	owners := []struct {
		dialect     string
		definitions []schemaext.PropertyDefinition
	}{
		{dialect: "spanner", definitions: spannersource.Definitions()},
		{dialect: "ydb", definitions: ydbsource.TTLDefinitions()},
	}
	for _, owner := range owners {
		for _, definition := range owner.definitions {
			for _, property := range definition.Keys {
				t.Run(owner.dialect+"."+property, func(t *testing.T) {
					c := qt.New(t)
					c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:table", "platform."+owner.dialect+"."+property), qt.IsTrue)
					c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:table", property), qt.IsFalse)
				})
			}
		}
	}
}

func shadeOwner(c *qt.C, directive, attribute string) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner:    "example.org/paint",
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
		Attributes: []annotation.DirectiveAttributes{{
			Directive: directive, Attributes: []annotation.Attribute{{Name: attribute, Value: "string"}},
			Decode: func(map[string]string) (schemaext.Facets, error) { return schemaext.Facets{}, nil },
		}},
	})
	c.Assert(err, qt.IsNil)
	return set
}

// TestNewCatalog_AddsOwnerAttributesToTheFrontendDirective pins that an
// owner's attribute is known on the directive it extends, and only on that
// one, and only in a catalog that selected the owner.
func TestNewCatalog_AddsOwnerAttributesToTheFrontendDirective(t *testing.T) {
	c := qt.New(t)

	catalog, err := annotationmeta.NewCatalog(shadeOwner(c, "ptah:schema:matview", "shade"))

	c.Assert(err, qt.IsNil)
	c.Assert(catalog.AllowsAttribute("ptah:schema:matview", "shade"), qt.IsTrue)
	c.Assert(catalog.AllowsAttribute("ptah:schema:view", "shade"), qt.IsFalse)
	c.Assert(annotationmeta.Common().AllowsAttribute("ptah:schema:matview", "shade"), qt.IsFalse)
}

func TestNewCatalog_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		attribute string
		wantErr   string
	}{
		{name: "an attribute the directive declares already", directive: "ptah:schema:matview", attribute: "body",
			wantErr: `an owner adds attribute "body" to "ptah:schema:matview", which declares it already`},
		{name: "attributes on a directive the frontend does not own", directive: "ptah:schema:gadget", attribute: "shade",
			wantErr: `an owner adds attributes to "ptah:schema:gadget", which is not one of the frontend's own directives`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			catalog, err := annotationmeta.NewCatalog(shadeOwner(c, test.directive, test.attribute))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(catalog.Directives(), qt.HasLen, 0)
		})
	}
	c := qt.New(t)
	_, err := annotationmeta.NewCatalog(annotation.Set{})
	c.Assert(err, qt.ErrorIs, annotation.ErrUnselected)
}
