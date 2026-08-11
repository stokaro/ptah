package annotationmeta_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/annotationmeta"
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

	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.type"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.generated.kind"), qt.IsTrue)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform..type"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.type-name"), qt.IsFalse)
	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:field", "platform.mysql.тип"), qt.IsFalse)
}

func TestAllowsAttribute_AcceptsRetainedPlatformOverrides(t *testing.T) {
	c := qt.New(t)

	directives := []string{
		"ptah:schema:field",
		"ptah:embedded",
		"ptah:schema:table",
	}

	for _, directive := range directives {
		c.Run(directive, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(directive, "platform.postgres.type"), qt.IsTrue)
		})
	}
}

func TestAllowsAttribute_AcceptsIndexInclude(t *testing.T) {
	c := qt.New(t)

	c.Assert(annotationmeta.AllowsAttribute("ptah:schema:index", "include"), qt.IsTrue)
}

func TestAllowsAttribute_RejectsDroppedCompatibilitySyntax(t *testing.T) {
	c := qt.New(t)

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
		c.Run(test.name, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(test.directive, test.attribute), qt.IsFalse)
		})
	}
}

func TestAllowsAttribute_RejectsPlatformOverridesWithoutRuntimeSupport(t *testing.T) {
	c := qt.New(t)

	directives := []string{
		"ptah:schema:index",
		"ptah:schema:schema",
		"ptah:schema:view",
		"ptah:schema:matview",
		"ptah:schema:trigger",
	}

	for _, directive := range directives {
		c.Run(directive, func(c *qt.C) {
			c.Assert(annotationmeta.AllowsAttribute(directive, "platform.postgres.type"), qt.IsFalse)
		})
	}
}

func TestDetachedFileScopesMatchParserSupport(t *testing.T) {
	c := qt.New(t)

	for _, directive := range annotationmeta.Directives() {
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

	table, ok := annotationmeta.Lookup("ptah:schema:table")
	c.Assert(ok, qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(table, annotationmeta.ScopeStruct), qt.IsTrue)
	c.Assert(annotationmeta.AllowsScope(table, annotationmeta.ScopeFile), qt.IsFalse)

	policy, ok := annotationmeta.Lookup("ptah:schema:rls:policy")
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
