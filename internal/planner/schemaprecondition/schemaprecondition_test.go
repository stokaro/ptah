package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/schemaprecondition"
)

// TestNode_CarriesTheDeclaration is the behavior stokaro/ptah#2618 was missing:
// a plan reaches a schema through an object's qualifier, which is a name and
// nothing else, so everything the declaration says about that schema has to be
// looked up here or it is lost.
//
// The rows separate the three attributes rather than asserting them together,
// because each is written by a different renderer and dropping one would
// otherwise hide behind the other two.
func TestNode_CarriesTheDeclaration(t *testing.T) {
	tests := []struct {
		name        string
		declared    []schemamodel.Schema
		semantics   identifier.Semantics
		wantComment string
		wantCharset string
		wantCollate string
	}{
		{
			name:        "comment",
			declared:    []schemamodel.Schema{{Name: "app", Comment: "the schema"}},
			semantics:   identifier.ForDialect("postgres"),
			wantComment: "the schema",
		},
		{
			name:        "character set",
			declared:    []schemamodel.Schema{{Name: "app", Charset: "utf8mb4"}},
			semantics:   identifier.ForDialect("mysql"),
			wantCharset: "utf8mb4",
		},
		{
			name:        "collation",
			declared:    []schemamodel.Schema{{Name: "app", Collate: "utf8mb4_general_ci"}},
			semantics:   identifier.ForDialect("mysql"),
			wantCollate: "utf8mb4_general_ci",
		},
		{
			// The exact spelling is preferred before any folding, so a
			// declaration that names the schema the way the qualifier does is
			// never re-interpreted as one that merely folds onto it.
			name: "exact spelling wins over a folded one",
			declared: []schemamodel.Schema{
				{Name: "App", Comment: "the folded one"},
				{Name: "app", Comment: "the exact one"},
			},
			semantics:   identifier.ForDialect("sqlite"),
			wantComment: "the exact one",
		},
		{
			// SQLite folds a schema name case-insensitively, so a declaration
			// spelled differently from the qualifier is still the same schema.
			name:        "a folded spelling still resolves",
			declared:    []schemamodel.Schema{{Name: "App", Comment: "the schema"}},
			semantics:   identifier.ForDialect("sqlite"),
			wantComment: "the schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := schemaprecondition.Node("app", test.declared, test.semantics)

			c.Assert(node.Name, qt.Equals, "app")
			c.Assert(node.IfNotExists, qt.IsTrue)
			c.Assert(node.Comment, qt.Equals, test.wantComment)
			c.Assert(node.Charset, qt.Equals, test.wantCharset)
			c.Assert(node.Collate, qt.Equals, test.wantCollate)
		})
	}
}

// TestNode_CreatesTheSchemaWithNothingAttached enumerates the cases where there
// is no single declaration to read, and every one of them still creates the
// schema.
//
// That is the decision rather than a fallback. The node is emitted because an
// object needs the schema to exist; withholding it because the declaration is
// missing or ambiguous would fail the migration on `schema "app" does not
// exist` to avoid attaching a comment.
func TestNode_CreatesTheSchemaWithNothingAttached(t *testing.T) {
	tests := []struct {
		name      string
		declared  []schemamodel.Schema
		semantics identifier.Semantics
	}{
		{
			name:      "no declarations at all",
			semantics: identifier.ForDialect("postgres"),
		},
		{
			// The established route: a schema nothing declares, reached only
			// through the qualified name of an object inside it.
			name:      "a schema no declaration names",
			declared:  []schemamodel.Schema{{Name: "other", Comment: "not this one"}},
			semantics: identifier.ForDialect("postgres"),
		},
		{
			// Two declarations that compare equal name no ONE declaration, and
			// choosing between their comments would write a coin toss into the
			// user's database.
			name: "two declarations fold onto the name",
			declared: []schemamodel.Schema{
				{Name: "App", Comment: "one"},
				{Name: "APP", Comment: "the other"},
			},
			semantics: identifier.ForDialect("sqlite"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := schemaprecondition.Node("app", test.declared, test.semantics)

			c.Assert(node.Name, qt.Equals, "app")
			c.Assert(node.IfNotExists, qt.IsTrue)
			c.Assert(node.Comment, qt.Equals, "")
			c.Assert(node.Charset, qt.Equals, "")
			c.Assert(node.Collate, qt.Equals, "")
		})
	}
}
