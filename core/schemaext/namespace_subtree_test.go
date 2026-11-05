package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// subtreeDomain claims every schema below the root complete under the
// separator /, and the subtree of schema hidden unknown.
func subtreeDomain(c *qt.C) schemaext.KnowledgeDomain {
	c.Helper()
	root := schemaext.NamespaceScope{Schema: new(namespacePart("")), SchemaSubtree: true, SchemaSeparator: "/"}
	hidden := schemaext.NamespaceScope{Schema: new(namespacePart("hidden")), SchemaSubtree: true, SchemaSeparator: "/"}
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside the read"},
		schemaext.NamespaceKnowledge{Scope: root, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		schemaext.NamespaceKnowledge{Scope: hidden, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "a hidden directory is not read"}})
	c.Assert(err, qt.IsNil)
	return domain
}

// A subtree region covers its own schema and every schema nested below it,
// and an unknown subtree below a complete one stays unknown.
func TestNamespaceScopeSubtree(t *testing.T) {
	for _, test := range []struct {
		name, schema string
		want         schemaext.KnowledgeState
	}{
		{name: "the root", schema: "", want: schemaext.Complete},
		{name: "a schema that does not exist yet", schema: "fresh/nested", want: schemaext.Complete},
		{name: "the unknown subtree", schema: "hidden", want: schemaext.Uninspected},
		{name: "a schema below the unknown subtree", schema: "hidden/inner", want: schemaext.Uninspected},
		{name: "a sibling sharing the unknown prefix", schema: "hiddenness", want: schemaext.Complete},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := widgetRef("secret", "pw")
			subject.Schema = namespacePart(test.schema)

			c.Assert(subtreeDomain(c).Lookup(subject).State, qt.Equals, test.want)
		})
	}
}

// Without the flag the same region covers its schema alone.
func TestNamespaceScopeWithoutSubtreeIsExact(t *testing.T) {
	c := qt.New(t)
	domain := scopedKnowledge(c, schemaext.NamespaceScope{Schema: new(namespacePart("app"))})
	nested := widgetRef("secret", "pw")
	nested.Schema = namespacePart("app/inner")

	c.Assert(domain.Lookup(nested).State, qt.Equals, schemaext.Uninspected)
}

func TestNamespaceScopeSubtree_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope schemaext.NamespaceScope
	}{
		{name: "a subtree without a schema", scope: schemaext.NamespaceScope{Database: new(namespacePart("db")), SchemaSubtree: true, SchemaSeparator: "/"}},
		{name: "a subtree without a separator", scope: schemaext.NamespaceScope{Schema: new(namespacePart("app")), SchemaSubtree: true}},
		{name: "a separator without a subtree", scope: schemaext.NamespaceScope{Schema: new(namespacePart("app")), SchemaSeparator: "/"}},
		{name: "a separator of two characters", scope: schemaext.NamespaceScope{Schema: new(namespacePart("app")), SchemaSubtree: true, SchemaSeparator: "//"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside"},
				schemaext.NamespaceKnowledge{Scope: test.scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got.Equal(schemaext.KnowledgeDomain{}), qt.IsTrue)
		})
	}
}

// The scope keeps the flag and the separator through its JSON form.
func TestNamespaceScopeSubtreeJSON(t *testing.T) {
	c := qt.New(t)
	domain := subtreeDomain(c)

	encoded, err := domain.MarshalJSON()
	c.Assert(err, qt.IsNil)
	var decoded schemaext.KnowledgeDomain
	c.Assert(decoded.UnmarshalJSON(encoded), qt.IsNil)

	c.Assert(decoded.Equal(domain), qt.IsTrue)
	c.Assert(string(encoded), qt.Contains, `"schema_subtree":true`)
	c.Assert(string(encoded), qt.Contains, `"schema_separator":"/"`)
}
