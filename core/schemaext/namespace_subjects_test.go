package schemaext_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestNamespaceKnowledgeBoundsSubjectKindsAndParents(t *testing.T) {
	c := qt.New(t)
	parent := objectidentity.Part{Source: "A.B", Normalized: "a.b"}
	scope := schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.KindColumn}, Parent: &parent, PathExact: true}
	domain := scopedKnowledge(c, scope)
	base := objectidentity.ID{Kind: objectidentity.KindColumn, Parent: parent, Name: namespacePart("value")}
	c.Assert(domain.Lookup(base).State, qt.Equals, schemaext.Complete)
	for _, test := range []struct {
		name   string
		change func(*objectidentity.ID)
	}{
		{"other kind", func(id *objectidentity.ID) { id.Kind = objectidentity.KindTable }},
		{"other parent", func(id *objectidentity.ID) { id.Parent = namespacePart("other") }},
		{"qualified column", func(id *objectidentity.ID) { id.Namespace = objectidentity.NewParts(namespacePart("foreign")) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			id := base
			test.change(&id)
			c.Assert(domain.Lookup(id).State, qt.Equals, schemaext.Uninspected)
		})
	}
	scope.SubjectKinds[0] = objectidentity.KindTable
	parent.Normalized = "other"
	c.Assert(domain.Lookup(base).State, qt.Equals, schemaext.Complete)
	encoded, err := json.Marshal(domain)
	c.Assert(err, qt.IsNil)
	var restored schemaext.KnowledgeDomain
	c.Assert(json.Unmarshal(encoded, &restored), qt.IsNil)
	c.Assert(restored.Equal(domain), qt.IsTrue)
	c.Assert(restored.Lookup(base).State, qt.Equals, schemaext.Complete)
}

func TestNamespaceSubjectScopesRefuseMalformedKindsAndParents(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope schemaext.NamespaceScope
	}{
		{"empty kind", schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{""}}},
		{"repeated kind", schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.KindColumn, objectidentity.KindColumn}}},
		{"missing parent normalization", schemaext.NamespaceScope{Parent: &objectidentity.Part{Source: "table"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceKnowledge{Scope: test.scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}
