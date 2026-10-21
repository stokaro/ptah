package schemaext_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestNamespaceBindingPreservesIndependentKnowledge(t *testing.T) {
	c := qt.New(t)
	scope := schemaext.NamespaceScope{Schema: new(namespacePart("source"))}
	first := scopedKnowledge(c, scope)
	second, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceKnowledge{Scope: scope, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "permission denied"}})
	c.Assert(err, qt.IsNil)
	source, err := first.Intersect(second)
	c.Assert(err, qt.IsNil)
	bound, err := source.MapScopes(func(scope schemaext.NamespaceScope) ([]schemaext.NamespaceScope, error) {
		scope.Schema = new(namespacePart("destination"))
		return []schemaext.NamespaceScope{scope}, nil
	})
	c.Assert(err, qt.IsNil)
	subject := objectidentity.ID{Kind: objectidentity.KindTable, Schema: namespacePart("destination"), Name: namespacePart("table")}
	c.Assert(bound.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(bound.Lookup(subject).Reason, qt.Equals, "permission denied")
	c.Assert(source.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
	c.Assert(first.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
}

func TestNamespaceBindingSnapshotsScopes(t *testing.T) {
	c := qt.New(t)
	source := scopedKnowledge(c, schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.KindColumn}, Parent: new(namespacePart("source"))})
	parent := namespacePart("destination")
	kinds := []objectidentity.Kind{objectidentity.KindColumn}
	bound, err := source.MapScopes(func(scope schemaext.NamespaceScope) ([]schemaext.NamespaceScope, error) {
		scope.Parent = &parent
		scope.SubjectKinds = kinds
		return []schemaext.NamespaceScope{scope}, nil
	})
	c.Assert(err, qt.IsNil)
	parent.Normalized = "changed"
	kinds[0] = objectidentity.KindTable
	subject := objectidentity.ID{Kind: objectidentity.KindColumn, Parent: namespacePart("destination"), Name: namespacePart("column")}
	c.Assert(bound.Lookup(subject).State, qt.Equals, schemaext.Complete)
	c.Assert(source.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
}

func TestNamespaceBindingRefusesPartialResults(t *testing.T) {
	c := qt.New(t)
	source := scopedKnowledge(c, schemaext.NamespaceScope{Schema: new(namespacePart("source"))})
	erased, err := source.MapScopes(func(schemaext.NamespaceScope) ([]schemaext.NamespaceScope, error) { return nil, nil })
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(erased.HasNamespaceScopes(), qt.IsFalse)
	failure := errors.New("cannot bind")
	partial, err := source.MapScopes(func(scope schemaext.NamespaceScope) ([]schemaext.NamespaceScope, error) {
		return []schemaext.NamespaceScope{scope}, failure
	})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(partial.HasNamespaceScopes(), qt.IsFalse)
}
