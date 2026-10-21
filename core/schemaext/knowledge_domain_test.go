package schemaext_test

import (
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func namespacePart(name string) objectidentity.Part {
	return objectidentity.Part{Source: name, Normalized: name}
}

func scopedKnowledge(c *qt.C, scope schemaext.NamespaceScope) schemaext.KnowledgeDomain {
	result, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside inspected namespace"},
		schemaext.NamespaceKnowledge{Scope: scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	c.Assert(err, qt.IsNil)
	return result
}

func TestKnowledgeDomainStructuredBoundaries(t *testing.T) {
	base := widgetRef("table", "name")
	base.Database, base.Catalog, base.Schema = namespacePart("db"), namespacePart("catalog"), namespacePart("public")
	base.Namespace = objectidentity.NewParts(namespacePart("team"), namespacePart("tables"))
	for _, test := range []struct {
		name  string
		scope schemaext.NamespaceScope
		want  schemaext.KnowledgeState
	}{
		{"database", schemaext.NamespaceScope{Database: new(namespacePart("db"))}, schemaext.Complete},
		{"other database", schemaext.NamespaceScope{Database: new(namespacePart("other"))}, schemaext.Uninspected},
		{"relative database is exact", schemaext.NamespaceScope{Database: new(objectidentity.Part{})}, schemaext.Uninspected},
		{"catalog", schemaext.NamespaceScope{Catalog: new(namespacePart("catalog"))}, schemaext.Complete},
		{"schema", schemaext.NamespaceScope{Schema: new(namespacePart("public"))}, schemaext.Complete},
		{"qualified intersection", schemaext.NamespaceScope{Database: new(namespacePart("db")), Schema: new(namespacePart("private"))}, schemaext.Uninspected},
		{"path descendant", schemaext.NamespaceScope{PathPrefix: objectidentity.NewParts(namespacePart("team"))}, schemaext.Complete},
		{"path sibling", schemaext.NamespaceScope{PathPrefix: objectidentity.NewParts(namespacePart("teams"))}, schemaext.Uninspected},
		{"joined path is distinct", schemaext.NamespaceScope{PathPrefix: objectidentity.NewParts(namespacePart("team/tables"))}, schemaext.Uninspected},
		{"longer path", schemaext.NamespaceScope{PathPrefix: objectidentity.NewParts(namespacePart("team"), namespacePart("tables"), namespacePart("child"))}, schemaext.Uninspected},
		{"normalized spelling", schemaext.NamespaceScope{Schema: &objectidentity.Part{Source: "PUBLIC", Normalized: "public"}}, schemaext.Complete},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			domain := scopedKnowledge(c, test.scope)
			c.Assert(domain.Lookup(base).State, qt.Equals, test.want)
			c.Assert(domain.Unscoped().State, qt.Equals, schemaext.Uninspected)
			c.Assert(domain.IsUninspected(), qt.IsFalse)
		})
	}
}

func TestKnowledgeDomainIntersectionAndTargets(t *testing.T) {
	c := qt.New(t)
	left := scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{" YDB "}, Database: new(namespacePart("one"))})
	right := scopedKnowledge(c, schemaext.NamespaceScope{Targets: []string{"spanner"}, Database: new(namespacePart("two"))})
	merged, err := left.Intersect(right)
	c.Assert(err, qt.IsNil)
	reverse, err := right.Intersect(left)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Equal(reverse), qt.IsTrue)
	repeat, err := merged.Intersect(merged)
	c.Assert(err, qt.IsNil)
	c.Assert(repeat.Equal(merged), qt.IsTrue)
	for _, name := range []string{"ydb", "spanner"} {
		target, err := schemaext.NewTargetSelection(name)
		c.Assert(err, qt.IsNil)
		projected, err := merged.ForTarget(target)
		c.Assert(err, qt.IsNil)
		for _, database := range []string{"one", "two", "three"} {
			subject := widgetRef("table", "value")
			subject.Database = namespacePart(database)
			c.Assert(projected.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
		}
	}
	target, err := schemaext.NewTargetSelection("ydb")
	c.Assert(err, qt.IsNil)
	selected, err := left.ForTarget(target)
	c.Assert(err, qt.IsNil)
	subject := widgetRef("table", "value")
	subject.Database = namespacePart("one")
	c.Assert(selected.Lookup(subject).State, qt.Equals, schemaext.Complete)
	unknown, err := left.Intersect(schemaext.KnowledgeDomain{})
	c.Assert(err, qt.IsNil)
	c.Assert(unknown.Lookup(subject).State, qt.Equals, schemaext.Uninspected)
}

func TestKnowledgeDomainOverlapsAndImmutableCapture(t *testing.T) {
	c := qt.New(t)
	schema := namespacePart("public")
	targets := []string{"ydb"}
	regions := []schemaext.NamespaceKnowledge{
		{Scope: schemaext.NamespaceScope{Targets: targets, Schema: &schema}, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Scope: schemaext.NamespaceScope{Schema: &schema, PathPrefix: objectidentity.NewParts(namespacePart("private"))}, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "permission denied"}},
	}
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside"}, regions...)
	c.Assert(err, qt.IsNil)
	schema.Normalized, targets[0] = "changed", "spanner"
	regions[0].Knowledge = schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "changed"}
	target, err := schemaext.NewTargetSelection("ydb")
	c.Assert(err, qt.IsNil)
	selected, err := domain.ForTarget(target)
	c.Assert(err, qt.IsNil)
	subject := widgetRef("table", "value")
	subject.Schema = namespacePart("public")
	c.Assert(selected.Lookup(subject).State, qt.Equals, schemaext.Complete)
	subject.Namespace = objectidentity.NewParts(namespacePart("private"))
	c.Assert(selected.Lookup(subject), qt.Equals, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "permission denied"})
	c.Assert(domain.Unscoped().State, qt.Equals, schemaext.Unrepresentable)
	data, err := json.Marshal(domain)
	c.Assert(err, qt.IsNil)
	var decoded schemaext.KnowledgeDomain
	c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)
	c.Assert(decoded.Equal(domain), qt.IsTrue)
	c.Assert(decoded.Lookup(subject), qt.Equals, domain.Lookup(subject))
}

func TestKnowledgeDomainInvalidClaims(t *testing.T) {
	for _, test := range []struct {
		name    string
		outside schemaext.Knowledge
		scope   schemaext.NamespaceScope
	}{
		{"no boundary", schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceScope{}},
		{"absence", schemaext.Knowledge{State: schemaext.Absent}, schemaext.NamespaceScope{Schema: new(namespacePart("public"))}},
		{"default", schemaext.Knowledge{State: schemaext.Defaulted}, schemaext.NamespaceScope{Schema: new(namespacePart("public"))}},
		{"missing reason", schemaext.Knowledge{}, schemaext.NamespaceScope{Schema: new(namespacePart("public"))}},
		{"invalid reason", schemaext.Knowledge{State: schemaext.Uninspected, Reason: "\xff"}, schemaext.NamespaceScope{Schema: new(namespacePart("public"))}},
		{"invalid qualifier", schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceScope{Schema: &objectidentity.Part{Source: "name"}}},
		{"invalid UTF-8", schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceScope{Schema: new(namespacePart("\xff"))}},
		{"empty path", schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceScope{PathPrefix: objectidentity.NewParts(objectidentity.Part{})}},
		{"invalid target", schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceScope{Targets: []string{"???"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := schemaext.NewScopedKnowledge(test.outside, schemaext.NamespaceKnowledge{Scope: test.scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got.Equal(schemaext.KnowledgeDomain{}), qt.IsTrue)
		})
	}
}

func TestKnowledgeDomainRefusesLegacyAndAmbiguousJSON(t *testing.T) {
	for _, input := range []string{
		`{"state":"complete"}`, `null`, `[]`,
		`[{"outside":{"state":"complete"},"extra":true}]`,
		`[{"outside":{"state":"complete"},"Outside":{"state":"complete"}}]`,
		`[{"outside":{"state":"complete"},"regions":[{"scope":{"schema":{"source":"public","normalized":"public"}},"knowledge":{"state":"absent"}}]}]`,
	} {
		t.Run(input, func(t *testing.T) {
			c := qt.New(t)
			initial := schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})
			result := initial
			c.Assert(json.Unmarshal([]byte(input), &result), qt.IsNotNil)
			c.Assert(result.Equal(initial), qt.IsTrue)
		})
	}
}

func TestKnowledgeDomainRepeatedIntersectionAtSourceLimit(t *testing.T) {
	c := qt.New(t)
	domain := schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source 0"})
	for i := 1; i < 256; i++ {
		next := schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: fmt.Sprintf("source %d", i)})
		merged, err := domain.Intersect(next)
		c.Assert(err, qt.IsNil)
		domain = merged
	}
	repeated, err := domain.Intersect(domain)
	c.Assert(err, qt.IsNil)
	c.Assert(repeated.Equal(domain), qt.IsTrue)
	_, err = domain.Intersect(schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "one too many"}))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
