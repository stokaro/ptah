package tableidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableidentity"
)

func relativeScope(kinds ...objectidentity.Kind) schemaext.NamespaceScope {
	return schemaext.NamespaceScope{SubjectKinds: kinds, Database: &objectidentity.Part{}, Catalog: &objectidentity.Part{}, Schema: &objectidentity.Part{}, PathExact: true}
}

func boundedCoverage(c *qt.C, scope schemaext.NamespaceScope) schemaext.Coverage {
	c.Helper()
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete}, schemaext.NamespaceKnowledge{Scope: scope, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not inspected"}})
	c.Assert(err, qt.IsNil)
	result, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: schemaext.CodecIdentity{Owner: "example.org/owner", Kind: "example.org/model", Representation: schemaext.Observed, Version: 1, Definition: "definition"}, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	return result
}

func TestComparisonBindingMovesUnknownNamespacesWithSubjects(t *testing.T) {
	for _, test := range []struct {
		kind   objectidentity.Kind
		parent objectidentity.Part
	}{
		{kind: objectidentity.KindTable}, {kind: objectidentity.KindColumn, parent: objectidentity.Part{Source: "table", Normalized: "table"}}, {kind: objectidentity.KindFunction}, {kind: objectidentity.KindProcedure}, {kind: objectidentity.KindIndex},
	} {
		t.Run(string(test.kind), func(t *testing.T) {
			c := qt.New(t)
			scope := relativeScope(test.kind)
			source := boundedCoverage(c, scope)
			selected, err := schemaext.NewTargetSelection("postgres")
			c.Assert(err, qt.IsNil)
			semantics := identifier.ForDialect("postgres")
			semantics.DefaultSchema = "connection_schema"
			result, err := tableidentity.BindComparisonCoverage(source, selected, semantics)
			c.Assert(err, qt.IsNil)
			ref := objectidentity.ID{Kind: test.kind, Parent: test.parent, Schema: objectidentity.Part{Source: "connection_schema", Normalized: "connection_schema"}, Name: objectidentity.Part{Source: "object", Normalized: "object"}}
			c.Assert(source.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Complete)
			c.Assert(result.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Uninspected)
			c.Assert(result.Lookup("example.org/model", ref).Reason, qt.Equals, "not inspected")
			ref.Schema = objectidentity.Part{Source: "other", Normalized: "other"}
			c.Assert(result.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Complete)
		})
	}
}

func TestComparisonBindingPreservesUnknownColumnParent(t *testing.T) {
	c := qt.New(t)
	scope := relativeScope(objectidentity.KindColumn)
	scope.Parent = &objectidentity.Part{Source: "A.B", Normalized: "A.B"}
	source := boundedCoverage(c, scope)
	selected, err := schemaext.NewTargetSelection("sqlite")
	c.Assert(err, qt.IsNil)
	semantics := identifier.ForDialect("sqlite")
	semantics.DefaultSchema = "main"
	result, err := tableidentity.BindComparisonCoverage(source, selected, semantics)
	c.Assert(err, qt.IsNil)
	subject := objectidentity.NewBuilder(semantics).ColumnParts("main", "A.B", "value")
	c.Assert(result.Lookup("example.org/model", subject).State, qt.Equals, schemaext.Uninspected)
	sibling := objectidentity.NewBuilder(semantics).ColumnParts("main", "other", "value")
	c.Assert(result.Lookup("example.org/model", sibling).State, qt.Equals, schemaext.Complete)
	c.Assert(result.Lookup("example.org/model", tableidentity.Subject("main", "A.B", "sqlite", semantics)).State, qt.Equals, schemaext.Complete)
}

func TestComparisonBindingRetainsQualifiedNativeNamespaces(t *testing.T) {
	c := qt.New(t)
	scope := schemaext.NamespaceScope{Database: &objectidentity.Part{Source: "db", Normalized: "db"}, Schema: &objectidentity.Part{Source: "", Normalized: ""}, PathPrefix: objectidentity.NewParts(objectidentity.Part{Source: "team/part", Normalized: "team/part"})}
	source := boundedCoverage(c, scope)
	selected, err := schemaext.NewTargetSelection("fixture")
	c.Assert(err, qt.IsNil)
	result, err := tableidentity.BindComparisonCoverage(source, selected, identifier.Semantics{DefaultSchema: "host_default"})
	c.Assert(err, qt.IsNil)
	subject := objectidentity.ID{Kind: "example.org/native", Database: *scope.Database, Namespace: scope.PathPrefix, Name: objectidentity.Part{Source: "value", Normalized: "value"}}
	c.Assert(result.Lookup("example.org/model", subject).State, qt.Equals, schemaext.Uninspected)
	subject.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "team", Normalized: "team"}, objectidentity.Part{Source: "part", Normalized: "part"})
	c.Assert(result.Lookup("example.org/model", subject).State, qt.Equals, schemaext.Complete)
}

func TestComparisonBindingRefusesAmbiguousNamespaces(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope schemaext.NamespaceScope
	}{
		{"untyped", schemaext.NamespaceScope{Schema: &objectidentity.Part{}}},
		{"wildcard database", schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.KindTable}, Schema: &objectidentity.Part{}, PathExact: true}},
		{"wildcard path", schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.KindTable}, Schema: &objectidentity.Part{}, Database: &objectidentity.Part{}}},
		{"ambiguous native parent", relativeScope("example.org/native")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := boundedCoverage(c, test.scope)
			selected, err := schemaext.NewTargetSelection("postgres")
			c.Assert(err, qt.IsNil)
			result, err := tableidentity.BindComparisonCoverage(source, selected, identifier.ForDialect("postgres"))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.IsZero(), qt.IsTrue)
		})
	}
}

func TestComparisonBindingUsesResolvedTargetAliases(t *testing.T) {
	c := qt.New(t)
	scope := relativeScope(objectidentity.KindTable)
	scope.Targets = []string{"alias", "other"}
	domain, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside capture"}, schemaext.NamespaceKnowledge{Scope: scope, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	c.Assert(err, qt.IsNil)
	source, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: schemaext.CodecIdentity{Owner: "example.org/owner", Kind: "example.org/model", Representation: schemaext.Observed, Version: 1, Definition: "definition"}, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	selected, err := schemaext.NewTargetSelection("fixture", "alias")
	c.Assert(err, qt.IsNil)
	semantics := identifier.Semantics{DefaultSchema: "selected_default"}
	result, err := tableidentity.BindComparisonCoverage(source, selected, semantics)
	c.Assert(err, qt.IsNil)
	ref := tableidentity.Subject("", "table", "fixture", semantics)
	c.Assert(result.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Complete)
	other, err := schemaext.NewTargetSelection("other")
	c.Assert(err, qt.IsNil)
	foreign, err := result.ForTarget(other)
	c.Assert(err, qt.IsNil)
	c.Assert(foreign.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Uninspected)
	relative := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "table", Normalized: "table"}}
	c.Assert(source.Lookup("example.org/model", relative).State, qt.Equals, schemaext.Complete)
}

func TestVerbatimTableBindingRetainsConnectionDefault(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	semantics.DefaultSchema = "first"
	ref := tableidentity.Subject("", " table ", "sqlite", semantics)
	c.Assert(ref.Schema.Defaulted, qt.IsTrue)
	c.Assert(ref.Schema.Authored(), qt.Equals, "")
	c.Assert(ref.Name.Source, qt.Equals, " table ")
	scope := relativeScope(objectidentity.KindTable)
	scope.Schema = &ref.Schema
	source := boundedCoverage(c, scope)
	selected, err := schemaext.NewTargetSelection("sqlite")
	c.Assert(err, qt.IsNil)
	semantics.DefaultSchema = "second"
	bound, err := tableidentity.BindComparisonCoverage(source, selected, semantics)
	c.Assert(err, qt.IsNil)
	next := tableidentity.Subject("", " table ", "sqlite", semantics)
	c.Assert(bound.Lookup("example.org/model", next).State, qt.Equals, schemaext.Uninspected)
	c.Assert(bound.Lookup("example.org/model", ref).State, qt.Equals, schemaext.Complete)
}
