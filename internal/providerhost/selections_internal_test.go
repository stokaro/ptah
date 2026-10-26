package providerhost

// White-box testing required: the selector adapter validates identities before
// a provider's values enter the shared model; starting a provider crosses a
// process boundary and belongs to the integration tests.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/internal/tableidentity"
)

func scopeDescription() *protocol.Description {
	return &protocol.Description{ProviderId: "example.org/queue", Engines: []*protocol.Engine{{
		Id: "example", DefaultSchema: "main",
		Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
			ColumnNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE, IndexNames: protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
			IndexNamespace: protocol.IdentifierRules_SCHEMA},
		ScopeSelections: []*protocol.ScopeSelection{
			{Kind: "example.org/queue", SelectorType: "queue", Naming: protocol.ScopeSelection_IN_SCHEMA, GrantObjectType: "TOPIC"},
			{Kind: "example.org/pool", SelectorType: "pool", Naming: protocol.ScopeSelection_DATABASE_WIDE},
		},
	}}}
}

func scopePart(name string) objectidentity.Part {
	return objectidentity.Part{Source: name, Normalized: name}
}

func TestScopeSelectionsGrantUsesTheEnginesDefaultSchema(t *testing.T) {
	c := qt.New(t)
	selection, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)

	qualified, found := selection.extension.Grants.GrantObject("topic", "MAIN.Jobs")
	c.Assert(found, qt.IsTrue)
	unqualified, found := selection.extension.Grants.GrantObject(" TOPIC ", "jobs")
	c.Assert(found, qt.IsTrue)
	c.Assert(qualified.Key(), qt.Equals, unqualified.Key())
	c.Assert(qualified.Schema.Normalized, qt.Equals, "main")
	c.Assert(qualified.Name.Normalized, qt.Equals, "jobs")
	c.Assert(selection.fit(qualified), qt.IsNil)
}

func TestScopeSelectionsRefuseIdentitiesWhoseQualifiersWouldBeLost(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "database qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Database: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "catalog qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Catalog: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "parent qualifier", ref: objectidentity.ID{Kind: "example.org/queue", Parent: scopePart("other"), Schema: scopePart("main"), Name: scopePart("jobs")}},
		{name: "missing default schema", ref: objectidentity.ID{Kind: "example.org/queue", Name: scopePart("jobs")}},
		{name: "schema on a database object", ref: objectidentity.ID{Kind: "example.org/pool", Schema: scopePart("main"), Name: scopePart("fast")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selection, err := newScopeSelections(scopeDescription())
			c.Assert(err, qt.IsNil)
			c.Assert(selection.fit(test.ref), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestScopeSelectionsKeepDistinctObjectNames(t *testing.T) {
	c := qt.New(t)
	selection, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/queue", Schema: scopePart("main"), Name: scopePart("jobs")}), qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/pool", Name: scopePart("fast.pool")}), qt.IsNil)
	c.Assert(selection.fit(objectidentity.ID{Kind: "example.org/unselected", Database: scopePart("other"), Name: scopePart("jobs")}), qt.IsNil)
}

func TestScopeSelectionsBindOnlyTheOpenedDatabase(t *testing.T) {
	for _, test := range []struct {
		name, source, normalized, session string
		refused                           bool
	}{
		{name: "unbound", session: "/local"},
		{name: "session", source: "/local", normalized: "/local", session: "/local"},
		{name: "foreign", source: "/other", normalized: "/other", session: "/local", refused: true},
		{name: "normalization changed", source: "/LOCAL", normalized: "/local", session: "/local", refused: true},
		{name: "unverified", source: "/local", normalized: "/local", refused: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selection, err := newScopeSelections(scopeDescription())
			c.Assert(err, qt.IsNil)
			ref := objectidentity.ID{Kind: "example.org/queue", Database: objectidentity.Part{Source: test.source, Normalized: test.normalized}, Schema: scopePart("main"), Name: scopePart("jobs")}
			bound, err := selection.inSession(ref, test.session)
			c.Assert(err != nil, qt.Equals, test.refused)
			c.Assert(bound.Database.Empty(), qt.IsTrue)
			c.Assert(ref.Database.Source, qt.Equals, test.source)
		})
	}
}

func TestScopedInspectionCoverageSurvivesComparisonBinding(t *testing.T) {
	c := qt.New(t)
	selection, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	semantics := identifier.Semantics{DefaultSchema: "main"}
	r := reader{session: &session{target: &target{host: &Host{selections: selection}}, remote: &protocol.Session{Engine: "example", Database: "/opened"}, info: catalog.ServerInfo{IdentifierSemantics: semantics}}}
	domain, err := r.observedNamespaceKnowledge("example.org/queue", schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside the inspected database"}, []*protocol.ModelNamespaceKnowledge{{
		Scope:     &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: "/opened", Normalized: "/opened"}, Schema: &protocol.NamespaceComponent{Source: "main", Normalized: "main"}},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
	}})
	c.Assert(err, qt.IsNil)
	model := schemaext.CodecIdentity{Owner: "example.org/queue", Kind: "example.org/queue", Representation: schemaext.Observed, Version: 1, Definition: "definition"}
	coverage, err := schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: domain}}, nil)
	c.Assert(err, qt.IsNil)
	selected, err := schemaext.NewTargetSelection("example")
	c.Assert(err, qt.IsNil)
	bound, err := tableidentity.BindComparisonCoverage(coverage, selected, semantics)
	c.Assert(err, qt.IsNil)
	ref := objectidentity.ID{Kind: "example.org/queue", Schema: scopePart("main"), Name: scopePart("jobs")}
	c.Assert(bound.Lookup(model.Kind, ref).State, qt.Equals, schemaext.Complete)
	ref.Database = scopePart("/other")
	c.Assert(bound.Lookup(model.Kind, ref).State, qt.Equals, schemaext.Uninspected)
}

func TestScopeRequirementsUseCapturedContextWithoutChangingOwner(t *testing.T) {
	for _, row := range []struct {
		name               string
		context            *protocol.DatabaseContext
		source, normalized string
		pending, refused   bool
	}{
		{name: "bound", context: &protocol.DatabaseContext{Database: "/realm", SourceDatabase: "/authored"}, source: "/realm", normalized: "/realm"},
		{name: "foreign", context: &protocol.DatabaseContext{Database: "/realm", SourceDatabase: "/authored"}, source: "/foreign", normalized: "/foreign", refused: true},
		{name: "changed spelling", context: &protocol.DatabaseContext{Database: "/realm", SourceDatabase: "/authored"}, source: "/REALM", normalized: "/realm", refused: true},
		{name: "missing context", source: "/realm", normalized: "/realm", refused: true},
		{name: "pending", pending: true, refused: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			description := scopeDescription()
			description.Engines[0].ScopeSelections[0].KeepsRequired = []string{"example.org/pool"}
			selection, err := newScopeSelections(description)
			c.Assert(err, qt.IsNil)
			owner := &protocol.Reference{Engine: "example", Kind: "example.org/queue", Schema: "main", Name: "jobs"}
			value := &resolvedModel{kind: "example.org/queue", wire: modelWire{Features: []string{provider.ValueContextFeature}}, normalized: &protocol.NormalizedValue{
				Value: &protocol.Envelope{Owner: owner, Context: row.context}, NormalizedOwner: owner, RequiresDatabaseContext: row.pending,
			}}
			if !row.pending {
				value.normalized.RequiredObjects = []*protocol.ComparisonIdentity{{Kind: "example.org/pool", Database: &protocol.ComparisonPart{Source: row.source, Normalized: row.normalized}, Name: &protocol.ComparisonPart{Source: "fast", Normalized: "fast"}}}
			}
			kept, err := schemaext.NewObjects(schemaext.Object{Ref: objectidentity.ID{Kind: "example.org/queue", Schema: scopePart("main"), Name: scopePart("jobs")}, Value: value})
			c.Assert(err, qt.IsNil)
			result, err := selection.extension.Service.SelectObjects(c.Context(), selectext.Request{Kept: kept})
			if row.refused {
				c.Assert(err, qt.IsNotNil)
				c.Assert(result.Requires, qt.HasLen, 0)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(result.Requires, qt.HasLen, 1)
			c.Assert(result.Requires[0].Database.Empty(), qt.IsTrue)
			c.Assert(result.Requires[0].Name.Source, qt.Equals, "fast")
			c.Assert(owner.GetDatabase(), qt.Equals, "")
		})
	}
}
