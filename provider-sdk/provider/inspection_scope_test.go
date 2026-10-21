package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func filteredInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, r, s := inspection()
	r.Schemas = []string{"owner-selector"}
	ref := s.Coverage.Subjects[0].NormalizedSubject
	part := func(s string) *protocol.NamespaceComponent {
		return &protocol.NamespaceComponent{Source: s, Normalized: s}
	}
	scope := &protocol.ModelNamespace{Database: part(ref.Database), Catalog: part(ref.Catalog), Schema: part(ref.Schema), PathPrefix: []*protocol.NamespaceComponent{part(ref.Path[0])}}
	s.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: r.Schemas[0], Namespaces: []*protocol.ModelNamespace{scope}}}
	s.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "outside selector"}
	s.Coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{{Scope: proto.CloneOf(scope), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
	return d, r, s
}

func TestFilteredInspectionBindsOwnerNamespaces(t *testing.T) {
	c := qt.New(t)
	d, r, s := filteredInspection()
	original := proto.CloneOf(s)
	values, err := provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 1)
	c.Assert(proto.Equal(s, original), qt.IsTrue)
	// A child region retains tuple boundaries and may describe unknown knowledge.
	region := proto.CloneOf(s.Coverage.Kinds[0].Regions[0])
	region.Scope.PathPrefix = append(region.Scope.PathPrefix, &protocol.NamespaceComponent{Source: "child", Normalized: "child"})
	region.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not read"}
	s.Coverage.Kinds[0].Regions = append(s.Coverage.Kinds[0].Regions, region)
	_, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	// A named selector can resolve to nothing without granting global authority.
	s.ScopeBindings[0].Namespaces = nil
	s.NativeObjects = nil
	s.Coverage.Subjects = nil
	s.Coverage.Kinds[0].Regions = nil
	values, err = provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 0)
}

func TestFilteredInspectionRefusesChangedOrWidenedScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.InspectRequest, *protocol.Schema)
	}{
		{"missing binding", func(_ *protocol.InspectRequest, s *protocol.Schema) { s.ScopeBindings = nil }},
		{"renamed selector", func(_ *protocol.InspectRequest, s *protocol.Schema) { s.ScopeBindings[0].Selector = "foreign" }},
		{"extra selector", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.ScopeBindings = append(s.ScopeBindings, proto.CloneOf(s.ScopeBindings[0]))
		}},
		{"duplicate request", func(r *protocol.InspectRequest, s *protocol.Schema) {
			r.Schemas = append(r.Schemas, r.Schemas[0])
			s.ScopeBindings = append(s.ScopeBindings, proto.CloneOf(s.ScopeBindings[0]))
		}},
		{"empty selector", func(r *protocol.InspectRequest, s *protocol.Schema) {
			r.Schemas[0] = ""
			s.ScopeBindings[0].Selector = ""
		}},
		{"global completeness", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
		}},
		{"region database wildcard", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Kinds[0].Regions[0].Scope.Database = nil
		}},
		{"region parent prefix", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Kinds[0].Regions[0].Scope.PathPrefix = nil
		}},
		{"region delimiter collision", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Kinds[0].Regions[0].Scope.PathPrefix = []*protocol.NamespaceComponent{{Source: "a", Normalized: "a"}, {Source: "b/c", Normalized: "b/c"}}
		}},
		{"subject outside", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Subjects[0].NormalizedSubject.Schema = "foreign"
		}},
		{"empty binding with values", func(_ *protocol.InspectRequest, s *protocol.Schema) { s.ScopeBindings[0].Namespaces = nil }},
		{"unbound table", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.Tables = []*protocol.Table{{Identity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "name"}, NormalizedIdentity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "name", Schema: "foreign"}}}
		}},
		{"empty qualifier is not wildcard", func(_ *protocol.InspectRequest, s *protocol.Schema) {
			s.ScopeBindings[0].Namespaces[0].Database = &protocol.NamespaceComponent{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, s := filteredInspection()
			test.change(r, s)
			values, err := provider.InspectionValues(d, r, s)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(values, qt.IsNil)
		})
	}
}
