package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// databaseInspection is a read filtered to the schema app whose engine
// declares its native kind a database kind: it lists nothing in app and
// reports one object of that kind named at the database, with complete
// subject coverage and a complete region at the database namespace.
func databaseInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, r, s := inspection()
	value := s.NativeObjects[0]
	engine := d.Engines[0]
	d.Features = append(d.Features, provider.DatabaseKindsFeature)
	engine.Features = append(engine.Features, provider.DatabaseKindsFeature)
	engine.DatabaseKinds = []string{value.Kind}
	s.TargetFeatures = append(s.TargetFeatures, provider.DatabaseKindsFeature)
	value.Owner = &protocol.Reference{Engine: "fixture", Database: "database", Kind: value.Kind, Name: "pool"}
	s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(value)}
	part := func(text string) *protocol.NamespaceComponent {
		return &protocol.NamespaceComponent{Source: text, Normalized: text}
	}
	r.Schemas = []string{"app"}
	s.ScopeBindings = []*protocol.InspectionScopeBinding{{Selector: "app", Namespaces: []*protocol.ModelNamespace{{Database: part("database"), Catalog: part(""), Schema: part("app")}}}}
	s.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read knows the kind in its database alone"}
	s.Coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{{Scope: &protocol.ModelNamespace{Database: part("database"), Catalog: part(""), Schema: part("")},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
	return d, r, s
}

func TestDatabaseKindsInspection_HappyPath(t *testing.T) {
	t.Run("an object beside the scope", func(t *testing.T) {
		c := qt.New(t)
		d, r, s := databaseInspection()
		before := proto.CloneOf(s)
		values, err := provider.InspectionValues(d, r, s)
		c.Assert(err, qt.IsNil)
		c.Assert(values.GetValues(), qt.HasLen, 1)
		c.Assert(values.GetValues()[0].GetValue().GetOwner().GetName(), qt.Equals, "pool")
		c.Assert(proto.Equal(s, before), qt.IsTrue)
	})
	t.Run("no object, the database known to hold none", func(t *testing.T) {
		c := qt.New(t)
		d, r, s := databaseInspection()
		s.NativeObjects, s.Coverage.Subjects = nil, nil
		values, err := provider.InspectionValues(d, r, s)
		c.Assert(err, qt.IsNil)
		c.Assert(values.GetValues(), qt.HasLen, 0)
	})
	t.Run("an unfiltered read", func(t *testing.T) {
		c := qt.New(t)
		d, r, s := databaseInspection()
		r.Schemas, s.ScopeBindings = nil, nil
		values, err := provider.InspectionValues(d, r, s)
		c.Assert(err, qt.IsNil)
		c.Assert(values.GetValues(), qt.HasLen, 1)
	})
}

// Everything that is not a negotiated database kind named at the database
// keeps the scope rule.
func TestDatabaseKindsInspection_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*protocol.Description, *protocol.Schema)
		wantErr string
	}{
		{"a target without the feature", func(_ *protocol.Description, s *protocol.Schema) {
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.DatabaseKindsFeature })
		}, `.*inspected subject is outside the requested scope`},
		{"a kind that is not a database kind", func(d *protocol.Description, _ *protocol.Schema) {
			d.Engines[0].DatabaseKinds = []string{"example.org/fixture/other"}
		}, `.*inspected subject is outside the requested scope`},
		{"an object with a schema", func(_ *protocol.Description, s *protocol.Schema) {
			s.NativeObjects[0].Owner.Schema = "elsewhere"
			s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(s.NativeObjects[0])}
		}, `.*inspected subject is outside the requested scope`},
		{"an object with a catalog", func(_ *protocol.Description, s *protocol.Schema) {
			s.NativeObjects[0].Owner.Catalog = "catalog"
			s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(s.NativeObjects[0])}
		}, `.*inspected subject is outside the requested scope`},
		{"an object with a path", func(_ *protocol.Description, s *protocol.Schema) {
			s.NativeObjects[0].Owner.Path = []string{"nested"}
			s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(s.NativeObjects[0])}
		}, `.*inspected subject is outside the requested scope`},
		{"a normalized subject another way", func(_ *protocol.Description, s *protocol.Schema) {
			s.Coverage.Subjects[0].NormalizedSubject.Schema = "elsewhere"
		}, `.*inspected subject is outside the requested scope`},
		{"a facet of a table at the root", func(_ *protocol.Description, s *protocol.Schema) {
			s.NativeObjects = nil
			table := &protocol.Reference{Engine: "fixture", Database: "database", Kind: "table", Name: "root"}
			s.Coverage.Subjects = []*protocol.ModelSubjectCoverage{{Kind: s.Coverage.Kinds[0].Model.Kind, Subject: proto.CloneOf(table), NormalizedSubject: proto.CloneOf(table),
				Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
		}, `.*inspected subject is outside the requested scope`},
		{"a region below the database namespace", func(_ *protocol.Description, s *protocol.Schema) {
			s.Coverage.Kinds[0].Regions[0].Scope.PathPrefix = []*protocol.NamespaceComponent{{Source: "nested", Normalized: "nested"}}
		}, `.*inspected region exceeds the requested scope`},
		{"a region of another schema", func(_ *protocol.Description, s *protocol.Schema) {
			s.Coverage.Kinds[0].Regions[0].Scope.Schema = &protocol.NamespaceComponent{Source: "elsewhere", Normalized: "elsewhere"}
		}, `.*inspected region exceeds the requested scope`},
		{"a claim of global completeness", func(_ *protocol.Description, s *protocol.Schema) {
			s.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
		}, `.*filtered inspection cannot claim global completeness`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, s := databaseInspection()
			test.change(d, s)
			values, err := provider.InspectionValues(d, r, s)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

// A host that does not list the feature receives no database kinds, so a
// filtered read that reports an object at the database is refused with the
// copy both boundaries validate with.
func TestDatabaseKindsNegotiation(t *testing.T) {
	c := qt.New(t)
	d, r, s := databaseInspection()
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.GetEngines()[0].GetDatabaseKinds(), qt.HasLen, 0)
	c.Assert(unlisted.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.DatabaseKindsFeature)
	s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.DatabaseKindsFeature })
	values, err := provider.InspectionValues(unlisted, r, s)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(values, qt.IsNil)
	listed := provider.NegotiatedDescription(d, []string{provider.DatabaseKindsFeature})
	c.Assert(listed.GetEngines()[0].GetDatabaseKinds(), qt.DeepEquals, d.Engines[0].DatabaseKinds)
}

// databaseDescription is a valid description whose engine assesses a planning
// context kind and declares its observed kind a database kind.
func databaseDescription() *protocol.Description {
	d, _, _ := contextPlanning()
	engine := d.Engines[0]
	d.Features = append(d.Features, provider.DatabaseKindsFeature)
	engine.Features = append(engine.Features, provider.DatabaseKindsFeature)
	for _, model := range slices.Clone(engine.ValueModels) {
		if model.Representation == protocol.ValueInput_OBSERVED && slices.Contains(engine.Planning[0].ContextKinds, model.Kind) {
			pool := proto.CloneOf(model)
			pool.Kind = model.Kind + "-pool"
			engine.ValueModels = append(engine.ValueModels, pool)
			engine.DatabaseKinds = []string{pool.Kind}
		}
	}
	return d
}

func TestDatabaseKindsDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	d := databaseDescription()
	c.Assert(d.Engines[0].DatabaseKinds, qt.HasLen, 1)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
}

func TestDatabaseKindsDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.Description)
	}{
		{"kinds without the feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.DatabaseKindsFeature })
		}},
		{"the feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.DatabaseKindsFeature })
		}},
		{"the feature without a kind", func(d *protocol.Description) { d.Engines[0].DatabaseKinds = nil }},
		{"a kind named twice", func(d *protocol.Description) {
			d.Engines[0].DatabaseKinds = append(d.Engines[0].DatabaseKinds, d.Engines[0].DatabaseKinds[0])
		}},
		{"a kind without an observed model", func(d *protocol.Description) { d.Engines[0].DatabaseKinds = []string{"example.org/fixture/unknown"} }},
		{"a planning context kind", func(d *protocol.Description) {
			d.Engines[0].DatabaseKinds = slices.Clone(d.Engines[0].Planning[0].ContextKinds[:1])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := databaseDescription()
			test.change(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*database kinds: .*`)
		})
	}
}
