package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// namespaceDescription is commonDescription with the SCHEMA kind negotiated.
func namespaceDescription() *protocol.Description {
	d := commonDescription()
	d.Features = append(d.Features, provider.CommonNamespacesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonNamespacesFeature)
	d.Engines[0].CommonObjects.Kinds = append(d.Engines[0].CommonObjects.Kinds, protocol.CommonObjectSupport_SCHEMA)
	return d
}

// namespaceInspection is a read that covers every common kind and lists the
// default schema and one more.
func namespaceInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	_, request, snapshot := commonInspection()
	observed := snapshot.CommonObjects
	observed.Coverage = append(observed.Coverage, &protocol.CommonObjectCoverage{Kind: protocol.CommonObjectSupport_SCHEMA, Knowledge: complete()})
	observed.Namespaces = []*protocol.ObservedNamespace{{Identity: commonRef(provider.NamespaceKind, "", "public")},
		{Identity: commonRef(provider.NamespaceKind, "", "app")}}
	snapshot.TargetFeatures = []string{provider.CommonObjectsFeature, provider.CommonNamespacesFeature}
	return namespaceDescription(), request, snapshot
}

// namespaceRender creates one namespace with a comment and drops another.
func namespaceRender() (*protocol.Description, *protocol.RenderRequest, *protocol.RenderResult) {
	request := &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{
		commonChange("create-schema", commonRef(provider.NamespaceKind, "", "app"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_CreateSchema{CreateSchema: &protocol.NamespaceCreation{Name: "app", Comment: "Application"}}}),
		commonChange("drop-schema", commonRef(provider.NamespaceKind, "", "old"), &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_DropSchema{DropSchema: commonRef(provider.NamespaceKind, "", "old")}}),
	}}
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.Changes {
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.Id, Body: &protocol.Unit_Sql{Sql: "STATEMENT " + change.Id + ";"}})
	}
	return namespaceDescription(), request, result
}

func TestCommonNamespaces_HappyPath(t *testing.T) {
	t.Parallel()
	t.Run("a description that declares the kind", func(t *testing.T) {
		c := qt.New(t)
		d := namespaceDescription()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		listed := provider.NegotiatedDescription(d, provider.TwoWayFeatures())
		c.Assert(listed.Engines[0].CommonObjects.GetKinds(), qt.Contains, protocol.CommonObjectSupport_SCHEMA)
		c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)
		c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.CommonNamespacesFeature)
	})
	t.Run("a host that does not list the feature keeps the other kinds", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(namespaceDescription(), withoutFeature(provider.TwoWayFeatures(), provider.CommonNamespacesFeature))
		c.Assert(negotiated.Engines[0].CommonObjects.GetKinds(), qt.DeepEquals, allCommonKinds())
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.CommonNamespacesFeature)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	// An engine whose only kind is SCHEMA carries no common object for such a
	// host, which a description without the wire states.
	t.Run("a host that does not list the feature takes away a kind that stood alone", func(t *testing.T) {
		c := qt.New(t)
		d := namespaceDescription()
		d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_SCHEMA}
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.CommonNamespacesFeature))
		c.Assert(negotiated.Engines[0].CommonObjects, qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.DeepEquals, []string{provider.ValidateSchemaFeature})
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	t.Run("a host that lists the feature without the wire gets neither", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(namespaceDescription(), []string{provider.CommonNamespacesFeature})
		c.Assert(negotiated.Engines[0].CommonObjects, qt.IsNil)
		c.Assert(negotiated.Engines[0].Features, qt.DeepEquals, []string{provider.ValidateSchemaFeature})
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
	t.Run("a read lists the namespaces it covers", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := namespaceInspection()
		before := proto.CloneOf(snapshot)
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
		c.Assert(proto.Equal(before, snapshot), qt.IsTrue)
	})
	t.Run("a read that did not cover the kind lists none", func(t *testing.T) {
		c := qt.New(t)
		d, request, snapshot := namespaceInspection()
		snapshot.CommonObjects.Namespaces = nil
		coverage := snapshot.CommonObjects.Coverage
		coverage[len(coverage)-1].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read was scoped"}
		_, err := provider.InspectionValues(d, request, snapshot)
		c.Assert(err, qt.IsNil)
	})
	t.Run("a plan creates and drops namespaces", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := namespaceRender()
		c.Assert(provider.ValidateRenderedBatch(d, request, result), qt.IsNil)
	})
}

func TestCommonNamespacesDescription_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"the kind without the feature", `.*an engine declares the SCHEMA kind exactly when it lists schema\.common-namespaces`, func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.CommonNamespacesFeature)
		}},
		{"the feature without the kind", `.*an engine declares the SCHEMA kind exactly when it lists schema\.common-namespaces`, func(d *protocol.Description) {
			d.Engines[0].CommonObjects.Kinds = allCommonKinds()
		}},
		{"the engine's feature without the provider's", `.*common namespaces require the provider's feature and the engine's common objects`, func(d *protocol.Description) {
			d.Features = withoutFeature(d.Features, provider.CommonNamespacesFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d := namespaceDescription()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestObservedNamespaces_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Schema)
	}{
		{"a namespace of an uncovered kind", `.*a namespace the read did not cover`, func(s *protocol.Schema) {
			coverage := s.CommonObjects.Coverage
			coverage[len(coverage)-1].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read was scoped"}
		}},
		{"another kind", `.*a namespace needs an identity of kind schema and a name`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces[1].Identity.Kind = "database"
		}},
		{"a schema-qualified name", `.*a namespace needs an identity of kind schema and a name`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces[1].Identity.Schema = "public"
		}},
		{"a path", `.*a namespace needs an identity of kind schema and a name`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces[1].Identity.Path = []string{"nested"}
		}},
		{"no name", `.*a namespace needs an identity of kind schema and a name`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces[1].Identity.Name = " "
		}},
		{"another engine", `.*a namespace needs an identity of kind schema and a name`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces[1].Identity.Engine = "other"
		}},
		// commonIdentifiers fold table names, and a namespace is named by
		// the rules for table names.
		{"a name repeated in another case", `.*a repeated namespace`, func(s *protocol.Schema) {
			s.CommonObjects.Namespaces = append(s.CommonObjects.Namespaces, &protocol.ObservedNamespace{Identity: commonRef(provider.NamespaceKind, "", "APP")})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, snapshot := namespaceInspection()
			d.Engines[0].Identifiers = commonIdentifiers().GetRules()
			test.mutate(snapshot)
			_, err := provider.InspectionValues(d, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// A host that did not list the feature reads a description without the kind,
// so a read that lists namespaces anyway reports a kind it may not cover.
func TestObservedNamespacesWithoutTheFeature_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := namespaceInspection()
	negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.CommonNamespacesFeature))
	snapshot.TargetFeatures = []string{provider.CommonObjectsFeature}
	_, err := provider.InspectionValues(negotiated, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*coverage of an undeclared or repeated kind`)
	snapshot.CommonObjects.Coverage = slices.DeleteFunc(snapshot.CommonObjects.Coverage, func(entry *protocol.CommonObjectCoverage) bool {
		return entry.GetKind() == protocol.CommonObjectSupport_SCHEMA
	})
	_, err = provider.InspectionValues(negotiated, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*a namespace the read did not cover`)
}

func TestRenderNamespaces_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.RenderRequest)
	}{
		{"an engine that does not carry the kind", `.*the engine does not carry schema objects`, func(d *protocol.Description, _ *protocol.RenderRequest) {
			d.Features = withoutFeature(d.Features, provider.CommonNamespacesFeature)
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.CommonNamespacesFeature)
			d.Engines[0].CommonObjects.Kinds = allCommonKinds()
		}},
		{"a creation without a name", `.*a namespace creation needs a name`, func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].GetCommonObject().GetCreateSchema().Name = " "
		}},
		{"a comment on an engine without comments", `.*the engine does not carry comment objects`, func(d *protocol.Description, _ *protocol.RenderRequest) {
			d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_SCHEMA}
		}},
		{"a creation of another namespace than the object", `.*a change's object differs from its subject`, func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[0].Object.Name = "other"
		}},
		{"a removal of a schema-qualified object", `.*a namespace removal needs the namespace`, func(_ *protocol.Description, r *protocol.RenderRequest) {
			removal := r.Changes[1].GetCommonObject().GetDropSchema()
			removal.Schema = "public"
			r.Changes[1].Object.Schema = "public"
		}},
		{"a removal of another kind", `.*a namespace removal needs the namespace`, func(_ *protocol.Description, r *protocol.RenderRequest) {
			r.Changes[1].GetCommonObject().GetDropSchema().Kind = "table"
			r.Changes[1].Object.Kind = "table"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := namespaceRender()
			test.mutate(d, request)
			err := provider.ValidateRenderRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
