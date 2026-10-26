package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func viewFacetExchange() (*protocol.Description, *protocol.PrepareCommonObjectsRequest) {
	d, comparison, _ := comparisonExchange()
	d.Features = append(d.Features, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature, provider.RenderFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_VIEW}}
	record := comparison.Desired.Records[0]
	record.Subject = &protocol.ComparisonIdentity{Kind: "view", Schema: &protocol.ComparisonPart{Source: "app", Normalized: "app"}, Name: &protocol.ComparisonPart{Source: "active", Normalized: "active"}}
	record.Value.Value.Owner = commonRef("view", "app", "active")
	record.Value.NormalizedOwner = proto.CloneOf(record.Value.Value.Owner)
	objects := proto.CloneOf(comparison.Desired)
	objects.Records = nil
	view := &protocol.SQLView{Schema: "app", Name: "active", Body: "SELECT 1", Features: &protocol.CapturedFeatures{Objects: objects, Facets: comparison.Desired}}
	return d, &protocol.PrepareCommonObjectsRequest{Engine: "fixture", TargetFeatures: slices.Clone(engine.Features), Identifiers: comparison.Identifiers,
		Declared: &protocol.DeclaredCommonObjects{Views: []*protocol.SQLView{view}}}
}

func TestCommonViewFacetsPreservePreparedOwnershipAndCoverage(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := viewFacetExchange()
	c.Assert(provider.ValidatePrepareCommonObjectsRequest(d, request), qt.IsNil)
	result := &protocol.PreparedCommonObjects{Complete: true, Prepared: proto.CloneOf(request.Declared)}
	result.Prepared.Views[0].Body = "SELECT 1 AS value"
	c.Assert(provider.ValidatePreparedCommonObjects(d, request, result), qt.IsNil)
	result.Prepared.Views[0].Features = nil
	c.Assert(provider.ValidatePreparedCommonObjects(d, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestCommonViewFacetsRefuseUnboundDesiredCarriers(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PrepareCommonObjectsRequest)
	}{
		{"unnegotiated", func(d *protocol.Description, _ *protocol.PrepareCommonObjectsRequest) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.CommonViewFacetsFeature })
		}},
		{"host opt out", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.TargetFeatures = []string{"retention", provider.CommonObjectsFeature}
		}},
		{"another view", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Name = "other"
		}},
		{"another kind", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Materialized = true
		}},
		{"foreign normalized owner", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Features.Facets.Records[0].Value.NormalizedOwner.Name = "other"
		}},
		{"foreign source owner", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Features.Facets.Records[0].Value.Value.Owner.Schema = "other"
		}},
		{"foreign source database", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Features.Facets.Records[0].Value.Value.Owner.Database = "/other"
		}},
		{"standalone twin", func(_ *protocol.Description, r *protocol.PrepareCommonObjectsRequest) {
			r.Declared.Views[0].Features.Objects.Records = r.Declared.Views[0].Features.Facets.Records
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := viewFacetExchange()
			c.Assert(provider.ValidatePrepareCommonObjectsRequest(d, request), qt.IsNil)
			test.mutate(d, request)
			c.Assert(provider.ValidatePrepareCommonObjectsRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func viewFacetInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, prepared := viewFacetExchange()
	facet := proto.CloneOf(prepared.Declared.Views[0].Features.Facets.Records[0].Value.Value)
	facet.Owner.Database = "/db"
	observed := &protocol.ObservedView{Identity: proto.CloneOf(facet.Owner), Body: "SELECT 1", Facets: []*protocol.Envelope{facet}}
	model := proto.CloneOf(d.Engines[0].ValueModels[1])
	snapshot := &protocol.Schema{Complete: true, Engine: "fixture", TargetFeatures: prepared.TargetFeatures,
		Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED,
			Kinds: []*protocol.ModelKindCoverage{{Model: model, Knowledge: complete()}}, Subjects: []*protocol.ModelSubjectCoverage{observedSubject(facet)}},
		CommonObjects: &protocol.ObservedCommonObjects{Views: []*protocol.ObservedView{observed}, Coverage: []*protocol.CommonObjectCoverage{{Kind: protocol.CommonObjectSupport_VIEW, Knowledge: complete()}}}}
	return d, &protocol.InspectRequest{SessionId: "session"}, snapshot
}

func TestInspectedViewFacetsEnterTheNormalizationInventory(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, snapshot := viewFacetInspection()
	batch, err := provider.InspectionValues(d, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(batch.Values, qt.HasLen, 1)
	c.Assert(batch.Values[0].Value.Owner.Kind, qt.Equals, "view")
	c.Assert(batch.Values[0].Value.Owner.Database, qt.Equals, "/db")
	batch.Values[0].Value.Payload[0] = 'x'
	c.Assert(snapshot.CommonObjects.Views[0].Facets[0].Payload[0], qt.Equals, byte('{'))
	for _, mutate := range []func(*protocol.Schema){
		func(s *protocol.Schema) { s.CommonObjects.Views[0].Facets[0].Owner.Name = "other" },
		func(s *protocol.Schema) { s.CommonObjects.Views[0].Facets[0].Owner.Database = "/other" },
		func(s *protocol.Schema) {
			s.CommonObjects.Views[0].Facets = append(s.CommonObjects.Views[0].Facets, proto.CloneOf(s.CommonObjects.Views[0].Facets[0]))
		},
		func(s *protocol.Schema) { s.Coverage.Subjects = nil },
	} {
		changed := proto.CloneOf(snapshot)
		mutate(changed)
		_, err := provider.InspectionValues(d, request, changed)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	}
}

func TestViewReplacementRetainsTheActualCurrentOperand(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, prepared := viewFacetExchange()
	_, _, snapshot := viewFacetInspection()
	current := snapshot.CommonObjects.Views[0]
	creation := &protocol.ViewCreation{View: prepared.Declared.Views[0], Replace: true, Current: current}
	request := &protocol.RenderRequest{Engine: "fixture", TargetFeatures: prepared.TargetFeatures,
		Changes: []*protocol.Change{{Id: "replace", Object: proto.CloneOf(current.Identity), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
			Operation: &protocol.CommonObjectChange_CreateView{CreateView: creation}}}}}}
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
	for _, mutate := range []func(*protocol.ViewCreation){
		func(v *protocol.ViewCreation) { v.Replace = false },
		func(v *protocol.ViewCreation) { v.Current.Identity.Database = "/other" },
		func(v *protocol.ViewCreation) { v.Current.Identity.Catalog = "other" },
		func(v *protocol.ViewCreation) { v.Current.Identity.Path = []string{"other"} },
		func(v *protocol.ViewCreation) { v.Current.Identity.Overload = []string{"other"} },
		func(v *protocol.ViewCreation) { v.Current.Identity.Name = "other" },
	} {
		changed := proto.CloneOf(request)
		mutate(changed.Changes[0].GetCommonObject().GetCreateView())
		c.Assert(provider.ValidateRenderRequest(d, changed), qt.ErrorIs, provider.ErrProtocol)
	}
	creation.Current = nil
	request.Changes[0].Object.Database = ""
	c.Assert(provider.ValidateRenderRequest(d, request), qt.IsNil)
}

func TestPlannedViewReplacementUsesCapturedObservation(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, _ := planningExchange()
	viewDescription, prepared := viewFacetExchange()
	d.Features = append(d.Features, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature)
	d.Engines[0].CommonObjects = proto.CloneOf(viewDescription.Engines[0].CommonObjects)
	request.TargetFeatures = append(request.TargetFeatures, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature)
	_, _, snapshot := viewFacetInspection()
	request.DesiredCommon = proto.CloneOf(prepared.Declared)
	request.CurrentCommon = proto.CloneOf(snapshot.CommonObjects)
	creation := &protocol.ViewCreation{View: proto.CloneOf(prepared.Declared.Views[0]), Replace: true, Current: proto.CloneOf(snapshot.CommonObjects.Views[0])}
	request.CommonSteps[0].CommonObject = &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateView{CreateView: creation}}
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	creation.Current.Body = "SELECT 2"
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
	creation.Current = proto.CloneOf(snapshot.CommonObjects.Views[0])
	request.CurrentCommon.Views = nil
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
}
