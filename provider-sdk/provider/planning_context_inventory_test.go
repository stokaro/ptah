package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// contextInspection is a filtered read whose engine assesses the native
// object's kind as planning context, with one object of that kind outside
// the requested scope in its planning context.
func contextInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, r, s := filteredInspection()
	engine := d.Engines[0]
	kind := s.NativeObjects[0].Kind
	for _, feature := range []string{provider.PlanningObjectContextFeature, provider.PlanningContextInventoryFeature} {
		d.Features = append(d.Features, feature)
		engine.Features = append(engine.Features, feature)
		s.TargetFeatures = append(s.TargetFeatures, feature)
	}
	engine.Planning = append(engine.Planning, &protocol.ModelPlanning{Id: "assess", ContextKinds: []string{kind}})
	outside := proto.CloneOf(s.NativeObjects[0])
	outside.Owner.Path = []string{"elsewhere"}
	s.PlanningContext = &protocol.PlanningContextInventory{Objects: []*protocol.Envelope{outside}, Coverage: &protocol.ModelCoverage{
		Representation: protocol.ValueInput_OBSERVED,
		Kinds:          []*protocol.ModelKindCoverage{{Model: proto.CloneOf(s.Coverage.Kinds[0].Model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		Subjects:       []*protocol.ModelSubjectCoverage{observedSubject(outside)},
	}}
	return d, r, s
}

func TestPlanningContextInventory_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, r, s := contextInspection()
	_, err := provider.InspectionValues(d, r, s)
	c.Assert(err, qt.IsNil)
	values, err := provider.PlanningContextValues(d, r, s)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 1)
	c.Assert(proto.Equal(values.Values[0].Value, s.PlanningContext.Objects[0]), qt.IsTrue)
	c.Assert(values.Values[0].Representation, qt.Equals, protocol.ValueInput_OBSERVED)
	// A read without a planning context gives an empty batch.
	s.PlanningContext = nil
	values, err = provider.PlanningContextValues(d, r, s)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 0)
}

func TestPlanningContextInventory_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.InspectRequest, *protocol.Schema)
	}{
		{"not negotiated", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.TargetFeatures = slices.DeleteFunc(s.TargetFeatures, func(f string) bool { return f == provider.PlanningContextInventoryFeature })
		}},
		{"unfiltered read", func(_ *protocol.Description, r *protocol.InspectRequest, s *protocol.Schema) {
			r.Schemas, s.ScopeBindings = nil, nil
		}},
		{"an object inside the scope", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.PlanningContext.Objects[0] = proto.CloneOf(s.NativeObjects[0])
			s.PlanningContext.Coverage.Subjects = []*protocol.ModelSubjectCoverage{observedSubject(s.NativeObjects[0])}
		}},
		{"an object without coverage", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.PlanningContext.Coverage.Subjects = nil
		}},
		{"a kind no service assesses", func(d *protocol.Description, _ *protocol.InspectRequest, _ *protocol.Schema) {
			d.Engines[0].Planning = nil
		}},
		{"desired coverage", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.PlanningContext.Coverage.Representation = protocol.ValueInput_DESIRED
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, s := contextInspection()
			test.mutate(d, r, s)
			_, err := provider.InspectionValues(d, r, s)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			_, err = provider.PlanningContextValues(d, r, s)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// contextPlanning is a planning exchange whose request carries one object of
// the assessed kind outside the scope as its context inventory. The service
// returns no receipt for it.
func contextPlanning() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningObjectsExchange()
	d.Features = append(d.Features, provider.PlanningContextInventoryFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningContextInventoryFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.PlanningContextInventoryFeature)
	request.ContextInventory = proto.CloneOf(request.CurrentObjects)
	record := request.ContextInventory.Records[0]
	record.Subject.Name = &protocol.ComparisonPart{Source: "outside", Normalized: "outside"}
	record.Value.Value.Owner.Name, record.Value.NormalizedOwner.Name = "outside", "outside"
	return d, request, result
}

func TestPlanningContextInventoryRequest_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := contextPlanning()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	before := proto.CloneOf(request)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, before), qt.IsTrue)
	// The context is not part of the assessment inventory.
	c.Assert(provider.PlanningObjectSubjects(request), qt.HasLen, 1)
}

func TestPlanningContextInventoryRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
	}{
		{"not negotiated", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.TargetFeatures = slices.DeleteFunc(r.TargetFeatures, func(f string) bool { return f == provider.PlanningContextInventoryFeature })
		}},
		{"a captured object again", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.ContextInventory = proto.CloneOf(r.CurrentObjects)
		}},
		{"desired values", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.ContextInventory = proto.CloneOf(r.DesiredObjects)
			record := r.ContextInventory.Records[0]
			record.Subject.Name = &protocol.ComparisonPart{Source: "outside", Normalized: "outside"}
			record.Value.Value.Owner.Name, record.Value.NormalizedOwner.Name = "outside", "outside"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := contextPlanning()
			test.mutate(d, request)
			c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPlanningContextInventoryRequiresObjectContext(t *testing.T) {
	c := qt.New(t)
	d, _, _ := contextPlanning()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.PlanningObjectContextFeature })
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
}
