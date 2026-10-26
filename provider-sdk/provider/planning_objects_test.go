package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func planningObjectsExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	d.Features = append(d.Features, provider.PlanningObjectContextFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.PlanningObjectContextFeature)
	kind := engine.ValueModels[0].Kind
	engine.Planning[0].ContextKinds = []string{kind}
	request.ContextKinds = []string{kind}
	request.TargetFeatures = append(request.TargetFeatures, provider.PlanningObjectContextFeature)
	request.DesiredObjects = proto.CloneOf(request.Tables[0].Desired.Features.Facets)
	request.CurrentObjects = proto.CloneOf(request.Tables[0].Current.Features.Facets)
	for _, state := range []*protocol.ComparisonState{request.DesiredObjects, request.CurrentObjects} {
		record := state.Records[0]
		record.Subject = &protocol.ComparisonIdentity{Kind: kind, Name: &protocol.ComparisonPart{Source: "query", Normalized: "query"}}
		record.Value.Value.Owner = &protocol.Reference{Engine: "fixture", Kind: kind, Name: "query"}
		record.Value.NormalizedOwner = proto.CloneOf(record.Value.Value.Owner)
		state.Coverage.Subjects = nil
	}
	request.Changes, request.Tables, request.ParentKinds = nil, nil, nil
	result.Changes, result.Parents = nil, nil
	step := result.Contributions[0].Steps[0]
	step.Operation.Role, step.Operation.Parent = protocol.NativeOperation_STATEMENT, nil
	step.Metadata.Effects[0].Subject = proto.CloneOf(request.DesiredObjects.Records[0].Subject)
	result.Objects = []*protocol.PlanningReceipt{{Kind: kind, Subject: proto.CloneOf(request.DesiredObjects.Records[0].Subject),
		Strategy: "Restart unchanged dependent around its input mutation", Steps: []*protocol.StepIdentity{proto.CloneOf(step.Id)}}}
	return d, request, result
}

func TestPlanningObjectContextAccountsUnchangedObjectOperations(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := planningObjectsExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	before := proto.CloneOf(request)
	inputs := provider.PlanningObjectSubjects(request)
	c.Assert(inputs, qt.HasLen, 1)
	inputs[0].Name.Source = "mutated"
	c.Assert(proto.Equal(request, before), qt.IsTrue)
	result.Contributions, result.Objects[0].Steps = nil, nil
	result.Objects[0].Strategy = "No relevant input mutation"
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
}

func TestPlanningObjectContextRequiresExactRequestAndReceiptInventory(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures)
	}{
		{"host opt out", func(d *protocol.Description, _ *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.PlanningObjectContextFeature })
		}},
		{"request opt out", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.TargetFeatures = []string{"retention"}
		}},
		{"narrowed assignment", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.ContextKinds = nil
		}},
		{"missing capture", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects = nil
		}},
		{"missing coverage", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Coverage = nil
		}},
		{"wrong representation", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Representation = protocol.ValueInput_DESIRED
		}},
		{"foreign model", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Records[0].Kind = "foreign"
		}},
		{"foreign identity", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Records[0].Subject.Name.Source = "foreign"
		}},
		{"facet as object", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Records[0].Subject.Kind = "table"
		}},
		{"pending dependencies", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Records[0].Value.RequiresDatabaseContext = true
		}},
		{"duplicate object", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.CurrentObjects.Records = append(r.CurrentObjects.Records, proto.CloneOf(r.CurrentObjects.Records[0]))
		}},
		{"foreign changed subject", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.ChangedSubjects = []*protocol.ComparisonIdentity{proto.CloneOf(r.CurrentObjects.Records[0].Subject)}
			r.ChangedSubjects[0].Name.Source, r.ChangedSubjects[0].Name.Normalized = "foreign", "foreign"
		}},
		{"duplicate changed subject", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest, _ *protocol.PlannedFeatures) {
			r.ChangedSubjects = []*protocol.ComparisonIdentity{proto.CloneOf(r.CurrentObjects.Records[0].Subject), proto.CloneOf(r.CurrentObjects.Records[0].Subject)}
		}},
		{"missing receipt", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects = nil
		}},
		{"duplicate receipt", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects = append(r.Objects, proto.CloneOf(r.Objects[0]))
		}},
		{"foreign receipt", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects[0].Subject.Name.Source = "foreign"
		}},
		{"no strategy", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects[0].Strategy = ""
		}},
		{"foreign step", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects[0].Steps[0].Name = "foreign"
		}},
		{"unaccounted operation", func(_ *protocol.Description, _ *protocol.PlanFeaturesRequest, r *protocol.PlannedFeatures) {
			r.Objects[0].Steps = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := planningObjectsExchange()
			c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
			test.mutate(d, request, result)
			c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPlanningObjectContextRetainsUnknownSubjectsAndKindKnowledge(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := planningObjectsExchange()
	unknown := &protocol.ComparisonIdentity{Kind: request.ContextKinds[0], Name: &protocol.ComparisonPart{Source: "unknown", Normalized: "unknown"}}
	request.CurrentObjects.Coverage.Subjects = []*protocol.ComparisonSubjectCoverage{{Kind: unknown.Kind, Subject: unknown,
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "Permission denied"}}}
	result.Objects = append(result.Objects, &protocol.PlanningReceipt{Kind: unknown.Kind, Subject: proto.CloneOf(unknown), Strategy: "No affected dependencies"})
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	request.ChangedSubjects = []*protocol.ComparisonIdentity{proto.CloneOf(unknown), proto.CloneOf(request.DesiredObjects.Records[0].Subject)}
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
	slices.SortFunc(request.ChangedSubjects, provider.CompareIdentities)
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	slices.Reverse(result.Objects)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	result = &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE,
		Kind: unknown.Kind, Message: "Cannot establish dependent state", Input: &protocol.PlanningDiagnostic_ContextObject{ContextObject: 1}}}}
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	result.Diagnostics[0].Input = &protocol.PlanningDiagnostic_ContextObject{ContextObject: 2}
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
	request.DesiredObjects.Records, request.CurrentObjects.Records, request.CurrentObjects.Coverage.Subjects, request.ChangedSubjects = nil, nil, nil, nil
	request.CurrentObjects.Coverage.Kinds[0].Sources = []*protocol.ComparisonKnowledgeSource{{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "No visibility"}}}
	result.Diagnostics[0].Input = nil
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
}

func TestPlanningObjectContextLocalizesOnlyTheEstablishedDatabase(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := planningObjectsExchange()
	current := request.CurrentObjects.Records[0].Value
	current.Value.Owner.Database, current.NormalizedOwner.Database = "/db", "/db"
	request.DatabasePath = "/db"
	d.Features = append(d.Features, provider.ValueContextFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ValueContextFeature)
	request.TargetFeatures = append(request.TargetFeatures, provider.ValueContextFeature)
	current.Value.Context = &protocol.DatabaseContext{Database: "/db", SourceDatabase: "/authored"}
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	for _, mutate := range []func(*protocol.PlanFeaturesRequest){
		func(r *protocol.PlanFeaturesRequest) {
			r.CurrentObjects.Records[0].Value.Value.Context.Database = "/foreign"
		},
		func(r *protocol.PlanFeaturesRequest) { r.DatabasePath = "" },
		func(r *protocol.PlanFeaturesRequest) {
			r.CurrentObjects.Records[0].Value.Value.Owner.Database = "/foreign"
		},
		func(r *protocol.PlanFeaturesRequest) {
			r.CurrentObjects.Records[0].Value.NormalizedOwner.Database = "/foreign"
		},
	} {
		changed := proto.CloneOf(request)
		mutate(changed)
		c.Assert(provider.ValidatePlanningRequest(d, changed), qt.ErrorIs, provider.ErrProtocol)
	}
}

func TestPlanningObjectAssignmentIsExclusiveAndNegotiated(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, _, _ := planningObjectsExchange()
	service := d.Engines[0].Planning[0]
	service.ChangeKinds, service.ParentKinds = nil, nil
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	without := provider.NegotiatedDescription(d, nil)
	c.Assert(provider.ValidateDescription(without, nil), qt.IsNil)
	c.Assert(without.Engines[0].Planning, qt.HasLen, 0)
	other := proto.CloneOf(service)
	other.Id = "other"
	d.Engines[0].Planning = append(d.Engines[0].Planning, other)
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
}
