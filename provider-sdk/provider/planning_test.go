package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func planningExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, comparison, compared := comparisonExchange()
	d.Features = append(d.Features, provider.PlanFeaturesFeature)
	engine := d.Engines[0]
	model := proto.CloneOf(engine.ValueModels[2])
	model.Kind = "example.org/fixture/retention-operation"
	model.Representation = protocol.ValueInput_OPERATION
	engine.ValueModels = append(engine.ValueModels, model)
	service := &protocol.ModelPlanning{Id: "retention", ChangeKinds: []string{engine.ValueModels[2].Kind}, ParentKinds: []string{engine.ValueModels[0].Kind}, OperationKinds: []string{model.Kind}}
	engine.Planning = []*protocol.ModelPlanning{service}
	subject := proto.CloneOf(comparison.Parents[0].Subject)
	empty := func(representation protocol.ValueInput_Representation) *protocol.ComparisonState {
		return &protocol.ComparisonState{Representation: representation, Coverage: &protocol.ComparisonCoverage{}}
	}
	request := &protocol.PlanFeaturesRequest{Engine: comparison.Engine, PlanningId: service.Id, TargetFeatures: comparison.TargetFeatures, Identifiers: comparison.Identifiers,
		Capabilities: map[string]bool{"transactional_ddl": false}, ParentKinds: service.ParentKinds,
		Changes: []*protocol.PlanningChange{{Subject: subject, Value: proto.CloneOf(compared.Changes[0].Value)}},
		Tables: []*protocol.PlanningTable{{Subject: subject, Action: protocol.PlanningTable_ALTER,
			Desired: &protocol.DeclaredTable{Name: "C", Schema: "a.b", Columns: []*protocol.DeclaredColumn{{Name: "id", Type: "INTEGER"}}, Features: &protocol.CapturedFeatures{Objects: empty(protocol.ValueInput_DESIRED), Facets: comparison.Desired}},
			Current: &protocol.ObservedTable{Name: "C", Schema: "a.b", Features: &protocol.CapturedFeatures{Objects: empty(protocol.ValueInput_OBSERVED), Facets: comparison.Current}}}},
		CommonSteps: []*protocol.CommonPlanningStep{{Id: &protocol.StepIdentity{Owner: "host", Name: "common"}, Metadata: &protocol.PlanningMetadata{Impact: protocol.ChangeSemantics_UNKNOWN}}},
	}
	value := proto.CloneOf(compared.Changes[0].Value)
	value.Id = "apply"
	value.Value.Kind = model.Kind
	value.ChangeSemantics = nil
	value.OperationSemantics = &protocol.OperationSemantics{Impact: protocol.ChangeSemantics_BEHAVIORAL, Reason: "Retention changes", Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "No access change"}
	id := &protocol.StepIdentity{Owner: d.ProviderId, Name: "apply"}
	step := &protocol.PlanningStep{Id: id, Metadata: &protocol.PlanningMetadata{Impact: protocol.ChangeSemantics_BEHAVIORAL, Reason: "Retention changes", Transaction: protocol.PlanningMetadata_FORBIDDEN,
		Effects: []*protocol.PlanningEffect{{Subject: subject, Action: protocol.PlanningEffect_ALTER}}}, Operation: &protocol.PlanningOperation{Role: protocol.NativeOperation_ALTER_TABLE, Parent: subject, Value: value}}
	result := &protocol.PlannedFeatures{Complete: true, Contributions: []*protocol.PlanningContribution{{Owner: d.ProviderId, Steps: []*protocol.PlanningStep{step},
		Dependencies: []*protocol.PlanningDependency{{Before: request.CommonSteps[0].Id, After: id}}}},
		Changes: []*protocol.PlanningReceipt{{Subject: subject, Kind: service.ChangeKinds[0], Strategy: "Apply changed settings", Steps: []*protocol.StepIdentity{id}}},
		Parents: []*protocol.PlanningParentReceipt{{Action: protocol.PlanningTable_ALTER, Receipt: &protocol.PlanningReceipt{Subject: subject, Kind: service.ParentKinds[0], Strategy: "Preserve the remaining settings", Steps: []*protocol.StepIdentity{id}}}},
	}
	return d, request, result
}

func TestPlanningPreservesCapturedKnowledgeAndExecutionRequirements(t *testing.T) {
	c := qt.New(t)
	d, request, result := planningExchange()
	unknown := &protocol.ComparisonKnowledgeSource{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "Second source did not inspect settings"}}
	request.Tables[0].Current.Features.Facets.Coverage.Kinds[0].Sources = append(request.Tables[0].Current.Features.Facets.Coverage.Kinds[0].Sources, unknown)
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(result.Contributions[0].Steps[0].Metadata.Transaction, qt.Equals, protocol.PlanningMetadata_FORBIDDEN)
	value, found := request.Capabilities["transactional_ddl"]
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.IsFalse)
}

func TestPlanningRefusesIncompleteAndUnownedContext(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) { d.Features = nil }},
		{"foreign service", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.PlanningId = "foreign" }},
		{"foreign target", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Engine = "foreign" }},
		{"narrowed parent assignment", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.ParentKinds = nil }},
		{"unowned change", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Changes[0].Value.Value.Kind = "foreign"
		}},
		{"duplicate change", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Changes = append(r.Changes, proto.CloneOf(r.Changes[0]))
		}},
		{"missing identifiers", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Identifiers = nil }},
		{"duplicate parent", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
		}},
		{"missing capture", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Tables[0].Current = nil }},
		{"missing knowledge", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Tables[0].Current.Features = nil }},
		{"missing coverage", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Current.Features.Facets.Coverage = nil
		}},
		{"unknown action", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Tables[0].Action = 99 }},
		{"invalid declared operand", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Desired.Columns[0].Type = ""
		}},
		{"duplicate common step", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.CommonSteps = append(r.CommonSteps, proto.CloneOf(r.CommonSteps[0]))
		}},
		{"unknown transaction", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.CommonSteps[0].Metadata.Transaction = 99
		}},
		{"duplicate effect", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			effect := &protocol.PlanningEffect{Subject: r.Tables[0].Subject, Action: protocol.PlanningEffect_READ}
			r.CommonSteps[0].Metadata.Effects = []*protocol.PlanningEffect{effect, effect}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, _ := planningExchange()
			c.Assert(provider.ValidatePlanningRequest(d, r), qt.IsNil)
			test.mutate(d, r)
			c.Assert(provider.ValidatePlanningRequest(d, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPlanningRefusesPartialOrUnaccountedReplies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.PlannedFeatures)
	}{
		{"incomplete", func(r *protocol.PlannedFeatures) { r.Complete = false }},
		{"missing change receipt", func(r *protocol.PlannedFeatures) { r.Changes = nil }},
		{"missing parent receipt", func(r *protocol.PlannedFeatures) { r.Parents = nil }},
		{"extra parent receipt", func(r *protocol.PlannedFeatures) { r.Parents = append(r.Parents, proto.CloneOf(r.Parents[0])) }},
		{"changed action", func(r *protocol.PlannedFeatures) { r.Parents[0].Action = protocol.PlanningTable_DROP }},
		{"changed subject spelling", func(r *protocol.PlannedFeatures) {
			r.Changes[0] = proto.CloneOf(r.Changes[0])
			r.Changes[0].Subject.Name.Source = "changed"
		}},
		{"missing strategy", func(r *protocol.PlannedFeatures) { r.Changes[0].Strategy = "" }},
		{"unknown receipt step", func(r *protocol.PlannedFeatures) {
			r.Changes[0].Steps = []*protocol.StepIdentity{{Owner: "foreign", Name: "x"}}
		}},
		{"unaccounted operation", func(r *protocol.PlannedFeatures) { r.Changes[0].Steps = nil; r.Parents[0].Receipt.Steps = nil }},
		{"foreign contribution", func(r *protocol.PlannedFeatures) { r.Contributions[0].Owner = "foreign" }},
		{"duplicate operation", func(r *protocol.PlannedFeatures) {
			r.Contributions[0].Steps = append(r.Contributions[0].Steps, proto.CloneOf(r.Contributions[0].Steps[0]))
		}},
		{"foreign model", func(r *protocol.PlannedFeatures) { r.Contributions[0].Steps[0].Operation.Value.Value.Kind = "foreign" }},
		{"missing operation facts", func(r *protocol.PlannedFeatures) {
			r.Contributions[0].Steps[0].Operation.Value.OperationSemantics = nil
		}},
		{"unknown transaction", func(r *protocol.PlannedFeatures) { r.Contributions[0].Steps[0].Metadata.Transaction = 99 }},
		{"unknown phase", func(r *protocol.PlannedFeatures) { r.Contributions[0].Steps[0].Operation.Phase = 99 }},
		{"unknown parent", func(r *protocol.PlannedFeatures) {
			r.Contributions[0].Steps[0].Operation.Parent = &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "other", Normalized: "other"}}
		}},
		{"self dependency", func(r *protocol.PlannedFeatures) {
			r.Contributions[0].Dependencies[0].Before = r.Contributions[0].Dependencies[0].After
		}},
		{"refused with output", func(r *protocol.PlannedFeatures) {
			r.Diagnostics = []*protocol.PlanningDiagnostic{{Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: "feature", Message: "Cannot preserve state"}}
		}},
		{"rewrite unknown footprint", func(r *protocol.PlannedFeatures) {
			r.Rewrites = []*protocol.PlanningRewrite{{Sources: []*protocol.StepIdentity{{Owner: "host", Name: "common"}}, Replacement: r.Contributions[0].Steps[0].Id}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, q, r := planningExchange()
			c.Assert(provider.ValidatePlannedFeatures(d, q, r), qt.IsNil)
			test.mutate(r)
			c.Assert(provider.ValidatePlannedFeatures(d, q, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPlanningKeepsCompletedRefusalsSeparateFromFailures(t *testing.T) {
	c := qt.New(t)
	d, q, _ := planningExchange()
	diagnostic := &protocol.PlanningDiagnostic{Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE, Kind: q.ParentKinds[0], Message: "Unknown attached state", Input: &protocol.PlanningDiagnostic_Parent{Parent: 0}}
	refusal := &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{diagnostic}}
	c.Assert(provider.ValidatePlannedFeatures(d, q, refusal), qt.IsNil)
	diagnostic.Input = &protocol.PlanningDiagnostic_Parent{Parent: 1}
	c.Assert(provider.ValidatePlannedFeatures(d, q, refusal), qt.ErrorIs, provider.ErrProtocol)
	diagnostic.Input = &protocol.PlanningDiagnostic_Change{Change: 0}
	c.Assert(provider.ValidatePlannedFeatures(d, q, refusal), qt.ErrorIs, provider.ErrProtocol)
	diagnostic.Kind = q.Changes[0].Value.Value.Kind
	c.Assert(provider.ValidatePlannedFeatures(d, q, refusal), qt.IsNil)
}

func TestPlanningAcceptsExplicitRewritesButRefusesDuplicateClaims(t *testing.T) {
	c := qt.New(t)
	d, request, result := planningExchange()
	step := result.Contributions[0].Steps[0]
	request.CommonSteps[0].Metadata.Effects = []*protocol.PlanningEffect{{Subject: proto.CloneOf(request.Tables[0].Subject), Action: protocol.PlanningEffect_ALTER}}
	result.Rewrites = []*protocol.PlanningRewrite{{Sources: []*protocol.StepIdentity{proto.CloneOf(request.CommonSteps[0].Id)}, Replacement: proto.CloneOf(step.Id)}}
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	result.Rewrites = append(result.Rewrites, proto.CloneOf(result.Rewrites[0]))
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestPlanningDescriptionRequiresExclusiveModelAssignments(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"missing operation vocabulary", func(d *protocol.Description) { d.Engines[0].Planning[0].OperationKinds = nil }},
		{"duplicate service", func(d *protocol.Description) {
			d.Engines[0].Planning = append(d.Engines[0].Planning, proto.CloneOf(d.Engines[0].Planning[0]))
		}},
		{"competing assignment", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0].Planning[0])
			other.Id = "second"
			d.Engines[0].Planning = append(d.Engines[0].Planning, other)
		}},
		{"foreign operation model", func(d *protocol.Description) { d.Engines[0].Planning[0].OperationKinds = []string{"foreign"} }},
		{"missing observed model", func(d *protocol.Description) {
			d.Engines[0].ValueModels[1].Representation = protocol.ValueInput_OPERATION
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := planningExchange()
			c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func plannedColumnRequest() (*protocol.Description, *protocol.PlanFeaturesRequest) {
	d, request, _ := planningExchange()
	step := request.CommonSteps[0]
	step.Parent = proto.CloneOf(request.Tables[0].Subject)
	step.AddedColumn = &protocol.PlanningColumn{Name: "owner_id", Type: "INTEGER", EnumType: true,
		Default: &protocol.PlanningDefault{ValueSet: true},
		ForeignKey: &protocol.PlanningForeignKey{Table: "owners", Columns: []string{"id"}, Deferrable: true,
			Facets: []*protocol.CapturedFacet{{Kind: request.ParentKinds[0], Targets: []string{"other"}}}},
	}
	return d, request
}

func TestPlanningColumnWireRetainsPresenceAndNestedFacets(t *testing.T) {
	c := qt.New(t)
	d, request := plannedColumnRequest()
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	data, err := proto.Marshal(request)
	c.Assert(err, qt.IsNil)
	roundTrip := new(protocol.PlanFeaturesRequest)
	c.Assert(proto.Unmarshal(data, roundTrip), qt.IsNil)
	c.Assert(proto.Equal(roundTrip, request), qt.IsTrue)
	column := roundTrip.CommonSteps[0].AddedColumn
	c.Assert(column.Default, qt.Not(qt.IsNil))
	c.Assert(column.Default.ValueSet, qt.IsTrue)
	c.Assert(column.Default.Value, qt.Equals, "")
	c.Assert(column.EnumType, qt.IsTrue)
	c.Assert(column.ForeignKey.Facets[0].Targets, qt.DeepEquals, []string{"other"})
	c.Assert(column.ForeignKey.Facets[0].Value, qt.IsNil)
	c.Assert(provider.ValidatePlanningRequest(d, roundTrip), qt.IsNil)
}

func TestPlanningColumnRefusesMissingContextAndFacetScope(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.CommonPlanningStep)
	}{
		{"missing parent", func(s *protocol.CommonPlanningStep) { s.Parent = nil }},
		{"unknown parent", func(s *protocol.CommonPlanningStep) { s.Parent.Name.Normalized = "unknown" }},
		{"missing type", func(s *protocol.CommonPlanningStep) { s.AddedColumn.Type = "" }},
		{"missing referenced table", func(s *protocol.CommonPlanningStep) { s.AddedColumn.ForeignKey.Table = "" }},
		{"missing referenced columns", func(s *protocol.CommonPlanningStep) { s.AddedColumn.ForeignKey.Columns = nil }},
		{"unscoped exclusion", func(s *protocol.CommonPlanningStep) { s.AddedColumn.ForeignKey.Facets[0].Targets = nil }},
		{"unowned facet", func(s *protocol.CommonPlanningStep) { s.AddedColumn.ForeignKey.Facets[0].Kind = "foreign" }},
		{"duplicate facet", func(s *protocol.CommonPlanningStep) {
			f := s.AddedColumn.ForeignKey.Facets[0]
			s.AddedColumn.ForeignKey.Facets = append(s.AddedColumn.ForeignKey.Facets, proto.CloneOf(f))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := plannedColumnRequest()
			c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
			test.mutate(request.CommonSteps[0])
			c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
