package provider_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func indexIdentity(name string) *protocol.ComparisonIdentity {
	return &protocol.ComparisonIdentity{Kind: "index", Parent: &protocol.ComparisonPart{Source: "C", Normalized: "C"}, Name: &protocol.ComparisonPart{Source: name, Normalized: name}}
}

func planningPolicy() *protocol.PlanningPolicy {
	return &protocol.PlanningPolicy{AllowTableRebuild: true, TableRebuildRequest: "--allow-table-rebuild", OnlineAlter: true,
		ConcurrentIndexBuildSubjects: []*protocol.ComparisonIdentity{indexIdentity("by_name")},
		ConcurrentIndexDropSubjects:  []*protocol.ComparisonIdentity{indexIdentity("by_age")}, OmitNullBackfill: true}
}

// policyExchange is a planning exchange whose provider and engine honor the
// planning policy.
func policyExchange() (*protocol.Description, *protocol.PlanFeaturesRequest) {
	d, request, _ := planningExchange()
	d.Features = append(d.Features, provider.PlanningPolicyFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningPolicyFeature)
	request.Policy = planningPolicy()
	return d, request
}

func TestPlanningPolicy_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := policyExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	request.Policy = &protocol.PlanningPolicy{TableRebuildRequest: "PTAH_ALLOW_TABLE_REBUILD=1"}
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	request.Policy = nil
	c.Assert(provider.ValidatePlanningRequest(provider.NegotiatedDescription(d, nil), request), qt.IsNil)
}

func TestPlanningPolicy_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
	}{
		{"the provider does not list the feature", func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{"the engine does not list the feature", func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
		}},
		{"no words for a rebuild request", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Policy.TableRebuildRequest = "" }},
		{"two lines for a rebuild request", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Policy.TableRebuildRequest = "--allow-table-rebuild\n--force"
		}},
		{"a build subject that is no index", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Policy.ConcurrentIndexBuildSubjects[0].Kind = "table"
		}},
		{"a build subject named twice", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Policy.ConcurrentIndexBuildSubjects = append(r.Policy.ConcurrentIndexBuildSubjects, indexIdentity("by_name"))
		}},
		{"a drop subject without a name", func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Policy.ConcurrentIndexDropSubjects[0].Name = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := policyExchange()
			test.mutate(d, request)
			c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestPlanningStatementStyle_ExactNeedsNegotiation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.PlanFeaturesRequest)
		valid  bool
	}{
		{name: "both sides advertise", valid: true},
		{name: "provider omits the feature", mutate: func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{name: "engine omits the feature", mutate: func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
		}},
		{name: "unknown style", mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
			r.Policy.StatementStyle = 99
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := policyExchange()
			d.Features = append(d.Features, provider.PlanningStatementStyleFeature)
			d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningStatementStyleFeature)
			request.Policy.StatementStyle = protocol.PlanningStatementStyle_PLANNING_STATEMENT_STYLE_EXACT
			if test.mutate != nil {
				test.mutate(d, request)
			}
			err := provider.ValidatePlanningRequest(d, request)
			if test.valid {
				c.Assert(err, qt.IsNil)
			} else {
				c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			}
		})
	}
}

func TestPlanningStatementStyleFeatureNeedsPolicy(t *testing.T) {
	c := qt.New(t)
	d, _ := policyExchange()
	d.Features = append(d.Features, provider.PlanningStatementStyleFeature)
	d.Features = []string{provider.NormalizeValuesFeature, provider.PlanFeaturesFeature, provider.PlanningStatementStyleFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
	d.Features = append(d.Features, provider.PlanningPolicyFeature)
	d.Engines[0].Features = []string{provider.PlanningStatementStyleFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
}

func TestPlanningPolicyFeatureNeedsPlanningOrComparison(t *testing.T) {
	c := qt.New(t)
	d := description()
	d.Features = append(d.Features, provider.PlanningPolicyFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*a planning policy requires planning or comparison`)
	d.Engines[0].Features = []string{provider.PlanningPolicyFeature}
	d.Features = []string{provider.InspectFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*engine planning.policy requires the provider's`)
	// Planning alone is enough, and so is comparison alone.
	d.Features = []string{provider.NormalizeValuesFeature, provider.PlanFeaturesFeature, provider.PlanningPolicyFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	d.Features = []string{provider.NormalizeValuesFeature, provider.CompareModelsFeature, provider.PlanningPolicyFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
}

// rotationExchange is an object comparison that offers the action rotate and
// receives the operator's spelling of a secret.
func rotationExchange() (*protocol.Description, *protocol.CompareModelsRequest, *protocol.ComparedModels) {
	d, request, result := comparisonExchange()
	d.Features = append(d.Features, provider.PlanningPolicyFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.PlanningPolicyFeature)
	service := engine.Comparisons[0]
	service.Placement, service.OwnerKinds, service.Actions = protocol.ModelComparison_OBJECTS, nil, []string{"rotate"}
	request.Requests = []*protocol.RequestedChange{{Action: "rotate", Written: "ext/pg"},
		{Action: "rotate", Subject: proto.CloneOf(request.Desired.Records[0].Subject)}}
	result.ResolvedRequests = []*protocol.ResolvedRequest{{Request: 0, Subject: proto.CloneOf(request.Desired.Records[0].Subject)}}
	return d, request, result
}

func TestWrittenRequests_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := rotationExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateComparedModels(d, request, result), qt.IsNil)
	result.ResolvedRequests = nil
	c.Assert(provider.ValidateComparedModels(d, request, result), qt.IsNil)
}

func TestWrittenRequests_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.CompareModelsRequest, *protocol.ComparedModels)
	}{
		{"a written request without the feature", "a written request needs planning.policy", func(d *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
			r.ResolvedRequests = nil
		}},
		{"a request without a subject", "comparison identity requires a kind and name", func(_ *protocol.Description, q *protocol.CompareModelsRequest, _ *protocol.ComparedModels) {
			q.Requests[1].Subject = nil
		}},
		{"a written request with a subject", "a written request carries one line of text and no subject", func(_ *protocol.Description, q *protocol.CompareModelsRequest, _ *protocol.ComparedModels) {
			q.Requests[0].Subject = proto.CloneOf(q.Requests[1].Subject)
		}},
		{"a written request of two lines", "a written request carries one line of text and no subject", func(_ *protocol.Description, q *protocol.CompareModelsRequest, _ *protocol.ComparedModels) {
			q.Requests[0].Written = "ext/pg\next/other"
		}},
		{"a written request of an action the service lacks", "comparison action is not owned", func(_ *protocol.Description, q *protocol.CompareModelsRequest, _ *protocol.ComparedModels) {
			q.Requests[0].Action = "replace"
		}},
		{"a resolved request outside the batch", "a resolved request names no written request of the batch, or one twice", func(_ *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			r.ResolvedRequests[0].Request = 9
		}},
		{"a resolved request with a subject already", "a resolved request names no written request of the batch, or one twice", func(_ *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			r.ResolvedRequests[0].Request = 1
		}},
		{"a request resolved twice", "a resolved request names no written request of the batch, or one twice", func(_ *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			r.ResolvedRequests = append(r.ResolvedRequests, proto.CloneOf(r.ResolvedRequests[0]))
		}},
		{"a request resolved to an undeclared object", "a request resolves to a desired object the service compares", func(_ *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			r.ResolvedRequests[0].Subject.Name = &protocol.ComparisonPart{Source: "other", Normalized: "other"}
		}},
		{"a request resolved to no identity", "comparison identity requires a kind and name", func(_ *protocol.Description, _ *protocol.CompareModelsRequest, r *protocol.ComparedModels) {
			r.ResolvedRequests[0].Subject = nil
		}},
		{"resolved requests without the feature", "resolved requests need planning.policy", func(d *protocol.Description, q *protocol.CompareModelsRequest, _ *protocol.ComparedModels) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
			q.Requests = q.Requests[1:]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := rotationExchange()
			test.mutate(d, request, result)
			err := provider.ValidateComparedModels(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}
