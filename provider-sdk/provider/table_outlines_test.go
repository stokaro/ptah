package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// outlinedExchange is planningExchange for a service that plans the change
// alone, assigned no parent kind, and reads the outlines of its tables: the
// request carries each table by its subject, its action and the names of its
// states.
func outlinedExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	d.Features = append(d.Features, provider.TableOutlinesFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.TableOutlinesFeature)
	service := d.Engines[0].Planning[0]
	service.ParentKinds, service.Tables = nil, protocol.PlanningTables_PLANNING_TABLES_OUTLINED
	request.ParentKinds = nil
	for i, table := range request.Tables {
		request.Tables[i] = provider.TableOutline(table)
	}
	result.Parents = nil
	return d, request, result
}

func TestTableOutlines_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := outlinedExchange()
	_, whole, _ := planningExchange()

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.TableOutlinesFeature)
	negotiated := provider.NegotiatedDescription(d, []string{provider.TableOutlinesFeature})
	c.Assert(negotiated.Engines[0].Planning[0].GetTables(), qt.Equals, protocol.PlanningTables_PLANNING_TABLES_OUTLINED)
	c.Assert(provider.ValidatePlanningRequest(negotiated, request), qt.IsNil)
	c.Assert(provider.ValidatePlannedFeatures(negotiated, request, result), qt.IsNil)
	c.Assert(proto.Size(request.Tables[0]) < proto.Size(whole.Tables[0]), qt.IsTrue)
	c.Assert(request.Tables[0].GetDesired().GetName(), qt.Equals, "C")
	c.Assert(request.Tables[0].GetCurrent().GetName(), qt.Equals, "C")
}

// A host that does not list the feature sends every table whole, and the
// negotiated description says the service reads them whole, so the whole
// tables validate and the provider's service reads them as it reads outlines.
func TestTableOutlinesWithoutTheHost_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _, result := outlinedExchange()
	_, whole, _ := planningExchange()
	whole.ParentKinds = nil

	negotiated := provider.NegotiatedDescription(d, nil)

	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.TableOutlinesFeature)
	c.Assert(negotiated.Engines[0].Planning[0].GetTables(), qt.Equals, protocol.PlanningTables_PLANNING_TABLES_CAPTURED)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.ValidatePlannedFeatures(negotiated, whole, result), qt.IsNil)
	c.Assert(d.Engines[0].Planning[0].GetTables(), qt.Equals, protocol.PlanningTables_PLANNING_TABLES_OUTLINED)
}

// A service reads outlines only beside the engine's and the provider's
// feature, and only when it plans no table-attached model.
func TestTableOutlinesDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.Description)
		wantErr string
	}{
		{name: "an outlined service without the engine's feature", wantErr: `.*table outlines: planning service retention reads outlines without the engine's feature`,
			mutate: func(d *protocol.Description) {
				d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.TableOutlinesFeature)
			}},
		{name: "the engine's feature without the provider's", wantErr: `.*table outlines: the engine's feature requires the provider's`,
			mutate: func(d *protocol.Description) { d.Features = withoutFeature(d.Features, provider.TableOutlinesFeature) }},
		{name: "an outlined service with parent kinds", wantErr: `.*table outlines: planning service retention plans parent kinds, which an outline leaves out`,
			mutate: func(d *protocol.Description) {
				d.Engines[0].Planning[0].ParentKinds = []string{d.Engines[0].ValueModels[0].Kind}
			}},
		{name: "a table form this SDK does not know", wantErr: `.*table outlines: planning service retention names an unknown table form`,
			mutate: func(d *protocol.Description) { d.Engines[0].Planning[0].Tables = protocol.PlanningTables(7) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := outlinedExchange()
			test.mutate(d)

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// An outlined request carries each table by its subject, its action and the
// names of its states and nothing else, and a service that reads whole tables
// is never sent an outline.
func TestOutlinedPlanningRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.Description, *protocol.PlanFeaturesRequest)
		wantErr string
	}{
		{name: "a table with its columns", wantErr: `.*table outlines: an outlined table carries more than its subject, action and the names of its states`,
			mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
				r.Tables[0].Desired.Columns = []*protocol.DeclaredColumn{{Name: "id", Type: "INTEGER"}}
			}},
		{name: "a table with its captured features", wantErr: `.*table outlines: an outlined table carries more than its subject, action and the names of its states`,
			mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
				r.Tables[0].Current.Features = &protocol.CapturedFeatures{}
			}},
		{name: "a state without its name", wantErr: `.*missing outlined table name`,
			mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) { r.Tables[0].Desired.Name = "" }},
		{name: "an action the states disagree with", wantErr: `.*planning table action disagrees with captured state`,
			mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
				r.Tables[0].Action = protocol.PlanningTable_CREATE
			}},
		{name: "two tables of one subject", wantErr: `.*duplicate comparison identity`,
			mutate: func(_ *protocol.Description, r *protocol.PlanFeaturesRequest) {
				r.Tables = append(r.Tables, proto.CloneOf(r.Tables[0]))
			}},
		{name: "an outline for a service that reads whole tables", wantErr: `.*missing captured feature knowledge`,
			mutate: func(d *protocol.Description, _ *protocol.PlanFeaturesRequest) {
				d.Engines[0].Planning[0].Tables = protocol.PlanningTables_PLANNING_TABLES_CAPTURED
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := outlinedExchange()
			test.mutate(d, request)

			err := provider.ValidatePlanningRequest(d, request)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
