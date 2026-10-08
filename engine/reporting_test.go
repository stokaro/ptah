package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type reportingFunc func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error)

func (f reportingFunc) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return f(ctx, request)
}

func reportingProvider(service schemaext.ReportingService) engine.Provider {
	provider := conversionProvider(nil)
	provider.Conversions = nil
	provider.Reporting = []engine.Reporting{{Representation: schemaext.Desired, Service: service, Definitions: []schemaext.ReportDefinition{
		{Kind: conversionSecond, DisplayName: "second objects", Metrics: []schemaext.MetricDefinition{{Name: "second_count", Help: "Second objects"}}},
		{Kind: conversionFirst, DisplayName: "first objects", Metrics: []schemaext.MetricDefinition{{Name: "first_count", Help: "First objects"}}},
	}}}
	return provider
}

func TestReporting_BatchesAndSnapshotsValuesAndMetadata(t *testing.T) {
	c := qt.New(t)
	calls := 0
	var received schemaext.ReportingRequest
	var reply []schemaext.ValueReport
	service := reportingFunc(func(_ context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
		calls++
		received = request
		for _, value := range request.Values {
			v := value.(*conversionValue)
			name := map[schemaext.Kind]string{conversionFirst: "first_count", conversionSecond: "second_count"}[v.ID]
			reply = append(reply, schemaext.ValueReport{Kind: v.ID, Counts: []schemaext.MetricCount{{Name: name, Value: v.Number}}})
			v.Number = 99
		}
		return reply, nil
	})
	provider := reportingProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Reporting[0].Definitions[0].DisplayName = "mutated"
	provider.Reporting[0].Definitions[0].Metrics[0].Name = "mutated"
	provider.Reporting[0].Service = nil
	values := []schemaext.Value{&conversionValue{ID: conversionSecond, Number: 3}, &conversionValue{ID: conversionFirst, Number: 7}, &conversionValue{ID: conversionSecond, Number: 5}}
	report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: " ALTERNATE ", Representation: schemaext.Desired, Values: values})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received.Target, qt.Equals, " ALTERNATE ")
	c.Assert(received.Values, qt.HasLen, 3)
	c.Assert(report.Definitions[0].Kind, qt.Equals, conversionFirst)
	c.Assert(report.Definitions[1].DisplayName, qt.Equals, "second objects")
	c.Assert(report.Definitions[1].Metrics[0].Name, qt.Equals, "second_count")
	c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{
		{Kind: conversionSecond, Counts: []schemaext.MetricCount{{Name: "second_count", Value: 3}}},
		{Kind: conversionFirst, Counts: []schemaext.MetricCount{{Name: "first_count", Value: 7}}},
		{Kind: conversionSecond, Counts: []schemaext.MetricCount{{Name: "second_count", Value: 5}}},
	})
	c.Assert(values[0].(*conversionValue).Number, qt.Equals, 3)
	reply[0].Counts[0].Value = 100
	c.Assert(report.Values[0].Counts[0].Value, qt.Equals, 3)
	report.Definitions[0].Metrics[0].Name = "modified result"
	empty, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(empty.Definitions[0].Metrics[0].Name, qt.Equals, "first_count")
	c.Assert(empty.Values, qt.HasLen, 0)
}

func TestReporting_RegistrationRefusesConflictsAndInvalidMetadata(t *testing.T) {
	var typedNil reportingFunc
	cases := []struct {
		name   string
		change func(*engine.Provider)
	}{
		{name: "no representation", change: func(p *engine.Provider) { p.Reporting[0].Representation = "" }},
		{name: "non-schema representation", change: func(p *engine.Provider) { p.Reporting[0].Representation = schemaext.Change }},
		{name: "nil service", change: func(p *engine.Provider) { p.Reporting[0].Service = nil }},
		{name: "typed nil service", change: func(p *engine.Provider) { p.Reporting[0].Service = typedNil }},
		{name: "no definitions", change: func(p *engine.Provider) { p.Reporting[0].Definitions = nil }},
		{name: "unknown model", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].Kind = "example.org/missing" }},
		{name: "missing representation", change: func(p *engine.Provider) { p.Codecs = p.Codecs[:2] }},
		{name: "empty label", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].DisplayName = "" }},
		{name: "label control character", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].DisplayName = "first\nobjects" }},
		{name: "unicode line separator", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].DisplayName = "first\u2028objects" }},
		{name: "invalid metric name", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].Metrics[0].Name = "bad-name" }},
		{name: "help control character", change: func(p *engine.Provider) { p.Reporting[0].Definitions[0].Metrics[0].Help = "bad\nhelp" }},
		{name: "duplicate kind", change: func(p *engine.Provider) {
			p.Reporting[0].Definitions = append(p.Reporting[0].Definitions, p.Reporting[0].Definitions[0])
		}},
		{name: "duplicate metric", change: func(p *engine.Provider) { p.Reporting[0].Definitions[1].Metrics[0].Name = "second_count" }},
		{name: "overlapping service", change: func(p *engine.Provider) { p.Reporting = append(p.Reporting, p.Reporting[0]) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			provider := reportingProvider(reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
				calls++
				return nil, nil
			}))
			test.change(&provider)
			runtime, err := engine.New(provider)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestReporting_ModelOwnershipDoesNotRequireTargets(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := reportingProvider(reportingFunc(func(_ context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
		calls++
		return []schemaext.ValueReport{{Kind: request.Values[0].Kind(), Counts: []schemaext.MetricCount{{Name: "first_count", Value: 2}}}}, nil
	}))
	provider.Targets = nil
	observed := provider.Reporting[0]
	observed.Representation = schemaext.Observed
	provider.Reporting = append(provider.Reporting, observed)
	runtime := mustRuntime(c, provider)
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Representation: representation, Values: []schemaext.Value{&conversionValue{ID: conversionFirst}}})
		c.Assert(err, qt.IsNil)
		c.Assert(report.Values[0].Counts[0].Value, qt.Equals, 2)
		c.Assert(report.Definitions, qt.HasLen, 2)
	}
	c.Assert(calls, qt.Equals, 2)
}

func TestReporting_PreflightsMissingHandlerBeforeCallingAnotherService(t *testing.T) {
	c := qt.New(t)
	calls := 0
	provider := reportingProvider(reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
		calls++
		return nil, nil
	}))
	provider.Reporting[0].Definitions = provider.Reporting[0].Definitions[:1]
	runtime := mustRuntime(c, provider)
	report, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Desired,
		Values: []schemaext.Value{&conversionValue{ID: conversionSecond}, &conversionValue{ID: conversionFirst}}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(report, qt.DeepEquals, schemaext.FeatureReport{})
	c.Assert(calls, qt.Equals, 0)
}

func TestReporting_RequiresContextAndRuntime(t *testing.T) {
	c := qt.New(t)
	var missing *engine.Runtime
	request := schemaext.ReportingRequest{Representation: schemaext.Desired}
	report, err := missing.ReportFeatures(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.DeepEquals, schemaext.FeatureReport{})
	runtime := mustRuntime(c)
	var missingContext context.Context
	report, err = runtime.ReportFeatures(missingContext, request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(report, qt.DeepEquals, schemaext.FeatureReport{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err = runtime.ReportFeatures(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(report, qt.DeepEquals, schemaext.FeatureReport{})
}

func TestReporting_PreflightsWholeInput(t *testing.T) {
	cases := []struct {
		name    string
		request schemaext.ReportingRequest
		want    error
	}{
		{name: "no representation", request: schemaext.ReportingRequest{Target: "custom"}, want: schemaext.ErrInvalidValue},
		{name: "unknown last value", request: schemaext.ReportingRequest{Target: "custom", Representation: schemaext.Desired,
			Values: []schemaext.Value{&conversionValue{ID: conversionFirst}, &conversionValue{ID: "example.org/missing"}}}, want: schemaext.ErrUnknownCodec},
		{name: "nil value", request: schemaext.ReportingRequest{Target: "custom", Representation: schemaext.Desired, Values: []schemaext.Value{nil}}, want: schemaext.ErrInvalidValue},
		{name: "typed nil value", request: schemaext.ReportingRequest{Target: "custom", Representation: schemaext.Desired, Values: []schemaext.Value{(*conversionValue)(nil)}}, want: schemaext.ErrInvalidValue},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, reportingProvider(reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
				calls++
				return nil, nil
			})))
			result, err := runtime.ReportFeatures(t.Context(), test.request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.FeatureReport{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

func TestReporting_RejectsMalformedReplies(t *testing.T) {
	cases := []struct {
		name  string
		reply []schemaext.ValueReport
	}{
		{name: "missing report"},
		{name: "wrong kind", reply: []schemaext.ValueReport{{Kind: conversionSecond}}},
		{name: "missing metric", reply: []schemaext.ValueReport{{Kind: conversionFirst}}},
		{name: "unknown metric", reply: []schemaext.ValueReport{{Kind: conversionFirst, Counts: []schemaext.MetricCount{{Name: "invented"}}}}},
		{name: "negative count", reply: []schemaext.ValueReport{{Kind: conversionFirst, Counts: []schemaext.MetricCount{{Name: "first_count", Value: -1}}}}},
		{name: "duplicate count", reply: []schemaext.ValueReport{{Kind: conversionFirst, Counts: []schemaext.MetricCount{{Name: "first_count"}, {Name: "first_count"}}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, reportingProvider(reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
				return test.reply, nil
			})))
			result, err := runtime.ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "custom", Representation: schemaext.Desired,
				Values: []schemaext.Value{&conversionValue{ID: conversionFirst, Number: 1}}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.FeatureReport{})
		})
	}
}

func TestReporting_ServiceFailureAndCancellationDiscardEarlierResults(t *testing.T) {
	failure := errors.New("report service failed")
	canceled, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		service reportingFunc
		want    error
	}{
		{name: "failure", ctx: t.Context(), want: failure, service: reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
			return nil, failure
		})},
		{name: "canceled reply", ctx: canceled, want: context.Canceled, service: reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
			cancel()
			return []schemaext.ValueReport{{Kind: conversionFirst, Counts: []schemaext.MetricCount{{Name: "first_count", Value: 7}}}}, nil
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			provider := reportingProvider(nil)
			definitions := provider.Reporting[0].Definitions
			provider.Reporting = []engine.Reporting{
				{Representation: schemaext.Observed, Definitions: definitions[:1], Service: reportingFunc(func(context.Context, schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
					calls++
					return []schemaext.ValueReport{{Kind: conversionSecond, Counts: []schemaext.MetricCount{{Name: "second_count", Value: 5}}}}, nil
				})},
				{Representation: schemaext.Observed, Definitions: definitions[1:], Service: reportingFunc(func(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
					calls++
					return test.service(ctx, request)
				})},
			}
			runtime := mustRuntime(c, provider)
			result, err := runtime.ReportFeatures(test.ctx, schemaext.ReportingRequest{Target: "custom", Representation: schemaext.Observed,
				Values: []schemaext.Value{&conversionValue{ID: conversionFirst}, &conversionValue{ID: conversionSecond}}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.FeatureReport{})
			c.Assert(calls, qt.Equals, 2)
		})
	}
}
