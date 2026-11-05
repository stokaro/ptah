package provider_test

import (
	"math"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// reportDescription offers report counts on the value exchange's engine,
// whose retention model declares two metrics.
func reportDescription() *protocol.Description {
	d, _ := valueInputs()
	d.Features = append(d.Features, provider.ReportCountsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ReportCountsFeature)
	model := engine.ValueModels[0]
	model.DisplayName = "retention settings"
	model.Metrics = []*protocol.ModelMetric{{Name: "retained_tables", Help: "Tables with a retention"}, {Name: "retention_days", Help: "Days of retention across tables"}}
	return d
}

// reportRequest asks for the counts of the value exchange's one value.
func reportRequest() *protocol.ReportValuesRequest {
	_, inputs := valueInputs()
	return &protocol.ReportValuesRequest{Engine: inputs.Engine, TargetFeatures: append(inputs.TargetFeatures, provider.ReportCountsFeature), Values: inputs.Values}
}

func valueReports(counts ...uint64) *protocol.ValueReports {
	return &protocol.ValueReports{Complete: true, Values: []*protocol.ValueReport{{Id: "first", Counts: []*protocol.MetricCount{
		{Name: "retained_tables", Value: counts[0]}, {Name: "retention_days", Value: counts[1]},
	}}}}
}

// secondModel adds a model of another kind to the engine, a clone of the
// retention model, and returns it.
func secondModel(d *protocol.Description) *protocol.ValueModel {
	model := proto.CloneOf(d.Engines[0].ValueModels[0])
	model.Kind = "example.org/fixture/archive"
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, model)
	return model
}

func TestReportCounts_HappyPath(t *testing.T) {
	t.Run("a description, a request and its reply", func(t *testing.T) {
		c := qt.New(t)
		d := reportDescription()
		request := reportRequest()
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateReportValuesRequest(d, request), qt.IsNil)
		c.Assert(provider.ValidateValueReports(d, request, valueReports(1, 30)), qt.IsNil)
		c.Assert(provider.ValidateValueReports(d, request, valueReports(0, math.MaxInt64)), qt.IsNil)
	})
	t.Run("two models that share a metric and its help", func(t *testing.T) {
		c := qt.New(t)
		d := reportDescription()
		secondModel(d).Metrics = []*protocol.ModelMetric{{Name: "retained_tables", Help: "Tables with a retention"}}
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("an observed model that names a desired model's metric with another help", func(t *testing.T) {
		c := qt.New(t)
		d := reportDescription()
		observed := proto.CloneOf(d.Engines[0].ValueModels[0])
		observed.Representation = protocol.ValueInput_OBSERVED
		observed.Metrics[0].Help = "Tables a read found with a retention"
		d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, observed)
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(reportDescription(), nil)
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.ReportCountsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReportCountsFeature)
		c.Assert(negotiated.Engines[0].ValueModels[0].Metrics, qt.HasLen, 0)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		listed := provider.NegotiatedDescription(reportDescription(), []string{provider.ReportCountsFeature})
		c.Assert(listed.Engines[0].ValueModels[0].Metrics, qt.HasLen, 2)
	})
}

func TestReportCountsDescription_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		change  func(*protocol.Description)
		wantErr string
	}{
		{"metrics on an engine that does not list the feature", func(d *protocol.Description) {
			d.Engines[0].Features = []string{"retention"}
		}, `.*report counts: a model declares metrics without the feature`},
		{"an engine whose provider does not list the feature", func(d *protocol.Description) {
			d.Features = d.Features[:len(d.Features)-1]
		}, `.*report counts: the engine requires the provider's feature`},
		{"an engine that lists the feature and declares no metric", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].Metrics = nil
		}, `.*report counts: the engine lists the feature and no model declares a metric`},
		{"metrics on a change model", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].Representation = protocol.ValueInput_CHANGE
			d.Engines[0].ValueModels[0].DisplayName = ""
		}, `.*report counts: only a desired or observed model declares metrics`},
		{"metrics without a display name", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].DisplayName = ""
		}, `.*report counts: a model that declares metrics needs a display name`},
		{"an uppercase name", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Name = "Retained" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a name that starts with a digit", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Name = "1tables" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a name that starts with an underscore", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Name = "_tables" },
			`.*report counts: a metric name is invalid or repeated`},
		{"an empty name", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Name = "" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a repeated name", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].Metrics[1].Name = "retained_tables"
		}, `.*report counts: a metric name is invalid or repeated`},
		{"an empty help", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Help = "" },
			`.*report counts: a metric's help is not one line`},
		{"a help of two lines", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].Metrics[0].Help = "Tables\nwith a retention"
		},
			`.*report counts: a metric's help is not one line`},
		{"a help with white space around it", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Help = " Tables" },
			`.*report counts: a metric's help is not one line`},
		{"a help above 256 bytes", func(d *protocol.Description) { d.Engines[0].ValueModels[0].Metrics[0].Help = strings.Repeat("h", 257) },
			`.*report counts: a metric's help is not one line`},
		{"too many metrics", func(d *protocol.Description) {
			model := d.Engines[0].ValueModels[0]
			for i := range 63 {
				model.Metrics = append(model.Metrics, &protocol.ModelMetric{Name: "metric_" + strings.Repeat("x", i+1), Help: "A metric"})
			}
		}, `.*report counts: a model declares too many metrics`},
		{"two models that give one metric two help texts", func(d *protocol.Description) {
			secondModel(d).Metrics = []*protocol.ModelMetric{{Name: "retained_tables", Help: "Archived tables"}}
		}, `.*report counts: two models give one metric two help texts`},
		{"engines that share a model and declare other metrics for it", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0])
			other.Id = "fixture2"
			other.ValueModels[0].Metrics = other.ValueModels[0].Metrics[:1]
			d.Engines = append(d.Engines, other)
		}, `.*report counts: engines that share a model declare other metrics for it`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d := reportDescription()
			row.change(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
}

func TestReportValuesRequest_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		change  func(*protocol.Description, *protocol.ReportValuesRequest)
		wantErr string
	}{
		{"a description without the feature", func(d *protocol.Description, _ *protocol.ReportValuesRequest) {
			d.Features = d.Features[:len(d.Features)-1]
		}, `.*report counts: the feature was not negotiated`},
		{"a target without the feature", func(_ *protocol.Description, r *protocol.ReportValuesRequest) {
			r.TargetFeatures = []string{"retention"}
		}, `.*report counts: the engine does not report counts`},
		{"another engine", func(_ *protocol.Description, r *protocol.ReportValuesRequest) { r.Engine = "other" },
			`.*value engine is not owned by this provider`},
		{"no value", func(_ *protocol.Description, r *protocol.ReportValuesRequest) { r.Values = nil },
			`.*report counts: the request carries no value`},
		{"a repeated value ID", func(_ *protocol.Description, r *protocol.ReportValuesRequest) {
			r.Values = append(r.Values, proto.CloneOf(r.Values[0]))
		}, `.*missing or duplicate value ID`},
		{"a value of a model without metrics", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			secondModel(d).Metrics = nil
			r.Values[0].Value.Kind = "example.org/fixture/archive"
		}, `.*report counts: a value's model declares no metric`},
		{"two representations", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			observed := proto.CloneOf(d.Engines[0].ValueModels[0])
			observed.Representation = protocol.ValueInput_OBSERVED
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, observed)
			second := proto.CloneOf(r.Values[0])
			second.Id, second.Representation = "second", protocol.ValueInput_OBSERVED
			r.Values = append(r.Values, second)
		}, `.*report counts: the values have two representations`},
		{"a change value", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			change := proto.CloneOf(d.Engines[0].ValueModels[0])
			change.Representation, change.DisplayName, change.Metrics = protocol.ValueInput_CHANGE, "", nil
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, change)
			r.Values[0].Representation = protocol.ValueInput_CHANGE
		}, `.*report counts: only desired or observed values are counted`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			d, r := reportDescription(), reportRequest()
			row.change(d, r)
			err := provider.ValidateReportValuesRequest(d, r)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
}

func TestValueReports_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		reply   func() *protocol.ValueReports
		wantErr string
	}{
		{"no reply", func() *protocol.ValueReports { return nil }, `.*report counts: the reply does not answer every value`},
		{"an incomplete reply", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Complete = false
			return reply
		}, `.*report counts: the reply does not answer every value`},
		{"a reply of another length", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Values = append(reply.Values, proto.CloneOf(reply.Values[0]))
			return reply
		}, `.*report counts: the reply does not answer every value`},
		{"another ID", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Values[0].Id = "second"
			return reply
		}, `.*report counts: the reply answers the values in another order`},
		{"a metric left out", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Values[0].Counts = reply.Values[0].Counts[:1]
			return reply
		}, `.*report counts: a report leaves a metric out or counts one twice`},
		{"a metric counted twice", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Values[0].Counts = append(reply.Values[0].Counts, proto.CloneOf(reply.Values[0].Counts[0]))
			return reply
		}, `.*report counts: a report leaves a metric out or counts one twice`},
		{"a metric the model does not declare", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			reply.Values[0].Counts[1].Name = "archived_tables"
			return reply
		}, `.*report counts: a report counts a metric the model does not declare, or in another order`},
		{"the metrics in another order", func() *protocol.ValueReports {
			reply := valueReports(1, 30)
			counts := reply.Values[0].Counts
			counts[0], counts[1] = counts[1], counts[0]
			return reply
		}, `.*report counts: a report counts a metric the model does not declare, or in another order`},
		{"a count above the largest signed 64-bit integer", func() *protocol.ValueReports { return valueReports(1, math.MaxInt64+1) },
			`.*report counts: a count is above the largest signed 64-bit integer`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateValueReports(reportDescription(), reportRequest(), row.reply())
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
}
