package provider_test

import (
	"math"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// reportDescription offers report counts on the value exchange's engine,
// which reports two metrics of its retention model.
func reportDescription() *protocol.Description {
	d, _ := valueInputs()
	d.Features = append(d.Features, provider.ReportCountsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ReportCountsFeature)
	model := engine.ValueModels[0]
	model.DisplayName = "retention settings"
	engine.Reports = []*protocol.ModelReport{{Kind: model.Kind, Representation: model.Representation, Metrics: []*protocol.ModelMetric{
		{Name: "retained_tables", Help: "Tables with a retention"}, {Name: "retention_days", Help: "Days of retention across tables"},
	}}}
	return d
}

// omissionCounts lists models.omission-counts on the provider and on each of
// its engines, which a report needs before it may set an omission label or
// count metric.
func omissionCounts(d *protocol.Description) *protocol.Description {
	d.Features = append(d.Features, provider.OmissionCountsFeature)
	for _, engine := range d.Engines {
		engine.Features = append(engine.Features, provider.OmissionCountsFeature)
	}
	return d
}

// metrics are the metrics the engine reports for its first model.
func metrics(d *protocol.Description) []*protocol.ModelMetric { return d.Engines[0].Reports[0].Metrics }

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
// retention model that the engine does not report, and returns it.
func secondModel(d *protocol.Description) *protocol.ValueModel {
	model := proto.CloneOf(d.Engines[0].ValueModels[0])
	model.Kind = "example.org/fixture/archive"
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, model)
	return model
}

// report reports the model with the metrics, beside the engine's other
// reports.
func report(d *protocol.Description, model *protocol.ValueModel, metrics ...*protocol.ModelMetric) {
	d.Engines[0].Reports = append(d.Engines[0].Reports, &protocol.ModelReport{Kind: model.Kind, Representation: model.Representation, Metrics: metrics})
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
		report(d, secondModel(d), &protocol.ModelMetric{Name: "retained_tables", Help: "Tables with a retention"})
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("an observed model that names a desired model's metric with another help", func(t *testing.T) {
		c := qt.New(t)
		d := reportDescription()
		observed := proto.CloneOf(d.Engines[0].ValueModels[0])
		observed.Representation = protocol.ValueInput_OBSERVED
		d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, observed)
		report(d, observed, &protocol.ModelMetric{Name: "retained_tables", Help: "Tables a read found with a retention"})
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("a report chooses an omission count and label", func(t *testing.T) {
		c := qt.New(t)
		d := omissionCounts(reportDescription())
		d.Engines[0].Reports[0].OmissionLabel = "retentions"
		d.Engines[0].Reports[0].OmissionCountMetric = "retained_tables"
		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidateValueReports(d, reportRequest(), valueReports(2, 30)), qt.IsNil)
	})
	t.Run("a host that does not list the feature", func(t *testing.T) {
		c := qt.New(t)
		negotiated := provider.NegotiatedDescription(reportDescription(), nil)
		c.Assert(negotiated.Features, qt.Not(qt.Contains), provider.ReportCountsFeature)
		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReportCountsFeature)
		c.Assert(negotiated.Engines[0].Reports, qt.HasLen, 0)
		c.Assert(proto.Equal(negotiated.Engines[0].ValueModels[0], reportDescription().Engines[0].ValueModels[0]), qt.IsTrue)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
		listed := provider.NegotiatedDescription(reportDescription(), []string{provider.ReportCountsFeature})
		c.Assert(listed.Engines[0].Reports, qt.HasLen, 1)
	})
	t.Run("a host that counts reports and does not read omission counts", func(t *testing.T) {
		c := qt.New(t)
		d := omissionCounts(reportDescription())
		d.Engines[0].Reports[0].OmissionLabel = "retentions"
		d.Engines[0].Reports[0].OmissionCountMetric = "retained_tables"

		counted := provider.NegotiatedDescription(d, []string{provider.ReportCountsFeature})
		both := provider.NegotiatedDescription(d, []string{provider.ReportCountsFeature, provider.OmissionCountsFeature})

		c.Assert(counted.Engines[0].Features, qt.Not(qt.Contains), provider.OmissionCountsFeature)
		c.Assert(counted.Engines[0].Reports[0].OmissionLabel, qt.Equals, "")
		c.Assert(counted.Engines[0].Reports[0].OmissionCountMetric, qt.Equals, "")
		c.Assert(counted.Engines[0].Reports[0].Metrics, qt.HasLen, 2)
		c.Assert(provider.ValidateDescription(counted, nil), qt.IsNil)
		c.Assert(both.Engines[0].Reports[0].OmissionLabel, qt.Equals, "retentions")
		c.Assert(both.Engines[0].Reports[0].OmissionCountMetric, qt.Equals, "retained_tables")
		c.Assert(d.Engines[0].Reports[0].OmissionLabel, qt.Equals, "retentions", qt.Commentf("negotiation must not change the description"))
	})
}

func TestReportCountsDescription_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		change  func(*protocol.Description)
		wantErr string
	}{
		{"reports on an engine that does not list the feature", func(d *protocol.Description) {
			d.Engines[0].Features = []string{"retention"}
		}, `.*report counts: the engine declares reports without the feature`},
		{"an engine whose provider does not list the feature", func(d *protocol.Description) {
			d.Features = d.Features[:len(d.Features)-1]
		}, `.*report counts: the engine requires the provider's feature`},
		{"an engine that lists the feature and reports no model", func(d *protocol.Description) {
			d.Engines[0].Reports = nil
		}, `.*report counts: the engine lists the feature and reports no model`},
		{"a report of a change model", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].Representation = protocol.ValueInput_CHANGE
			d.Engines[0].ValueModels[0].DisplayName = ""
			d.Engines[0].Reports[0].Representation = protocol.ValueInput_CHANGE
		}, `.*report counts: only a desired or observed model is reported`},
		{"a report of a model the engine lacks", func(d *protocol.Description) {
			d.Engines[0].Reports[0].Kind = "example.org/fixture/archive"
		}, `.*report counts: a report names no model of the engine`},
		{"a report of another representation", func(d *protocol.Description) {
			d.Engines[0].Reports[0].Representation = protocol.ValueInput_OBSERVED
		}, `.*report counts: a report names no model of the engine`},
		{"a model reported twice", func(d *protocol.Description) {
			d.Engines[0].Reports = append(d.Engines[0].Reports, proto.CloneOf(d.Engines[0].Reports[0]))
		}, `.*report counts: the engine reports a model twice`},
		{"a report without metrics", func(d *protocol.Description) {
			d.Engines[0].Reports[0].Metrics = nil
		}, `.*report counts: a report declares no metric or too many`},
		{"a report of a model without a display name", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].DisplayName = ""
		}, `.*report counts: a reported model needs a display name`},
		{"an uppercase name", func(d *protocol.Description) { metrics(d)[0].Name = "Retained" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a name that starts with a digit", func(d *protocol.Description) { metrics(d)[0].Name = "1tables" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a name that starts with an underscore", func(d *protocol.Description) { metrics(d)[0].Name = "_tables" },
			`.*report counts: a metric name is invalid or repeated`},
		{"an empty name", func(d *protocol.Description) { metrics(d)[0].Name = "" },
			`.*report counts: a metric name is invalid or repeated`},
		{"a repeated name", func(d *protocol.Description) {
			metrics(d)[1].Name = "retained_tables"
		}, `.*report counts: a metric name is invalid or repeated`},
		{"an empty help", func(d *protocol.Description) { metrics(d)[0].Help = "" },
			`.*report counts: a metric's help is not one line`},
		{"an omission label of two lines", func(d *protocol.Description) {
			omissionCounts(d).Engines[0].Reports[0].OmissionLabel = "retained\ntables"
		}, `.*report counts: an omission label is not one line`},
		{"an omission label with surrounding space", func(d *protocol.Description) {
			omissionCounts(d).Engines[0].Reports[0].OmissionLabel = " retentions"
		}, `.*report counts: an omission label is not one line`},
		{"an undeclared omission count metric", func(d *protocol.Description) {
			omissionCounts(d).Engines[0].Reports[0].OmissionCountMetric = "missing_tables"
		}, `.*report counts: an omission count metric is not declared`},
		{"an omission label the engine does not list the feature for", func(d *protocol.Description) {
			d.Features = append(d.Features, provider.OmissionCountsFeature)
			d.Engines[0].Reports[0].OmissionLabel = "retentions"
		}, `.*report counts: an omission label or count metric needs models\.omission-counts`},
		{"an omission count metric the provider does not list the feature for", func(d *protocol.Description) {
			d.Engines[0].Features = append(d.Engines[0].Features, provider.OmissionCountsFeature)
			d.Engines[0].Reports[0].OmissionCountMetric = "retained_tables"
		}, `.*report counts: an omission label or count metric needs models\.omission-counts`},
		{"an engine that lists omission counts the provider does not list", func(d *protocol.Description) {
			d.Engines[0].Features = append(d.Engines[0].Features, provider.OmissionCountsFeature)
		}, `.*report counts: an engine that lists models\.omission-counts lists models\.report-counts, and so does the provider`},
		{"omission counts without report counts", func(d *protocol.Description) {
			omissionCounts(d)
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.ReportCountsFeature })
			d.Engines[0].Reports = nil
		}, `.*report counts: an engine that lists models\.omission-counts lists models\.report-counts, and so does the provider`},
		{"a help of two lines", func(d *protocol.Description) {
			metrics(d)[0].Help = "Tables\nwith a retention"
		},
			`.*report counts: a metric's help is not one line`},
		{"a help with white space around it", func(d *protocol.Description) { metrics(d)[0].Help = " Tables" },
			`.*report counts: a metric's help is not one line`},
		{"a help above 256 bytes", func(d *protocol.Description) { metrics(d)[0].Help = strings.Repeat("h", 257) },
			`.*report counts: a metric's help is not one line`},
		{"too many metrics", func(d *protocol.Description) {
			report := d.Engines[0].Reports[0]
			for i := range 63 {
				report.Metrics = append(report.Metrics, &protocol.ModelMetric{Name: "metric_" + strings.Repeat("x", i+1), Help: "A metric"})
			}
		}, `.*report counts: a report declares no metric or too many`},
		{"two reports that give one metric two help texts", func(d *protocol.Description) {
			report(d, secondModel(d), &protocol.ModelMetric{Name: "retained_tables", Help: "Archived tables"})
		}, `.*report counts: two reports give one metric two help texts`},
		{"engines that share a model and report other metrics for it", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0])
			other.Id = "fixture2"
			other.Reports[0].Metrics = other.Reports[0].Metrics[:1]
			d.Engines = append(d.Engines, other)
		}, `.*report counts: engines that share a model report other metrics for it`},
		{"engines that share a model choose different omission counts", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0])
			other.Id = "fixture2"
			other.Reports[0].OmissionCountMetric = "retained_tables"
			omissionCounts(d)
			d.Engines = append(d.Engines, other)
			other.Features = append(other.Features, provider.OmissionCountsFeature)
		}, `.*report counts: engines that share a model report other omission metadata for it`},
		{"engines that share a model choose different omission labels", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0])
			other.Id = "fixture2"
			other.Reports[0].OmissionLabel = "retentions"
			omissionCounts(d)
			d.Engines = append(d.Engines, other)
			other.Features = append(other.Features, provider.OmissionCountsFeature)
		}, `.*report counts: engines that share a model report other omission metadata for it`},
		{"engines that share a model and one reports it", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0])
			other.Id = "fixture2"
			other.Features = []string{"retention"}
			other.Reports = nil
			d.Engines = append(d.Engines, other)
		}, `.*report counts: engines that share a model report other metrics for it`},
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
		{"a value of a model the engine does not report", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			secondModel(d)
			r.Values[0].Value.Kind = "example.org/fixture/archive"
		}, `.*report counts: the engine reports no model of a value`},
		{"two representations", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			observed := proto.CloneOf(d.Engines[0].ValueModels[0])
			observed.Representation = protocol.ValueInput_OBSERVED
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, observed)
			report(d, observed, metrics(d)...)
			second := proto.CloneOf(r.Values[0])
			second.Id, second.Representation = "second", protocol.ValueInput_OBSERVED
			r.Values = append(r.Values, second)
		}, `.*report counts: the values have two representations`},
		{"a change value", func(d *protocol.Description, r *protocol.ReportValuesRequest) {
			change := proto.CloneOf(d.Engines[0].ValueModels[0])
			change.Representation, change.DisplayName = protocol.ValueInput_CHANGE, ""
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
