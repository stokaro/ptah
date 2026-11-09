package providerhost

// White-box testing required: the metrics a model contract carries are
// assembled from the negotiated engines into private host state, which the
// reporting registration reads. The provider's counts across the process
// boundary are covered by the providercli process tests.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// reportingTarget is the YAML identity target with a normalizing provider and
// a display name on its model, which its engine reports with metrics.
func reportingTarget(metrics ...*protocol.ModelMetric) *target {
	selected := yamlIdentityTarget()
	selected.host.description.Features = []string{provider.NormalizeValuesFeature}
	model := selected.engine.ValueModels[0]
	model.DisplayName = "retention values"
	if len(metrics) != 0 {
		selected.engine.Reports = []*protocol.ModelReport{{Kind: model.Kind, Representation: model.Representation, Metrics: metrics}}
	}
	return selected
}

func TestModelContractsCarryTheReportedMetrics(t *testing.T) {
	for _, row := range []struct {
		name    string
		metrics []*protocol.ModelMetric
		want    []schemaext.MetricDefinition
	}{
		{name: "an unreported model"},
		{name: "a reported model", metrics: []*protocol.ModelMetric{{Name: "retained", Help: "Retained objects"}, {Name: "retained_seconds", Help: "Retained seconds"}},
			want: []schemaext.MetricDefinition{{Name: "retained", Help: "Retained objects"}, {Name: "retained_seconds", Help: "Retained seconds"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			selected := reportingTarget(row.metrics...)
			c.Assert(selected.host.initializeModels(), qt.IsNil)
			contract := selected.host.models[modelKey{kind: "example.org/fixture/value", representation: schemaext.Desired}]
			c.Assert(contract.metricDefinitions(), qt.DeepEquals, row.want)
		})
	}
}

// The registered reporting definition gives export consumers the provider's
// label and the metric that counts the omitted objects inside one value.
func TestProviderReportingCarriesOmissionCounts(t *testing.T) {
	c := qt.New(t)
	selected := reportingTarget(&protocol.ModelMetric{Name: "retained", Help: "Retained objects"})
	selected.engine.Reports[0].OmissionLabel = "retentions"
	selected.engine.Reports[0].OmissionCountMetric = "retained"
	c.Assert(selected.host.initializeModels(), qt.IsNil)

	reporting := selected.host.Provider().Reporting

	c.Assert(reporting, qt.HasLen, 1)
	c.Assert(reporting[0].Definitions, qt.DeepEquals, []schemaext.ReportDefinition{{
		Kind: "example.org/fixture/value", DisplayName: "retention values", OmissionLabel: "retentions",
		OmissionCountMetric: "retained", Metrics: []schemaext.MetricDefinition{{Name: "retained", Help: "Retained objects"}},
	}})
}

// Engines that share a model report the same metrics for it, or the host
// refuses the provider, as it refuses an engine-dependent display name.
func TestModelsRefuseEngineDependentReportMetrics(t *testing.T) {
	for _, row := range []struct {
		name    string
		reports []*protocol.ModelReport
	}{
		{name: "an engine that does not report the model"},
		{name: "an engine that reports another help", reports: []*protocol.ModelReport{{Kind: "example.org/fixture/value", Representation: protocol.ValueInput_DESIRED,
			Metrics: []*protocol.ModelMetric{{Name: "retained", Help: "Objects retained"}}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			selected := reportingTarget(&protocol.ModelMetric{Name: "retained", Help: "Retained objects"})
			other := proto.CloneOf(selected.engine)
			other.Id, other.Reports = "other", row.reports
			selected.host.description.Engines = append(selected.host.description.Engines, other)
			c.Assert(selected.host.initializeModels(), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// Shared models have one omission definition. A later engine cannot change
// the label or the metric and make export totals depend on the selected engine.
func TestModelsRefuseEngineDependentOmissionMetadata(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*protocol.ModelReport)
	}{
		{name: "label", change: func(report *protocol.ModelReport) { report.OmissionLabel = "other retentions" }},
		{name: "metric", change: func(report *protocol.ModelReport) { report.OmissionCountMetric = "" }},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			selected := reportingTarget(&protocol.ModelMetric{Name: "retained", Help: "Retained objects"})
			selected.engine.Reports[0].OmissionLabel = "retentions"
			selected.engine.Reports[0].OmissionCountMetric = "retained"
			other := proto.CloneOf(selected.engine)
			other.Id = "other"
			row.change(other.Reports[0])
			selected.host.description.Engines = append(selected.host.description.Engines, other)

			c.Assert(selected.host.initializeModels(), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
