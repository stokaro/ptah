package providerhost

// White-box testing required: the metrics a model contract carries are
// assembled from the negotiated engines into private host state, which the
// reporting registration reads. The provider's counts across the process
// boundary are covered by the providercli process tests.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
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
