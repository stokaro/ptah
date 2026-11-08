package providerhost

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
)

// modelReporter publishes the owner's frozen omission label and, when the
// model declares count metrics, the counts the provider gives each value. It
// never interprets a payload: a model without metrics reports no count, and
// one with metrics is counted by its owner through ReportValues.
type modelReporter struct {
	host     *Host
	contract *modelContract
}

func (r modelReporter) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if request.Representation != r.contract.key.representation {
		return nil, fmt.Errorf("%w: reporting representation differs from model", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	captured := make([]*resolvedModel, 0, len(request.Values))
	for _, value := range request.Values {
		resolved, err := r.contract.value(value)
		if err != nil {
			return nil, err
		}
		captured = append(captured, resolved)
		reports = append(reports, schemaext.ValueReport{Kind: r.contract.key.kind})
	}
	if len(r.contract.metrics) != 0 {
		if err := r.count(ctx, captured, reports); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

// count asks the provider for the counts of captured values, one request per
// engine and target profile the values were captured under, in the order the
// profiles first appear, and writes each value's counts into its report.
func (r modelReporter) count(ctx context.Context, captured []*resolvedModel, reports []schemaext.ValueReport) error {
	var groups []*protocol.ReportValuesRequest
	positions := make(map[string]int)
	members := make([][]int, 0)
	for i, value := range captured {
		features := featureSet(append(slices.Clone(value.wire.Features), provider.ReportCountsFeature))
		key := value.wire.Engine + "\x00" + strings.Join(features, "\x00")
		group, found := positions[key]
		if !found {
			group = len(groups)
			positions[key] = group
			groups = append(groups, &protocol.ReportValuesRequest{Engine: value.wire.Engine, TargetFeatures: features})
			members = append(members, nil)
		}
		groups[group].Values = append(groups[group].Values, &protocol.ValueInput{Id: strconv.Itoa(i),
			Representation: protocolRepresentation(r.contract.key.representation), Value: proto.CloneOf(value.normalized.GetValue())})
		members[group] = append(members[group], i)
	}
	for group, request := range groups {
		reply, err := r.host.client.ReportValues(ctx, request)
		if err != nil {
			return err
		}
		for j, position := range members[group] {
			counts := make([]schemaext.MetricCount, 0, len(reply.GetValues()[j].GetCounts()))
			for _, count := range reply.GetValues()[j].GetCounts() {
				value := count.GetValue()
				if value > math.MaxInt {
					return fmt.Errorf("%w: provider count of %q exceeds this platform's integers", provider.ErrProtocol, count.GetName())
				}
				counts = append(counts, schemaext.MetricCount{Name: count.GetName(), Value: int(value)})
			}
			reports[position].Counts = counts
		}
	}
	return nil
}

// reportedModel returns the report engine gives model. The negotiated
// description carries it only when the host lists provider.ReportCountsFeature.
func reportedModel(engine *protocol.Engine, model *protocol.ValueModel) *protocol.ModelReport {
	for _, report := range engine.GetReports() {
		if report.GetKind() == model.GetKind() && report.GetRepresentation() == model.GetRepresentation() {
			return report
		}
	}
	return nil
}

// metricDefinitions returns the model's count metrics as the reporting
// runtime declares them, in the declared order.
func (m *modelContract) metricDefinitions() []schemaext.MetricDefinition {
	if len(m.metrics) == 0 {
		return nil
	}
	result := make([]schemaext.MetricDefinition, 0, len(m.metrics))
	for _, metric := range m.metrics {
		result = append(result, schemaext.MetricDefinition{Name: metric.GetName(), Help: metric.GetHelp()})
	}
	return result
}
