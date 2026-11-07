package schemaexportloss

import (
	"context"
	"fmt"
	"math"
	"slices"

	"ptah.run/core/schemaext"
)

// FeatureCounts adds the owner-declared omitted object counts by label.
// Kinds remain the identities while reports are matched to input values.
// No concrete model is imported by this boundary.
func FeatureCounts(ctx context.Context, target string, values []schemaext.Value, runtime schemaext.ReportingRuntime) (map[string]int, error) {
	omissions, err := FeatureOmissions(ctx, target, values, runtime)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, omission := range omissions {
		if counts[omission.Label] > math.MaxInt-omission.Count {
			return nil, fmt.Errorf("%w: omitted feature count overflows", schemaext.ErrInvalidValue)
		}
		counts[omission.Label] += omission.Count
	}
	return counts, nil
}

// FeatureLabels returns one owner-declared omission label per value in input
// order. It preserves the label-only API for callers that do not need counts.
func FeatureLabels(ctx context.Context, target string, values []schemaext.Value, runtime schemaext.ReportingRuntime) ([]string, error) {
	omissions, err := FeatureOmissions(ctx, target, values, runtime)
	if err != nil {
		return nil, err
	}
	labels := make([]string, len(omissions))
	for i, omission := range omissions {
		labels[i] = omission.Label
	}
	return labels, nil
}

// FeatureOmission is the owner-declared label and object count for one value.
type FeatureOmission struct {
	Label string
	Count int
}

// FeatureOmissions reports each value in input order. A caller can attach the
// counts to projection scopes without splitting the provider request.
func FeatureOmissions(ctx context.Context, target string, values []schemaext.Value, runtime schemaext.ReportingRuntime) ([]FeatureOmission, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	report, err := runtime.ReportFeatures(ctx, schemaext.ReportingRequest{Target: target, Representation: schemaext.Desired, Values: values})
	if err != nil {
		return nil, err
	}
	if len(report.Values) != len(values) {
		return nil, fmt.Errorf("%w: omitted feature report changed the value count", schemaext.ErrInvalidValue)
	}
	definitions := make(map[schemaext.Kind]schemaext.ReportDefinition, len(report.Definitions))
	for _, definition := range report.Definitions {
		definitions[definition.Kind] = definition
	}
	result := make([]FeatureOmission, len(values))
	for i, described := range report.Values {
		definition, found := definitions[described.Kind]
		if !found || definition.DisplayName == "" || described.Kind != values[i].Kind() {
			return nil, fmt.Errorf("%w: omitted feature has no matching report definition", schemaext.ErrInvalidValue)
		}
		label := definition.DisplayName
		if definition.OmissionLabel != "" {
			label = definition.OmissionLabel
		}
		count := 1
		if definition.OmissionCountMetric != "" {
			position := slices.IndexFunc(described.Counts, func(metric schemaext.MetricCount) bool {
				return metric.Name == definition.OmissionCountMetric
			})
			if position < 0 {
				return nil, fmt.Errorf("%w: omitted feature has no count metric", schemaext.ErrInvalidValue)
			}
			count = described.Counts[position].Value
		}
		if count < 0 {
			return nil, fmt.Errorf("%w: omitted feature has a negative count", schemaext.ErrInvalidValue)
		}
		result[i] = FeatureOmission{Label: label, Count: count}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
