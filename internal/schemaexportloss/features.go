package schemaexportloss

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

// FeatureCounts describes each omitted value through its selected provider.
// The labels are display text; kinds remain the identities while reports are
// matched to input values. No concrete model is imported by this boundary.
func FeatureCounts(ctx context.Context, target string, values []schemaext.Value, runtime schemaext.ReportingRuntime) (map[string]int, error) {
	labels, err := FeatureLabels(ctx, target, values, runtime)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, label := range labels {
		counts[label]++
	}
	return counts, nil
}

// FeatureLabels returns one owner-declared omission label per value in input
// order. A caller can attach the result to its own projection scopes without
// splitting one semantic reporting request into per-object service calls.
func FeatureLabels(ctx context.Context, target string, values []schemaext.Value, runtime schemaext.ReportingRuntime) ([]string, error) {
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
	labels := make(map[schemaext.Kind]string, len(report.Definitions))
	for _, definition := range report.Definitions {
		labels[definition.Kind] = definition.DisplayName
	}
	result := make([]string, len(values))
	for i, described := range report.Values {
		label, found := labels[described.Kind]
		if !found || label == "" || described.Kind != values[i].Kind() {
			return nil, fmt.Errorf("%w: omitted feature has no matching report definition", schemaext.ErrInvalidValue)
		}
		result[i] = label
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
