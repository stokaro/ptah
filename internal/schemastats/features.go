package schemastats

import (
	"context"
	"fmt"
	"math"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/featurereport"
)

func appendFeatureMetrics(ctx context.Context, db *schemamodel.Database, target string, runtime schemaext.ReportingRuntime, metrics []Metric) ([]Metric, error) {
	values, err := featurereport.Values(db)
	if err != nil {
		return nil, err
	}
	report, err := runtime.ReportFeatures(ctx, schemaext.ReportingRequest{Target: target, Representation: schemaext.Desired, Values: values})
	if err != nil {
		return nil, err
	}
	positions := make(map[string]int, len(metrics))
	reserved := make(map[string]bool, len(metrics))
	for _, metric := range metrics {
		reserved[metric.Name] = true
	}
	for _, definition := range report.Definitions {
		for _, metric := range definition.Metrics {
			if _, exists := positions[metric.Name]; exists || reserved[metric.Name] {
				return nil, fmt.Errorf("%w: feature metric conflicts with %q", schemaext.ErrDuplicate, metric.Name)
			}
			positions[metric.Name] = len(metrics)
			metrics = append(metrics, Metric{Name: metric.Name, Help: metric.Help})
		}
	}
	for _, value := range report.Values {
		for _, count := range value.Counts {
			i, found := positions[count.Name]
			if !found || count.Value < 0 || count.Value > math.MaxInt-metrics[i].Value {
				return nil, fmt.Errorf("%w: invalid or overflowing feature metric %q", schemaext.ErrInvalidValue, count.Name)
			}
			metrics[i].Value += count.Value
		}
	}
	return metrics, ctx.Err()
}
