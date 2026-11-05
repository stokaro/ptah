package schemastats

import (
	"context"
	"fmt"
	"math"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/featurereport"
)

// typeMetrics are the common metrics that count types, with the common kind
// of the types each counts. The types are owners' objects, so each starts at
// zero and keeps its place in the report for every target; an owner's metric
// of the same name and help, such as the PostgreSQL type owner's "domains",
// adds its count to the common one.
var typeMetrics = map[string]objectidentity.Kind{
	"domains":         objectidentity.KindDomain,
	"composite_types": objectidentity.KindComposite,
}

// indexesMetric is the common metric of indexes. An index its engine holds
// outside the common model, such as a YDB vector index, is in no common
// index the read returns, so the owner that holds it counts it, under the
// common metric's name and help, and the count adds to the common one.
const indexesMetric = "indexes"

// joinsCommon reports whether an owner's metric adds to the common metric of
// its name: a type metric or the indexes, where the two mean the same thing,
// which the help says. No other common metric takes an owner's count: nothing
// says the two would count the same thing.
func joinsCommon(common Metric, owner schemaext.MetricDefinition) bool {
	_, counts := typeMetrics[common.Name]
	return (counts || common.Name == indexesMetric) && common.Help == owner.Help
}

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
	reserved := make(map[string]int, len(metrics))
	for i, metric := range metrics {
		reserved[metric.Name] = i
	}
	for _, definition := range report.Definitions {
		for _, metric := range definition.Metrics {
			if i, common := reserved[metric.Name]; common && joinsCommon(metrics[i], metric) {
				positions[metric.Name] = i
				continue
			}
			if i, exists := positions[metric.Name]; exists && metrics[i].Help == metric.Help {
				continue
			}
			if _, exists := positions[metric.Name]; exists || reservedName(reserved, metric.Name) {
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

func reservedName(reserved map[string]int, name string) bool {
	_, found := reserved[name]
	return found
}
