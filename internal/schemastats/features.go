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
// of the types each counts. A metric counts its objects whoever owns them, so
// an owner's metric of the same name and help, such as the PostgreSQL type
// owner's "domains", adds its count to the common one. No other common metric
// takes an owner's count: nothing says the two would count the same thing.
var typeMetrics = map[string]objectidentity.Kind{
	"domains":         objectidentity.KindDomain,
	"composite_types": objectidentity.KindComposite,
}

// joinsCommon reports whether an owner's metric adds to the common metric of
// its name: a type metric, where the two mean the same thing, which the help
// says.
func joinsCommon(common Metric, owner schemaext.MetricDefinition) bool {
	_, counts := typeMetrics[common.Name]
	return counts && common.Help == owner.Help
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

// typeKey is a type as a count sees it: its common kind, the schema its
// source wrote, empty where the source left it to the default, and its name.
type typeKey struct {
	kind   objectidentity.Kind
	schema string
	name   string
}

// refuseTypesCountedTwice refuses a type the common model holds and an owner's
// object defines too, under the same kind, schema and name. Owners never
// share an object with the common model, so a type metric that joins both
// would count that type twice, and a larger number is no way to say so.
func refuseTypesCountedTwice(db *schemamodel.Database) error {
	common := make(map[typeKey]bool, len(db.Domains)+len(db.CompositeTypes))
	for _, domain := range db.Domains {
		common[typeKey{kind: objectidentity.KindDomain, schema: domain.Schema, name: domain.Name}] = true
	}
	for _, composite := range db.CompositeTypes {
		common[typeKey{kind: objectidentity.KindComposite, schema: composite.Schema, name: composite.Name}] = true
	}
	for _, defined := range schemaext.DefinedTypes(db.FeatureObjects) {
		key := typeKey{kind: defined.ID.Kind, schema: defined.ID.Schema.Authored(), name: defined.ID.Name.Source}
		if common[key] {
			return fmt.Errorf("%w: %s %q is counted both in the common model and as an owner's object", schemaext.ErrDuplicate,
				key.kind, schemamodel.QualifyTableName(key.schema, key.name))
		}
	}
	return nil
}
