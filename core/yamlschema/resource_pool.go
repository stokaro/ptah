package yamlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbpool"
)

// resourcePoolSpec is one YDB resource pool in a YAML document, keyed by its
// name, with its settings spelled as YDB spells them. Each setting is a
// pointer, so a setting the document leaves out, which has no limit, is told
// from one written with an empty value, which is refused.
type resourcePoolSpec struct {
	ConcurrentQueryLimit           *stringScalar `yaml:"concurrent_query_limit"`
	QueueSize                      *stringScalar `yaml:"queue_size"`
	DatabaseLoadCPUThreshold       *stringScalar `yaml:"database_load_cpu_threshold"`
	QueryMemoryLimitPercentPerNode *stringScalar `yaml:"query_memory_limit_percent_per_node"`
	QueryCPULimitPercentPerNode    *stringScalar `yaml:"query_cpu_limit_percent_per_node"`
	TotalCPULimitPercentPerNode    *stringScalar `yaml:"total_cpu_limit_percent_per_node"`
	ResourceWeight                 *stringScalar `yaml:"resource_weight"`
}

// values are the pool's settings keyed by attribute name, as
// [ydbpool.ParsePool] reads them.
func (spec resourcePoolSpec) values(name string) map[string]string {
	values := map[string]string{ydbpool.AttributeName: name}
	for attribute, value := range map[string]*stringScalar{
		ydbpool.AttributeConcurrentQueryLimit:           spec.ConcurrentQueryLimit,
		ydbpool.AttributeQueueSize:                      spec.QueueSize,
		ydbpool.AttributeDatabaseLoadCPUThreshold:       spec.DatabaseLoadCPUThreshold,
		ydbpool.AttributeQueryMemoryLimitPercentPerNode: spec.QueryMemoryLimitPercentPerNode,
		ydbpool.AttributeQueryCPULimitPercentPerNode:    spec.QueryCPULimitPercentPerNode,
		ydbpool.AttributeTotalCPULimitPercentPerNode:    spec.TotalCPULimitPercentPerNode,
		ydbpool.AttributeResourceWeight:                 spec.ResourceWeight,
	} {
		if value != nil {
			values[attribute] = string(*value)
		}
	}
	return values
}

// resourcePoolClassifierSpec is one YDB resource pool classifier, keyed by its
// name.
type resourcePoolClassifierSpec struct {
	ResourcePool stringScalar  `yaml:"resource_pool"`
	MemberName   stringScalar  `yaml:"member_name"`
	Rank         *stringScalar `yaml:"rank"`
}

// values are the classifier's settings keyed by attribute name, as
// [ydbpool.ParseClassifier] reads them.
func (spec resourcePoolClassifierSpec) values(name string) map[string]string {
	values := map[string]string{
		ydbpool.AttributeName:         name,
		ydbpool.AttributeResourcePool: string(spec.ResourcePool),
		ydbpool.AttributeMemberName:   string(spec.MemberName),
	}
	if spec.Rank != nil {
		values[ydbpool.AttributeRank] = string(*spec.Rank)
	}
	return values
}

// addResourcePools reads the document's resource pools and classifiers, each
// checked by the rules the annotation parser reads one with.
func (d document) addResourcePools(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.ResourcePools) {
		name, spec, err := ydbpool.ParsePool(d.ResourcePools[key].values(key))
		if err != nil {
			return fmt.Errorf("resource pool %q: %w", key, err)
		}
		db.ResourcePools = append(db.ResourcePools, schemamodel.ResourcePool{Name: name, Spec: spec})
	}
	for _, key := range sortedKeys(d.ResourcePoolClassifiers) {
		name, spec, err := ydbpool.ParseClassifier(d.ResourcePoolClassifiers[key].values(key))
		if err != nil {
			return fmt.Errorf("resource pool classifier %q: %w", key, err)
		}
		db.ResourcePoolClassifiers = append(db.ResourcePoolClassifiers,
			schemamodel.ResourcePoolClassifier{Name: name, Spec: spec})
	}
	return nil
}
