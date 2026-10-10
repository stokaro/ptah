package yamlschema

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbttl"
)

// columnStoreSpec is a table's `column_store` block: YDB column storage, its
// hash key, its shard count and its tiered TTL.
type columnStoreSpec struct {
	HashColumns []string       `yaml:"hash_columns,omitempty"`
	Partitions  uint64         `yaml:"partitions,omitempty"`
	TTL         *tieredTTLSpec `yaml:"ttl,omitempty"`
}

// tieredTTLSpec is a column table's `ttl` block.
type tieredTTLSpec struct {
	Column string        `yaml:"column"`
	Unit   string        `yaml:"unit,omitempty"`
	Tiers  []ttlTierSpec `yaml:"tiers"`
}

// ttlTierSpec is one tier of a column table's `ttl` block.
type ttlTierSpec struct {
	Interval       string `yaml:"interval"`
	ExternalSource string `yaml:"external_source,omitempty"`
}

// declare adds the YDB owner's declaration of the block to facets; a table
// without one is a row table and adds nothing. The unit is kept as YQL writes
// it.
func (s *columnStoreSpec) declare(facets schemaext.Facets) (schemaext.Facets, error) {
	if s == nil {
		return facets, nil
	}
	store := &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: s.HashColumns, Partitions: s.Partitions}}
	if s.TTL != nil {
		unit, err := ydbttl.Unit(s.TTL.Unit)
		if err != nil {
			return schemaext.Facets{}, err
		}
		store.TTL = &ydbschema.TieredTTL{Column: s.TTL.Column, Unit: unit}
		for _, tier := range s.TTL.Tiers {
			store.TTL.Tiers = append(store.TTL.Tiers, ydbschema.TTLTier{Interval: tier.Interval, ExternalSource: tier.ExternalSource})
		}
	}
	if err := ydbschema.CheckColumnStore(store.ColumnStore); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(store)
}
