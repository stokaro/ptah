package ast

import "slices"

// YDBColumnTableSpec selects YDB's column-oriented storage. A nil spec selects
// row storage. HashColumns is the hash-partitioning key; an empty list uses the
// primary key. Partitions is the initial shard count, or zero for the server's
// default. These properties cannot be changed by an ordinary ALTER TABLE.
type YDBColumnTableSpec struct {
	// HashColumns names primary-key columns used to distribute rows.
	HashColumns []string `json:"hash_columns,omitempty" yaml:"hash_columns,omitempty"`
	// Partitions is the initial number of column shards; zero uses the server default.
	Partitions uint64 `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// TTL moves older data to external storage, optionally deleting the oldest tier.
	TTL *YDBTieredTTLSpec `json:"ttl,omitempty" yaml:"ttl,omitempty"`
}

// YDBTieredTTLSpec is a column table's ordered retention policy.
type YDBTieredTTLSpec struct {
	// Column holds the timestamp or integer epoch value the intervals start from.
	Column string `json:"column" yaml:"column"`
	// Unit names the unit of an integer epoch column, or is empty for a date column.
	Unit string `json:"unit,omitempty" yaml:"unit,omitempty"`
	// Tiers apply in increasing order of age. Only the last tier may delete data.
	Tiers []YDBTTLTierSpec `json:"tiers" yaml:"tiers"`
}

// YDBTTLTierSpec moves data older than an interval to an external source or deletes it.
type YDBTTLTierSpec struct {
	// Interval is an ISO 8601 duration with whole-second precision.
	Interval string `json:"interval" yaml:"interval"`
	// ExternalSource is the external data source path. Empty means DELETE.
	ExternalSource string `json:"external_source,omitempty" yaml:"external_source,omitempty"`
}

// Clone returns an independent copy. Nil stays nil.
func (s *YDBColumnTableSpec) Clone() *YDBColumnTableSpec {
	if s == nil {
		return nil
	}
	out := *s
	out.HashColumns = slices.Clone(s.HashColumns)
	if s.TTL != nil {
		ttl := *s.TTL
		ttl.Tiers = slices.Clone(s.TTL.Tiers)
		out.TTL = &ttl
	}
	return &out
}
