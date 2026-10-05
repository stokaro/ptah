// Package ydbcolumn owns column-table declarations shared by schema readers,
// renderers and migration planning. Row tables have a nil column-table spec.
package ydbcolumn

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbttl"
)

// Annotation attributes are shared by the Go parser, annotation metadata and exporter.
const (
	// AttributeStore selects ROW or COLUMN storage.
	AttributeStore = "store"
	// AttributeHash names the hash-partitioning columns as a comma-separated list.
	AttributeHash = "partition_by_hash"
	// AttributeShards names the initial column-shard count.
	AttributeShards = "column_shards"
	// AttributeTTL carries a JSON-encoded YDBTieredTTLSpec.
	AttributeTTL = "column_ttl"
)

// Parse reads column-table annotations. Storage must be selected explicitly;
// a hash key or a shard count alone must not change a row table's storage kind.
func Parse(values map[string]string) (*ast.YDBColumnTableSpec, error) {
	if value, present := values[AttributeStore]; present && strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("store must be ROW or COLUMN")
	}
	store := strings.ToUpper(strings.TrimSpace(values[AttributeStore]))
	if store != "COLUMN" {
		if store != "" && store != "ROW" {
			return nil, fmt.Errorf("store %q must be ROW or COLUMN", values[AttributeStore])
		}
		for _, name := range []string{AttributeHash, AttributeShards, AttributeTTL} {
			if _, present := values[name]; present {
				return nil, fmt.Errorf("%s requires store=column", name)
			}
		}
		return nil, nil
	}
	spec := &ast.YDBColumnTableSpec{}
	if raw, present := values[AttributeHash]; present {
		for name := range strings.SplitSeq(raw, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, fmt.Errorf("%s contains an empty column name", AttributeHash)
			}
			spec.HashColumns = append(spec.HashColumns, name)
		}
	}
	if raw, present := values[AttributeShards]; present {
		count, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || count == 0 {
			return nil, fmt.Errorf("%s must be a positive 32-bit integer", AttributeShards)
		}
		spec.Partitions = count
	}
	if raw, present := values[AttributeTTL]; present {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&spec.TTL); err != nil {
			return nil, fmt.Errorf("%s: %w", AttributeTTL, err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return nil, fmt.Errorf("%s must contain exactly one JSON object", AttributeTTL)
		}
		if spec.TTL == nil {
			return nil, fmt.Errorf("%s must be an object, not null", AttributeTTL)
		}
	}
	if err := Validate(spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// Validate checks facts independent of the table's columns and server capabilities.
func Validate(spec *ast.YDBColumnTableSpec) error {
	if spec == nil {
		return nil
	}
	seen := make(map[string]bool, len(spec.HashColumns))
	for _, name := range spec.HashColumns {
		if strings.TrimSpace(name) == "" || seen[name] {
			return fmt.Errorf("hash partitioning column %q is empty or repeated", name)
		}
		seen[name] = true
	}
	if spec.Partitions > 1<<32-1 {
		return fmt.Errorf("column shard count exceeds a 32-bit integer")
	}
	return validateTTL(spec.TTL)
}

func validateTTL(spec *ast.YDBTieredTTLSpec) error {
	if spec == nil {
		return nil
	}
	if strings.TrimSpace(spec.Column) == "" || len(spec.Tiers) == 0 {
		return fmt.Errorf("tiered TTL requires a column and at least one tier")
	}
	if _, err := ydbttl.Unit(spec.Unit); err != nil {
		return err
	}
	var previous uint64
	hasEviction := false
	for position, tier := range spec.Tiers {
		seconds, err := ydbttl.IntervalSeconds(tier.Interval)
		if err != nil {
			return fmt.Errorf("TTL tier %d: %w", position+1, err)
		}
		if position > 0 && seconds <= previous {
			return fmt.Errorf("TTL tier intervals must increase strictly")
		}
		if tier.ExternalSource == "" && position != len(spec.Tiers)-1 {
			return fmt.Errorf("only the last TTL tier may delete data")
		}
		if tier.ExternalSource != "" && !strings.HasPrefix(tier.ExternalSource, "/") {
			return fmt.Errorf("TTL tier external source must be an absolute database path")
		}
		hasEviction = hasEviction || tier.ExternalSource != ""
		previous = seconds
	}
	if !hasEviction {
		return fmt.Errorf("column_ttl requires an eviction tier; use row_deletion_policy for deletion alone")
	}
	return nil
}
