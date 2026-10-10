// Package ydbcolumn reads and writes YDB column-table declarations for the
// sources, the reader and the renderers: the Go annotation attributes, the
// monitoring description and the TTL clause. The model is the YDB owner's
// [ydbschema.ColumnStore]; a row table has none.
package ydbcolumn

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"ptah.run/dialect/ydb/ydbschema"
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
	// AttributeTTL carries a JSON-encoded ydbschema.TieredTTL.
	AttributeTTL = "column_ttl"
)

// Parse reads column-table annotations. Storage must be selected explicitly;
// a hash key or a shard count alone must not change a row table's storage kind.
func Parse(values map[string]string) (*ydbschema.ColumnStore, error) {
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
	spec := &ydbschema.ColumnStore{}
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
		ttl, err := parseTTL(raw)
		if err != nil {
			return nil, err
		}
		spec.TTL = ttl
	}
	if err := ydbschema.CheckColumnStore(*spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// parseTTL reads the JSON object of the column_ttl attribute, with its unit
// written as YQL writes it.
func parseTTL(raw string) (*ydbschema.TieredTTL, error) {
	var ttl *ydbschema.TieredTTL
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ttl); err != nil {
		return nil, fmt.Errorf("%s: %w", AttributeTTL, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%s must contain exactly one JSON object", AttributeTTL)
	}
	if ttl == nil {
		return nil, fmt.Errorf("%s must be an object, not null", AttributeTTL)
	}
	unit, err := ydbttl.Unit(ttl.Unit)
	if err != nil {
		return nil, err
	}
	ttl.Unit = unit
	return ttl, nil
}
