package ydbcolumn

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
)

// Description preserves the column-table properties omitted by DescribeTable.
type Description struct {
	// Spec contains the storage kind, hash key and shard count.
	Spec *ast.YDBColumnTableSpec
	// Indexes contains the local indexes DescribeTable omits entirely.
	Indexes []LocalIndex
	// PrimaryKey is the ordered key used to check the table-service description.
	PrimaryKey []string
}

// LocalIndex is a column-table index read from the scheme description.
type LocalIndex struct {
	// Name is the index name within its table.
	Name string
	// Method is bloom_filter, bloom_ngram_filter or min_max.
	Method string
	// Columns is the ordered indexed column list.
	Columns []string
	// Options contains the index's declarative settings.
	Options map[string]string
}

type columnDescription struct {
	TTLSettings struct {
		Version  string
		Enabled  json.RawMessage
		Disabled *struct{}
	}
	IsRestore     bool
	StorageConfig struct{ DataChannelCount uint64 }

	Name             string
	ColumnShardCount uint64
	Schema           columnSchema
	Sharding         struct {
		ColumnShards []string
		HashSharding struct {
			Function uint64
			Columns  []string
		}
	}
}

func (s *columnDescription) UnmarshalJSON(data []byte) error {
	type plain columnDescription
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode((*plain)(s)); err != nil {
		return fmt.Errorf("column description: %w", err)
	}
	return nil
}

// Runtime counters are accepted explicitly; schema settings not represented by
// the model must never disappear during a read/export/apply cycle.
type columnSchema struct {
	Columns            []map[string]json.RawMessage
	Indexes            []json.RawMessage
	ColumnFamilies     []map[string]json.RawMessage
	NextColumnID       uint64
	NextColumnFamilyID uint64
	KeyColumnNames     []string
	Version            string
	Options            struct{ SchemeNeedActualization bool }
}

func (s *columnSchema) UnmarshalJSON(data []byte) error {
	type plain columnSchema
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode((*plain)(s)); err != nil {
		return fmt.Errorf("column schema: %w", err)
	}
	return nil
}

// Decode reads a viewer/json/describe response from YDB 25.1 or 26.2.
// The public table-service description omits local indexes on both lines.
// Unknown index settings are refused instead of producing a smaller index.
func Decode(body []byte, path string) (*Description, error) {
	var page struct {
		Status          string
		Path            string
		PathDescription struct {
			ColumnTableDescription *columnDescription
		}
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("column table description: %w", err)
	}
	if page.Status != "StatusSuccess" || page.Path != path || page.PathDescription.ColumnTableDescription == nil {
		return nil, fmt.Errorf("column table %s: monitoring returned status %q and path %q without a matching description", path, page.Status, page.Path)
	}
	table := page.PathDescription.ColumnTableDescription
	if table.StorageConfig.DataChannelCount != 64 {
		return nil, fmt.Errorf("column table %s: non-default storage channels are not modeled", path)
	}
	if table.ColumnShardCount == 0 || table.Sharding.HashSharding.Function != 2 || len(table.Sharding.HashSharding.Columns) == 0 {
		return nil, fmt.Errorf("column table %s: missing shard count or unsupported hash sharding", path)
	}
	columns, err := columnNames(table.Schema.Columns)
	if err != nil {
		return nil, err
	}
	for _, family := range table.Schema.ColumnFamilies {
		// The old line reports its implicit default family. A configured
		// family carries compression or storage settings and cannot be dropped.
		if string(family["Name"]) != `"default"` || len(family) > 2 {
			return nil, fmt.Errorf("column table %s: configured column families are not modeled", path)
		}
	}
	result := &Description{PrimaryKey: slices.Clone(table.Schema.KeyColumnNames), Spec: &ast.YDBColumnTableSpec{HashColumns: table.Sharding.HashSharding.Columns, Partitions: table.ColumnShardCount}}
	if table.TTLSettings.Enabled != nil {
		policy, err := decodeColumnTTL(table.TTLSettings.Enabled)
		if err != nil {
			return nil, err
		}
		result.Spec.TTL = policy
	}
	for _, raw := range table.Schema.Indexes {
		index, err := decodeIndex(raw, columns)
		if err != nil {
			return nil, fmt.Errorf("column table %s: %w", path, err)
		}
		result.Indexes = append(result.Indexes, index)
	}
	slices.SortFunc(result.Indexes, func(a, b LocalIndex) int { return cmp.Compare(a.Name, b.Name) })
	return result, nil
}

func columnNames(columns []map[string]json.RawMessage) (map[uint64]string, error) {
	result := make(map[uint64]string, len(columns))
	for _, column := range columns {
		var id uint64
		var name string
		if err := json.Unmarshal(column["Id"], &id); err != nil {
			return nil, fmt.Errorf("column id: %w", err)
		}
		if err := json.Unmarshal(column["Name"], &name); err != nil {
			return nil, fmt.Errorf("column name: %w", err)
		}
		if name == "" || id == 0 || result[id] != "" {
			return nil, fmt.Errorf("empty or repeated column identity")
		}
		for key, value := range column {
			switch key {
			case "Id", "Name", "Type", "TypeId", "NotNull", "TypeInfo":
			case "ColumnFamilyId":
				if string(value) != "0" {
					return nil, fmt.Errorf("column %s belongs to a configured column family", name)
				}
			case "StorageId":
				if string(value) != `""` {
					return nil, fmt.Errorf("column %s has a configured storage id", name)
				}
			case "DefaultValue":
				if string(value) != "{}" {
					return nil, fmt.Errorf("column %s has a default value not modeled for column tables", name)
				}
			default:
				return nil, fmt.Errorf("column %s carries unmodeled setting %s", name, key)
			}
		}
		result[id] = name
	}
	return result, nil
}

type localIndexDescription struct {
	ID                    uint64
	Name                  string
	StorageID             string
	ClassName             string
	InheritPortionStorage bool
	BloomFilter           *bloomDescription
	BloomNGrammFilter     *ngramDescription
	MinMaxIndex           *minMaxDescription
}

type namedClass struct{ ClassName string }
type bloomDescription struct {
	DataExtractor            namedClass
	BitsStorage              namedClass
	FalsePositiveProbability *float64
	ColumnIDs                []uint64
}
type ngramDescription struct {
	DataExtractor            namedClass
	BitsStorage              namedClass
	FalsePositiveProbability *float64
	ColumnID                 uint64
	NGrammSize               *uint64
	CaseSensitive            *bool
}
type minMaxDescription struct {
	ColumnID      uint64
	DataExtractor namedClass
}

func decodeIndex(raw []byte, columns map[uint64]string) (LocalIndex, error) {
	var index localIndexDescription
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&index); err != nil {
		return LocalIndex{}, fmt.Errorf("local index: %w", err)
	}
	if index.Name == "" {
		return LocalIndex{}, fmt.Errorf("local index has no name")
	}
	if err := localIndexStorage(index); err != nil {
		return LocalIndex{}, err
	}
	result := LocalIndex{Name: index.Name, Options: make(map[string]string)}
	var ids []uint64
	var extractor, bits string
	switch {
	case index.ClassName == "BLOOM_FILTER" && index.BloomFilter != nil && index.BloomNGrammFilter == nil && index.MinMaxIndex == nil:
		v := index.BloomFilter
		result.Method = "bloom_filter"
		ids = v.ColumnIDs
		result.Options["false_positive_probability"] = strconv.FormatFloat(defaultFloat(v.FalsePositiveProbability, 0.1), 'g', -1, 64)
		extractor, bits = v.DataExtractor.ClassName, v.BitsStorage.ClassName
	case index.ClassName == "BLOOM_NGRAMM_FILTER" && index.BloomNGrammFilter != nil && index.BloomFilter == nil && index.MinMaxIndex == nil:
		v := index.BloomNGrammFilter
		result.Method = "bloom_ngram_filter"
		ids = []uint64{v.ColumnID}
		result.Options["false_positive_probability"] = strconv.FormatFloat(defaultFloat(v.FalsePositiveProbability, 0.1), 'g', -1, 64)
		result.Options["ngram_size"] = strconv.FormatUint(defaultUint(v.NGrammSize, 3), 10)
		result.Options["case_sensitive"] = strconv.FormatBool(defaultBool(v.CaseSensitive, true))
		extractor, bits = v.DataExtractor.ClassName, v.BitsStorage.ClassName
	case index.ClassName == "MIN_MAX" && index.MinMaxIndex != nil && index.BloomFilter == nil && index.BloomNGrammFilter == nil:
		result.Method = "min_max"
		ids = []uint64{index.MinMaxIndex.ColumnID}
		extractor = index.MinMaxIndex.DataExtractor.ClassName
	default:
		return LocalIndex{}, fmt.Errorf("local index %q uses unmodeled class %q", index.Name, index.ClassName)
	}
	if unsupportedLocalEncoding(extractor, bits) {
		return LocalIndex{}, fmt.Errorf("local index %q uses an unmodeled data extractor or bits storage", index.Name)
	}
	if err := resolveLocalColumns(&result, ids, columns); err != nil {
		return LocalIndex{}, err
	}
	kind, err := ydbindex.KindOf(result.Method)
	if err != nil {
		return LocalIndex{}, err
	}
	result.Options, err = ydbindex.ResolveLocal(kind, result.Options)
	if err != nil {
		return LocalIndex{}, err
	}
	return result, nil
}

func defaultFloat(value *float64, fallback float64) float64 {
	if value == nil {
		return fallback
	}
	return *value
}
func defaultUint(value *uint64, fallback uint64) uint64 {
	if value == nil {
		return fallback
	}
	return *value
}
func defaultBool(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

type columnTTLDescription struct {
	ColumnName         string
	ColumnUnit         uint64
	ExpireAfterSeconds uint64
	Tiers              []struct {
		ApplyAfterSeconds      uint64
		Delete                 *struct{}
		EvictToExternalStorage *struct{ Storage string }
	}
}

func decodeColumnTTL(raw []byte) (*ast.YDBTieredTTLSpec, error) {
	var description columnTTLDescription
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&description); err != nil {
		return nil, fmt.Errorf("column TTL: %w", err)
	}
	if len(description.Tiers) == 0 || (len(description.Tiers) == 1 && description.Tiers[0].Delete != nil) {
		return nil, nil
	}
	units := []string{"", "SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS"}
	if description.ColumnUnit >= uint64(len(units)) {
		return nil, fmt.Errorf("unknown column TTL unit %d", description.ColumnUnit)
	}
	policy := &ast.YDBTieredTTLSpec{Column: description.ColumnName, Unit: units[description.ColumnUnit]}
	for _, tier := range description.Tiers {
		if (tier.Delete == nil) == (tier.EvictToExternalStorage == nil) {
			return nil, fmt.Errorf("TTL tier must have exactly one action")
		}
		out := ast.YDBTTLTierSpec{Interval: "PT" + strconv.FormatUint(tier.ApplyAfterSeconds, 10) + "S"}
		if tier.EvictToExternalStorage != nil {
			out.ExternalSource = tier.EvictToExternalStorage.Storage
			if out.ExternalSource == "" {
				return nil, fmt.Errorf("TTL tier external source is empty")
			}
		}
		policy.Tiers = append(policy.Tiers, out)
	}
	if err := validateTTL(policy); err != nil {
		return nil, err
	}
	return policy, nil
}

// CREATE TABLE stores bloom indexes in __DEFAULT without inheritance; ALTER
// TABLE uses the same storage with inheritance. Both are representable because
// the reader rejects non-default table and column storage.
func localIndexStorage(index localIndexDescription) error {
	if index.ClassName == "MIN_MAX" {
		if index.StorageID != "__LOCAL_METADATA" || index.InheritPortionStorage {
			return fmt.Errorf("local index %q has unsupported storage settings", index.Name)
		}
	} else if index.StorageID != "__DEFAULT" && (index.StorageID != "" || !index.InheritPortionStorage) {
		return fmt.Errorf("local index %q has unsupported storage settings", index.Name)
	}
	return nil
}

func resolveLocalColumns(result *LocalIndex, ids []uint64, columns map[uint64]string) error {
	for _, id := range ids {
		name := columns[id]
		if name == "" {
			return fmt.Errorf("local index %q references unknown column id %d", result.Name, id)
		}
		result.Columns = append(result.Columns, name)
	}
	if len(result.Columns) == 0 {
		return fmt.Errorf("local index %q has no columns", result.Name)
	}
	return nil
}

func unsupportedLocalEncoding(extractor, bits string) bool {
	return (extractor != "" && extractor != "DEFAULT") || (bits != "" && bits != "SIMPLE_STRING")
}
