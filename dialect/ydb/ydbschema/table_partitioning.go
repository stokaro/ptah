package ydbschema

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
)

// TablePartitioningKind identifies a YDB row table's settings as its
// `CREATE TABLE ... WITH (...)` takes them: how it splits into partitions, how
// many read replicas it keeps, whether it keeps a bloom filter of its keys,
// and the partitions it starts with.
//
// A setting a declaration leaves out keeps what the table holds: a new table
// takes it from the cluster's table profile, and a plan never changes it. So
// removing the whole declaration changes nothing either.
const TablePartitioningKind schemaext.Kind = "ptah.run/ydb/table-partitioning"

// TablePartitioning is a row table's settings. Each field names the setting
// it carries, and a field left at its zero value states nothing. The switches
// are pointers because false is a value of its own, and a zero count or size
// states nothing because YDB refuses every count of zero.
//
// UniformPartitions and PartitionAtKeys are the table's starting layout. YDB
// takes them only when it creates the table (`UNIFORM_PARTITIONS alter is not
// supported`) and keeps no record of either, so only a declaration states
// them; a read never does.
type TablePartitioning struct {
	// BySize is AUTO_PARTITIONING_BY_SIZE: whether a partition that grows
	// past PartitionSizeMB splits.
	BySize *bool `json:"by_size,omitempty"`
	// PartitionSizeMB is AUTO_PARTITIONING_PARTITION_SIZE_MB, the size at
	// which a partition splits.
	PartitionSizeMB uint64 `json:"partition_size_mb,omitempty"`
	// ByLoad is AUTO_PARTITIONING_BY_LOAD: whether a busy partition splits.
	ByLoad *bool `json:"by_load,omitempty"`
	// MinPartitions is AUTO_PARTITIONING_MIN_PARTITIONS_COUNT.
	MinPartitions uint64 `json:"min_partitions,omitempty"`
	// MaxPartitions is AUTO_PARTITIONING_MAX_PARTITIONS_COUNT.
	MaxPartitions uint64 `json:"max_partitions,omitempty"`
	// ReadReplicas is READ_REPLICAS_SETTINGS in capitals: `PER_AZ:<n>` for n
	// replicas in every availability zone, or `ANY_AZ:<n>` for n in all of
	// them together. A count of zero states none, which removes the replicas
	// a table holds.
	ReadReplicas string `json:"read_replicas,omitempty"`
	// KeyBloomFilter is KEY_BLOOM_FILTER: whether the table keeps a bloom
	// filter of its keys.
	KeyBloomFilter *bool `json:"key_bloom_filter,omitempty"`
	// UniformPartitions is UNIFORM_PARTITIONS: the table starts with this
	// many partitions, splitting the range of its first key column evenly.
	UniformPartitions uint64 `json:"uniform_partitions,omitempty"`
	// PartitionAtKeys is PARTITION_AT_KEYS: the table starts split before
	// each of these keys. Each split point holds the values of the leading
	// key columns, as text, in key order; the order of the points is kept.
	PartitionAtKeys [][]string `json:"partition_at_keys,omitempty"`
}

// Clone returns independent settings: no switch or split point is shared.
func (p TablePartitioning) Clone() TablePartitioning {
	if p.BySize != nil {
		p.BySize = new(*p.BySize)
	}
	if p.ByLoad != nil {
		p.ByLoad = new(*p.ByLoad)
	}
	if p.KeyBloomFilter != nil {
		p.KeyBloomFilter = new(*p.KeyBloomFilter)
	}
	if p.PartitionAtKeys != nil {
		points := make([][]string, len(p.PartitionAtKeys))
		for i, point := range p.PartitionAtKeys {
			points[i] = slices.Clone(point)
		}
		p.PartitionAtKeys = points
	}
	return p
}

// IsZero reports whether the settings state nothing.
func (p TablePartitioning) IsZero() bool { return p.Equal(TablePartitioning{}) }

// Equal compares settings by value: two switches are equal when both are
// absent or both hold the same value.
func (p TablePartitioning) Equal(other TablePartitioning) bool {
	return sameSwitch(p.BySize, other.BySize) && p.PartitionSizeMB == other.PartitionSizeMB &&
		sameSwitch(p.ByLoad, other.ByLoad) && p.MinPartitions == other.MinPartitions &&
		p.MaxPartitions == other.MaxPartitions && p.ReadReplicas == other.ReadReplicas &&
		sameSwitch(p.KeyBloomFilter, other.KeyBloomFilter) && p.UniformPartitions == other.UniformPartitions &&
		slices.EqualFunc(p.PartitionAtKeys, other.PartitionAtKeys, slices.Equal[[]string]) &&
		(p.PartitionAtKeys == nil) == (other.PartitionAtKeys == nil)
}

func sameSwitch(a, b *bool) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

// DesiredTablePartitioning is the settings a declaration states for one
// table. A source that can declare them and states none asks for nothing: the
// table keeps every setting it holds.
type DesiredTablePartitioning struct {
	TablePartitioning
}

// ObservedTablePartitioning is what a read found a table holds, written as the
// settings that differ from what YDB's documentation gives a new table: split
// by size at 2048 MB, not by load, at least one partition, no maximum, no read
// replicas and no key bloom filter. A table holding all of those has no
// value. It never states a starting layout.
type ObservedTablePartitioning struct {
	TablePartitioning
}

// Kind returns the owned table partitioning identity.
func (*DesiredTablePartitioning) Kind() schemaext.Kind { return TablePartitioningKind }

// Kind returns the owned table partitioning identity.
func (*ObservedTablePartitioning) Kind() schemaext.Kind { return TablePartitioningKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredTablePartitioning) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredTablePartitioning.Clone] without the interface. A nil
// receiver returns nil.
func (v *DesiredTablePartitioning) Copy() *DesiredTablePartitioning {
	if v == nil {
		return nil
	}
	return &DesiredTablePartitioning{TablePartitioning: v.TablePartitioning.Clone()}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedTablePartitioning) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedTablePartitioning.Clone] without the interface. A nil
// receiver returns nil.
func (v *ObservedTablePartitioning) Copy() *ObservedTablePartitioning {
	if v == nil {
		return nil
	}
	return &ObservedTablePartitioning{TablePartitioning: v.TablePartitioning.Clone()}
}

// Equal compares declarations by value.
func (v *DesiredTablePartitioning) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredTablePartitioning)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.TablePartitioning.Equal(w.TablePartitioning)
}

// Equal compares observations by value.
func (v *ObservedTablePartitioning) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedTablePartitioning)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.TablePartitioning.Equal(w.TablePartitioning)
}

// Desired captures the observation as a declaration naming the same
// settings, which resolves to what the table holds wherever the table holds a
// setting at YDB's documented default. A nil receiver remains nil.
func (v *ObservedTablePartitioning) Desired() *DesiredTablePartitioning {
	if v == nil {
		return nil
	}
	return &DesiredTablePartitioning{TablePartitioning: v.TablePartitioning.Clone()}
}

// ValidateDesiredTablePartitioning refuses a declaration no source writes:
// read replicas not spelled `PER_AZ:<n>` or `ANY_AZ:<n>` in capitals, a
// split point holding no value, a split point list written out empty, and
// text that is not valid UTF-8 or holds NUL. Nil is invalid. Errors are
// schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
//
// A combination YDB refuses, such as a partition size on a table that does
// not split by size or two starting layouts, is a valid value: it is refused
// where the settings are resolved against what the table holds, with YDB's
// reason.
func ValidateDesiredTablePartitioning(v *DesiredTablePartitioning) error {
	if v == nil {
		return partitioningValidation(TablePartitioningKind, schemaext.Desired,
			fmt.Errorf("%w: nil YDB table partitioning declaration", schemaext.ErrInvalidValue))
	}
	return partitioningValidation(TablePartitioningKind, schemaext.Desired, validateTablePartitioning(v.TablePartitioning))
}

// ValidateObservedTablePartitioning refuses what
// [ValidateDesiredTablePartitioning] refuses, and a starting layout, which no
// read finds. Nil is invalid.
func ValidateObservedTablePartitioning(v *ObservedTablePartitioning) error {
	if v == nil {
		return partitioningValidation(TablePartitioningKind, schemaext.Observed,
			fmt.Errorf("%w: nil YDB table partitioning observation", schemaext.ErrInvalidValue))
	}
	err := validateTablePartitioning(v.TablePartitioning)
	if err == nil && (v.UniformPartitions != 0 || v.PartitionAtKeys != nil) {
		err = fmt.Errorf("%w: a read finds no starting layout, which YDB keeps no record of", schemaext.ErrInvalidValue)
	}
	return partitioningValidation(TablePartitioningKind, schemaext.Observed, err)
}

func validateTablePartitioning(p TablePartitioning) error {
	if err := validateReplicas(p.ReadReplicas); err != nil {
		return err
	}
	if p.PartitionAtKeys != nil && len(p.PartitionAtKeys) == 0 {
		return fmt.Errorf("%w: partition_at_keys lists no split point", schemaext.ErrInvalidValue)
	}
	for _, point := range p.PartitionAtKeys {
		if len(point) == 0 {
			return fmt.Errorf("%w: a split point of partition_at_keys holds no value", schemaext.ErrInvalidValue)
		}
		for _, value := range point {
			if err := schemaext.ValidText("partition_at_keys value", value); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateReplicas refuses read replicas not spelled as a model holds them:
// empty, or one mode in capitals, a colon and a count in decimal.
func validateReplicas(replicas string) error {
	if replicas == "" {
		return nil
	}
	mode, count, found := strings.Cut(replicas, ":")
	n, err := strconv.ParseUint(count, 10, 64)
	if !found || (mode != "PER_AZ" && mode != "ANY_AZ") || err != nil || strconv.FormatUint(n, 10) != count {
		return fmt.Errorf("%w: read replicas %q are not PER_AZ:<n> or ANY_AZ:<n>", schemaext.ErrInvalidValue, replicas)
	}
	return nil
}

func partitioningValidation(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}
