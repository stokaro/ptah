package ydbschema

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// IndexPartitioningKind identifies how a YDB global index's own table splits
// into partitions and how many read replicas it keeps, as YDB's `ALTER TABLE
// ... ALTER INDEX ... SET (...)` takes them.
//
// A setting a declaration leaves out keeps what the index holds: a new index
// takes it from YDB, and a plan never changes it. An index does not take its
// table's settings.
const IndexPartitioningKind schemaext.Kind = "ptah.run/ydb/index-partitioning"

// IndexPartitioning is a global index's settings. Each field names the
// setting it carries, and a field left at its zero value states nothing; see
// [TablePartitioning], whose first six settings these are.
type IndexPartitioning struct {
	// BySize is AUTO_PARTITIONING_BY_SIZE.
	BySize *bool `json:"by_size,omitempty"`
	// PartitionSizeMB is AUTO_PARTITIONING_PARTITION_SIZE_MB.
	PartitionSizeMB uint64 `json:"partition_size_mb,omitempty"`
	// ByLoad is AUTO_PARTITIONING_BY_LOAD.
	ByLoad *bool `json:"by_load,omitempty"`
	// MinPartitions is AUTO_PARTITIONING_MIN_PARTITIONS_COUNT.
	MinPartitions uint64 `json:"min_partitions,omitempty"`
	// MaxPartitions is AUTO_PARTITIONING_MAX_PARTITIONS_COUNT.
	MaxPartitions uint64 `json:"max_partitions,omitempty"`
	// ReadReplicas is READ_REPLICAS_SETTINGS in capitals, `PER_AZ:<n>` or
	// `ANY_AZ:<n>`; a count of zero states none.
	ReadReplicas string `json:"read_replicas,omitempty"`
}

// Clone returns independent settings: no switch is shared.
func (p IndexPartitioning) Clone() IndexPartitioning {
	if p.BySize != nil {
		p.BySize = new(*p.BySize)
	}
	if p.ByLoad != nil {
		p.ByLoad = new(*p.ByLoad)
	}
	return p
}

// IsZero reports whether the settings state nothing.
func (p IndexPartitioning) IsZero() bool { return p.Equal(IndexPartitioning{}) }

// Equal compares settings by value.
func (p IndexPartitioning) Equal(other IndexPartitioning) bool {
	return sameSwitch(p.BySize, other.BySize) && p.PartitionSizeMB == other.PartitionSizeMB &&
		sameSwitch(p.ByLoad, other.ByLoad) && p.MinPartitions == other.MinPartitions &&
		p.MaxPartitions == other.MaxPartitions && p.ReadReplicas == other.ReadReplicas
}

// DesiredIndexPartitioning is the settings a declaration states for one
// global index. A declaration stating none asks for nothing: the index keeps
// every setting it holds.
type DesiredIndexPartitioning struct {
	IndexPartitioning
}

// ObservedIndexPartitioning is what a read found an index holds, written as
// the settings that differ from what YDB's documentation gives a new index:
// split by size at 2048 MB, not by load, at least one partition, no maximum
// and no read replicas. An index holding all of those has no value.
type ObservedIndexPartitioning struct {
	IndexPartitioning
}

// Kind returns the owned index partitioning identity.
func (*DesiredIndexPartitioning) Kind() schemaext.Kind { return IndexPartitioningKind }

// Kind returns the owned index partitioning identity.
func (*ObservedIndexPartitioning) Kind() schemaext.Kind { return IndexPartitioningKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredIndexPartitioning) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredIndexPartitioning.Clone] without the interface. A nil
// receiver returns nil.
func (v *DesiredIndexPartitioning) Copy() *DesiredIndexPartitioning {
	if v == nil {
		return nil
	}
	return &DesiredIndexPartitioning{IndexPartitioning: v.IndexPartitioning.Clone()}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedIndexPartitioning) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedIndexPartitioning.Clone] without the interface. A nil
// receiver returns nil.
func (v *ObservedIndexPartitioning) Copy() *ObservedIndexPartitioning {
	if v == nil {
		return nil
	}
	return &ObservedIndexPartitioning{IndexPartitioning: v.IndexPartitioning.Clone()}
}

// Equal compares declarations by value.
func (v *DesiredIndexPartitioning) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredIndexPartitioning)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.IndexPartitioning.Equal(w.IndexPartitioning)
}

// Equal compares observations by value.
func (v *ObservedIndexPartitioning) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedIndexPartitioning)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.IndexPartitioning.Equal(w.IndexPartitioning)
}

// Desired captures the observation as a declaration naming the same
// settings. A nil receiver remains nil.
func (v *ObservedIndexPartitioning) Desired() *DesiredIndexPartitioning {
	if v == nil {
		return nil
	}
	return &DesiredIndexPartitioning{IndexPartitioning: v.IndexPartitioning.Clone()}
}

// ValidateDesiredIndexPartitioning refuses read replicas not spelled
// `PER_AZ:<n>` or `ANY_AZ:<n>` in capitals. Nil is invalid. A combination YDB
// refuses is refused where the settings are resolved against what the index
// holds. Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredIndexPartitioning(v *DesiredIndexPartitioning) error {
	if v == nil {
		return partitioningValidation(IndexPartitioningKind, schemaext.Desired,
			fmt.Errorf("%w: nil YDB index partitioning declaration", schemaext.ErrInvalidValue))
	}
	return partitioningValidation(IndexPartitioningKind, schemaext.Desired, validateReplicas(v.ReadReplicas))
}

// ValidateObservedIndexPartitioning refuses what
// [ValidateDesiredIndexPartitioning] refuses. Nil is invalid.
func ValidateObservedIndexPartitioning(v *ObservedIndexPartitioning) error {
	if v == nil {
		return partitioningValidation(IndexPartitioningKind, schemaext.Observed,
			fmt.Errorf("%w: nil YDB index partitioning observation", schemaext.ErrInvalidValue))
	}
	return partitioningValidation(IndexPartitioningKind, schemaext.Observed, validateReplicas(v.ReadReplicas))
}
