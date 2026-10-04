package ydbindex

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
)

// Settings is the partitioning of a YDB global index's own table, and its read
// replicas, with every setting resolved: what the server holds, rather than
// what a declaration names.
//
// It is the one reading the renderer, the reader, the comparator and the
// planner share. A declaration and a description are each resolved into
// Settings and compared as Settings, so a declaration naming a setting at its
// default and one leaving it out are the same index, and the statement that
// changes an index is written from the same values the comparison saw.
type Settings struct {
	// BySize is AUTO_PARTITIONING_BY_SIZE.
	BySize bool
	// PartitionSizeMB is AUTO_PARTITIONING_PARTITION_SIZE_MB. It is zero
	// while BySize is false, because YDB keeps no size then.
	PartitionSizeMB uint64
	// ByLoad is AUTO_PARTITIONING_BY_LOAD.
	ByLoad bool
	// MinPartitions is AUTO_PARTITIONING_MIN_PARTITIONS_COUNT, at least 1.
	MinPartitions uint64
	// MaxPartitions is AUTO_PARTITIONING_MAX_PARTITIONS_COUNT, zero for no
	// maximum.
	MaxPartitions uint64
	// ReadReplicas is READ_REPLICAS_SETTINGS.
	ReadReplicas Replicas
}

// Replicas is an index's read replicas.
type Replicas struct {
	// PerAZ selects `PER_AZ`, Count replicas in every availability zone;
	// false selects `ANY_AZ`, Count replicas in all of them together.
	PerAZ bool
	// Count is the number of replicas; zero is none, whatever PerAZ says.
	Count uint64
}

// None reports an index with no read replicas.
func (r Replicas) None() bool { return r.Count == 0 }

// String writes the replicas as READ_REPLICAS_SETTINGS spells them.
func (r Replicas) String() string {
	mode := "ANY_AZ"
	if r.PerAZ {
		mode = "PER_AZ"
	}
	return fmt.Sprintf("%s:%d", mode, r.Count)
}

// The settings YDB gives a new global index, measured on 25.1.4.7 and
// 26.2.1.14 by describing the index's implementation table right after the
// index was built: split by size at 2048 MB, not by load, at least one
// partition, no maximum and no read replicas. An index does not take its
// table's settings: on a table created with AUTO_PARTITIONING_BY_LOAD =
// ENABLED and a minimum of 5, its index still holds these.
const (
	defaultPartitionSizeMB = 2048
	defaultMinPartitions   = 1
)

// DefaultSettings is what YDB gives a new global index.
func DefaultSettings() Settings {
	return Settings{BySize: true, PartitionSizeMB: defaultPartitionSizeMB, MinPartitions: defaultMinPartitions}
}

// errSizeWithoutSplitting is YDB's refusal of a partition size on an index
// that does not split by size, measured on 25.1.4.7 and 26.2.1.14 as `Auto
// partitioning partition size is set while auto partitioning by size is
// disabled`. A size the server cannot hold could never be read back, so
// declaring one would plan the same change on every run.
var errSizeWithoutSplitting = errors.New("auto_partitioning_partition_size_mb is set while auto_partitioning_by_size " +
	"is disabled, which YDB refuses (`Auto partitioning partition size is set while auto partitioning by size is disabled`)")

// Resolve reads a declaration as the settings an index takes from it: each
// setting the declaration names, and YDB's default for each it leaves out. A
// nil declaration resolves to [DefaultSettings]. A declaration YDB would
// refuse is an error saying why.
func Resolve(spec *ast.IndexPartitioningSpec) (Settings, error) {
	settings := DefaultSettings()
	if spec.IsZero() {
		return settings, nil
	}
	if spec.BySize != nil {
		settings.BySize = *spec.BySize
	}
	switch {
	case !settings.BySize && spec.PartitionSizeMB != 0:
		return Settings{}, errSizeWithoutSplitting
	case !settings.BySize:
		settings.PartitionSizeMB = 0
	case spec.PartitionSizeMB != 0:
		settings.PartitionSizeMB = spec.PartitionSizeMB
	}
	if spec.ByLoad != nil {
		settings.ByLoad = *spec.ByLoad
	}
	if spec.MinPartitions != 0 {
		settings.MinPartitions = spec.MinPartitions
	}
	settings.MaxPartitions = spec.MaxPartitions
	if spec.ReadReplicas != "" {
		replicas, err := ParseReplicas(spec.ReadReplicas)
		if err != nil {
			return Settings{}, err
		}
		settings.ReadReplicas = replicas
	}
	return settings, nil
}

// ParseReplicas reads READ_REPLICAS_SETTINGS as YDB takes it: `PER_AZ:<n>` or
// `ANY_AZ:<n>`, in either case. Measured on 25.1.4.7 and 26.2.1.14, the server
// takes `per_az:2` and refuses a list such as `PER_AZ:1,ANY_AZ:1` with `Wrong
// format for read replicas settings`, so this reads exactly one.
func ParseReplicas(value string) (Replicas, error) {
	mode, count, found := strings.Cut(strings.TrimSpace(value), ":")
	if !found {
		return Replicas{}, replicasError(value)
	}
	var replicas Replicas
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "PER_AZ":
		replicas.PerAZ = true
	case "ANY_AZ":
	default:
		return Replicas{}, replicasError(value)
	}
	n, err := strconv.ParseUint(strings.TrimSpace(count), 10, 64)
	if err != nil {
		return Replicas{}, replicasError(value)
	}
	replicas.Count = n
	return replicas, nil
}

func replicasError(value string) error {
	return fmt.Errorf("read replicas %q are not one YDB takes: write PER_AZ:<n> for n replicas in every "+
		"availability zone, or ANY_AZ:<n> for n in all of them together", value)
}

// Spec writes settings as the declaration of what differs from
// [DefaultSettings], which is what a reader reports for an index: nil for an
// index nobody tuned. Resolving the result gives the settings back.
func (s Settings) Spec() *ast.IndexPartitioningSpec {
	defaults := DefaultSettings()
	spec := &ast.IndexPartitioningSpec{
		MinPartitions: s.MinPartitions,
		MaxPartitions: s.MaxPartitions,
	}
	if s.BySize != defaults.BySize {
		spec.BySize = new(s.BySize)
	}
	if s.BySize && s.PartitionSizeMB != defaults.PartitionSizeMB {
		spec.PartitionSizeMB = s.PartitionSizeMB
	}
	if s.ByLoad != defaults.ByLoad {
		spec.ByLoad = new(s.ByLoad)
	}
	if s.MinPartitions == defaults.MinPartitions {
		spec.MinPartitions = 0
	}
	if !s.ReadReplicas.None() {
		spec.ReadReplicas = s.ReadReplicas.String()
	}
	if spec.IsZero() {
		return nil
	}
	return spec
}

// Equal reports whether two indexes hold the same settings. Read replicas of
// zero are none in either mode: YDB reports `PER_AZ:0` as a count of zero,
// and it reads the same as an index that never had replicas.
func (s Settings) Equal(other Settings) bool {
	sameReplicas := s.ReadReplicas == other.ReadReplicas || (s.ReadReplicas.None() && other.ReadReplicas.None())
	return sameReplicas &&
		s.BySize == other.BySize && s.PartitionSizeMB == other.PartitionSizeMB && s.ByLoad == other.ByLoad &&
		s.MinPartitions == other.MinPartitions && s.MaxPartitions == other.MaxPartitions
}

// ChangeRefusal says why an index holding current cannot take desired in
// place, and answers "" where it can. YDB has no way back to an index without
// a maximum partition count: measured on 25.1.4.7 and 26.2.1.14, `SET
// (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 0)` answers `Can't set max
// partition count to 0`, and `RESET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT)`
// answers `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT reset is not supported`. Such
// an index is rebuilt, which gives it the defaults, and the rest of desired
// is set on the new one.
func ChangeRefusal(desired, current Settings) string {
	if desired.MaxPartitions == 0 && current.MaxPartitions != 0 {
		return fmt.Sprintf("its maximum of %d partitions cannot be removed in place (`Can't set max partition "+
			"count to 0`, and no RESET)", current.MaxPartitions)
	}
	return ""
}

// Clause writes the settings of `ALTER TABLE ... ALTER INDEX ... SET (...)`
// that take an index holding current to desired, and nil where the two are
// [Settings.Equal]. A caller asks [ChangeRefusal] first; Clause writes no
// statement for a maximum it cannot remove.
//
// It names every setting desired holds rather than only the ones that differ,
// because setting one resets others. Measured on 25.1.4.7 and 26.2.1.14: on an
// index holding a minimum of 5 partitions, `SET (AUTO_PARTITIONING_BY_LOAD =
// ENABLED)` leaves a minimum of 1, and on one holding a size of 100 MB and a
// minimum of 6, `SET (AUTO_PARTITIONING_BY_SIZE = ENABLED)` leaves 2048 MB and
// 1. A setting named in the same statement keeps the value it names, whatever
// its place in the list, so a statement naming them all leaves exactly
// desired. Read replicas are named where desired has some, or where current
// has some for desired to remove: `PER_AZ:0` clears them, and RESET is refused.
func Clause(desired, current Settings) []string {
	if desired.Equal(current) {
		return nil
	}
	enabled := func(on bool) string {
		if on {
			return "ENABLED"
		}
		return "DISABLED"
	}
	settings := []string{"AUTO_PARTITIONING_BY_SIZE = " + enabled(desired.BySize)}
	if desired.BySize {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_PARTITION_SIZE_MB = %d", desired.PartitionSizeMB))
	}
	settings = append(settings,
		"AUTO_PARTITIONING_BY_LOAD = "+enabled(desired.ByLoad),
		fmt.Sprintf("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d", desired.MinPartitions),
	)
	if desired.MaxPartitions != 0 {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d", desired.MaxPartitions))
	}
	switch {
	case !desired.ReadReplicas.None():
		settings = append(settings, fmt.Sprintf("READ_REPLICAS_SETTINGS = %q", desired.ReadReplicas.String()))
	case !current.ReadReplicas.None():
		settings = append(settings, `READ_REPLICAS_SETTINGS = "PER_AZ:0"`)
	}
	return settings
}
