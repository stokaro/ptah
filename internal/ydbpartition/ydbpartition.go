// Package ydbpartition holds YDB's auto partitioning settings -- when a table
// splits a partition and how many partitions it keeps -- and its read
// replicas, with the one reading of them that the renderer, the reader, the
// comparator and the planner share.
//
// A row table carries these settings, and so does the table YDB keeps each
// global index in. internal/ydbindex reads an index's declaration into
// [Settings]; this package reads a table's into [TableSettings], which adds
// what only a table has: its key bloom filter, and the partitions it is
// created with ([LayoutClause]).
//
// A setting a declaration leaves out keeps what the table holds. YDB gives a
// new table the settings of the cluster's table profile, not one fixed set:
// measured on 25.1.4.7 and 26.2.1.14, a table created after a dynamic
// configuration replaced the cluster's holds AUTO_PARTITIONING_BY_SIZE =
// DISABLED, where one created before it holds ENABLED at 2048 MB. So a
// declaration is resolved over the settings the table holds ([Declared.Over]),
// and removing a declaration changes nothing. A description is resolved the
// same way over [DefaultSettings], the baseline a reader writes its report
// against, and the statement that changes a table is written from the same
// values the comparison saw.
package ydbpartition

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Settings is how a YDB table splits into partitions, and its read replicas,
// with every setting resolved: what the server holds, rather than what a
// declaration names.
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

// Replicas is a table's read replicas.
type Replicas struct {
	// PerAZ selects `PER_AZ`, Count replicas in every availability zone;
	// false selects `ANY_AZ`, Count replicas in all of them together.
	PerAZ bool
	// Count is the number of replicas; zero is none, whatever PerAZ says.
	Count uint64
}

// None reports a table with no read replicas.
func (r Replicas) None() bool { return r.Count == 0 }

// String writes the replicas as READ_REPLICAS_SETTINGS spells them.
func (r Replicas) String() string {
	mode := "ANY_AZ"
	if r.PerAZ {
		mode = "PER_AZ"
	}
	return fmt.Sprintf("%s:%d", mode, r.Count)
}

// Equal reports whether two sets of replicas are the same. A count of zero is
// none in either mode: YDB reports `PER_AZ:0` as a count of zero, and it reads
// the same as a table that never had replicas.
func (r Replicas) Equal(other Replicas) bool {
	return r == other || (r.None() && other.None())
}

// The settings YDB's documentation gives a new table and a new global index,
// measured on 25.1.4.7 and 26.2.1.14 by describing each right after it was
// created on a cluster with no dynamic configuration: split by size at 2048
// MB, not by load, at least one partition, no maximum and no read replicas. An
// index does not take its table's settings: on a table created with
// AUTO_PARTITIONING_BY_LOAD = ENABLED and a minimum of 5, its index still holds
// these, and so does an index created under a configuration whose new tables
// do not split by size.
const (
	defaultPartitionSizeMB = 2048
	defaultMinPartitions   = 1
)

// DefaultSettings is what YDB's documentation gives a new table or a new
// global index. It is the baseline a reader writes its report against (see
// [Settings.Declared]) and the one [Declared.Held] reads a report over. It is
// never what a setting a declaration leaves out means: that setting keeps
// what the table holds (see [Declared.Over]), because the cluster's table
// profile, not this set, decides what a new table gets.
func DefaultSettings() Settings {
	return Settings{BySize: true, PartitionSizeMB: defaultPartitionSizeMB, MinPartitions: defaultMinPartitions}
}

// errSizeWithoutSplitting is YDB's refusal of a partition size on a table that
// does not split by size, measured on 25.1.4.7 and 26.2.1.14 as `Auto
// partitioning partition size is set while auto partitioning by size is
// disabled`, at CREATE TABLE and at ALTER alike. A size the server cannot hold
// could never be read back, so declaring one would plan the same change on
// every run.
var errSizeWithoutSplitting = errors.New("auto_partitioning_partition_size_mb is set while auto_partitioning_by_size " +
	"is disabled, which YDB refuses (`Auto partitioning partition size is set while auto partitioning by size is disabled`)")

// Declared is what a declaration names of the settings a table and an index
// share. A nil switch, a zero count or size and an empty ReadReplicas are
// settings it leaves out; `PER_AZ:0` is read replicas it declares as none.
type Declared struct {
	BySize          *bool
	PartitionSizeMB uint64
	ByLoad          *bool
	MinPartitions   uint64
	MaxPartitions   uint64
	ReadReplicas    string
}

// Over reads a declaration against the settings a table or an index holds:
// each setting it names, and the held value of each it leaves out, so a plan
// changes only what the declaration names. A declaration YDB would refuse is
// an error saying why.
//
// A size declared without AUTO_PARTITIONING_BY_SIZE turns splitting by size
// on, as YDB does: measured on 25.1.4.7 and 26.2.1.14, `CREATE TABLE ... WITH
// (AUTO_PARTITIONING_PARTITION_SIZE_MB = 100)` under a configuration whose new
// tables do not split by size, and `SET (AUTO_PARTITIONING_PARTITION_SIZE_MB =
// 100)` on a table that does not, both leave a table splitting by size at 100
// MB. Splitting by size turned on with no size declared or held takes the
// size YDB gives it, 2048 MB: measured on both lines, `SET
// (AUTO_PARTITIONING_BY_SIZE = ENABLED)` on a table that did not split by size
// leaves 2048 MB, under the cluster's default configuration and under one
// whose new tables do not split by size. Naming it changes nothing on the
// server, and tells a reader of the statement that no size was reset.
func (d Declared) Over(held Settings) (Settings, error) {
	settings := held
	switch {
	case d.BySize != nil:
		settings.BySize = *d.BySize
	case d.PartitionSizeMB != 0:
		settings.BySize = true
	}
	switch {
	case !settings.BySize && d.PartitionSizeMB != 0:
		return Settings{}, errSizeWithoutSplitting
	case !settings.BySize:
		settings.PartitionSizeMB = 0
	case d.PartitionSizeMB != 0:
		settings.PartitionSizeMB = d.PartitionSizeMB
	case settings.PartitionSizeMB == 0:
		settings.PartitionSizeMB = defaultPartitionSizeMB
	}
	if d.ByLoad != nil {
		settings.ByLoad = *d.ByLoad
	}
	if d.MinPartitions != 0 {
		settings.MinPartitions = d.MinPartitions
	}
	if d.MaxPartitions != 0 {
		settings.MaxPartitions = d.MaxPartitions
	}
	if d.ReadReplicas != "" {
		replicas, err := ParseReplicas(d.ReadReplicas)
		if err != nil {
			return Settings{}, err
		}
		settings.ReadReplicas = replicas
	}
	return settings, nil
}

// Held reads a reader's report of what a table or an index holds, written by
// [Settings.Declared] as the settings that differ from [DefaultSettings]. A
// report YDB could not have written is an error saying why.
func (d Declared) Held() (Settings, error) {
	return d.Over(DefaultSettings())
}

// Declared writes settings as what differs from [DefaultSettings], which is
// what a reader reports. [Declared.Held] gives the settings back. As a
// declaration it would leave every setting at its default to what the table
// holds; [Settings.Explicit] names them all.
func (s Settings) Declared() Declared {
	defaults := DefaultSettings()
	declared := Declared{MaxPartitions: s.MaxPartitions}
	if s.BySize != defaults.BySize {
		declared.BySize = new(s.BySize)
	}
	if s.BySize && s.PartitionSizeMB != defaults.PartitionSizeMB {
		declared.PartitionSizeMB = s.PartitionSizeMB
	}
	if s.ByLoad != defaults.ByLoad {
		declared.ByLoad = new(s.ByLoad)
	}
	if s.MinPartitions != defaults.MinPartitions {
		declared.MinPartitions = s.MinPartitions
	}
	if !s.ReadReplicas.None() {
		declared.ReadReplicas = s.ReadReplicas.String()
	}
	return declared
}

// noReplicas is how a declaration names read replicas as none. YDB takes it
// at CREATE TABLE and in SET, and removes the replicas a table holds with it.
const noReplicas = "PER_AZ:0"

// Explicit writes settings as a declaration that names every setting, so it
// resolves to them over what a table holds. A rebuild writes it for the new
// table, which would otherwise take the cluster's settings, and a rollback for
// the table it restores. No declaration names the absence of a maximum
// partition count, which YDB cannot remove; a table with no maximum resolves
// to the maximum it is read over.
func (s Settings) Explicit() Declared {
	declared := Declared{
		BySize:        new(s.BySize),
		ByLoad:        new(s.ByLoad),
		MinPartitions: s.MinPartitions,
		MaxPartitions: s.MaxPartitions,
		ReadReplicas:  noReplicas,
	}
	if s.BySize {
		declared.PartitionSizeMB = s.PartitionSizeMB
	}
	if !s.ReadReplicas.None() {
		declared.ReadReplicas = s.ReadReplicas.String()
	}
	return declared
}

// CreateClause writes the settings a statement names for an object YDB has
// just created, in a fixed order: each setting the declaration names, as it
// names it, and nothing for one it leaves out, which the object takes from
// YDB. Read replicas declared as none are not written: a new table and a new
// index hold none, measured on 25.1.4.7 and 26.2.1.14 under the default
// configuration and one whose new tables do not split by size. A caller
// resolves the declaration first, so what is written is a declaration YDB
// takes.
func (d Declared) CreateClause() []string {
	var settings []string
	if d.BySize != nil {
		settings = append(settings, "AUTO_PARTITIONING_BY_SIZE = "+switchSpelling[*d.BySize])
	}
	if d.PartitionSizeMB != 0 {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_PARTITION_SIZE_MB = %d", d.PartitionSizeMB))
	}
	if d.ByLoad != nil {
		settings = append(settings, "AUTO_PARTITIONING_BY_LOAD = "+switchSpelling[*d.ByLoad])
	}
	if d.MinPartitions != 0 {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d", d.MinPartitions))
	}
	if d.MaxPartitions != 0 {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d", d.MaxPartitions))
	}
	if replicas, err := ParseReplicas(d.ReadReplicas); err == nil && !replicas.None() {
		settings = append(settings, fmt.Sprintf("READ_REPLICAS_SETTINGS = %q", replicas.String()))
	}
	return settings
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

// Equal reports whether two tables hold the same settings.
func (s Settings) Equal(other Settings) bool {
	return s.ReadReplicas.Equal(other.ReadReplicas) && s.sameSplitting(other)
}

// sameSplitting reports whether two tables split and merge alike: every
// setting but the read replicas.
func (s Settings) sameSplitting(other Settings) bool {
	return s.BySize == other.BySize && s.PartitionSizeMB == other.PartitionSizeMB && s.ByLoad == other.ByLoad &&
		s.MinPartitions == other.MinPartitions && s.MaxPartitions == other.MaxPartitions
}

// Clause writes the settings of a SET (...) that take a table or an index
// holding current to desired, and nil where the two are [Settings.Equal].
// desired is resolved over current ([Declared.Over]), so it holds a maximum
// wherever current does: YDB has no way back to a table without one, measured
// on 25.1.4.7 and 26.2.1.14 as `Can't set max partition count to 0` and
// `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT reset is not supported`, on a table
// and on an index alike.
//
// It names every splitting setting desired holds rather than only the ones
// that differ, because setting one resets others. Measured on 25.1.4.7 and
// 26.2.1.14, on a table and on an index: on one holding a minimum of 6
// partitions, `SET (AUTO_PARTITIONING_BY_LOAD = ENABLED)` leaves a minimum of
// 1, and on one holding a size of 100 MB and a minimum of 6, `SET
// (AUTO_PARTITIONING_BY_SIZE = ENABLED)` leaves 2048 MB and 1. A setting named
// in the same statement keeps the value it names, whatever its place in the
// list, so a statement naming them all leaves exactly desired. Read replicas
// are named where desired has some, or where current has some for desired to
// remove: `PER_AZ:0` clears them, and RESET is refused.
func Clause(desired, current Settings) []string {
	if desired.Equal(current) {
		return nil
	}
	settings := desired.splittingClause()
	if replicas, named := replicasClause(desired.ReadReplicas, current.ReadReplicas, true); named {
		settings = append(settings, replicas)
	}
	return settings
}

// splittingClause names every splitting setting the settings hold: the size
// where they split by size, the maximum where they have one, and the rest
// always.
func (s Settings) splittingClause() []string {
	settings := []string{"AUTO_PARTITIONING_BY_SIZE = " + switchSpelling[s.BySize]}
	if s.BySize {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_PARTITION_SIZE_MB = %d", s.PartitionSizeMB))
	}
	settings = append(settings,
		"AUTO_PARTITIONING_BY_LOAD = "+switchSpelling[s.ByLoad],
		fmt.Sprintf("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d", s.MinPartitions),
	)
	if s.MaxPartitions != 0 {
		settings = append(settings, fmt.Sprintf("AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d", s.MaxPartitions))
	}
	return settings
}

// replicasClause writes READ_REPLICAS_SETTINGS for desired, and reports
// whether to name it: where desired has replicas and always is set, or where
// the two differ. Replicas going away are written as `PER_AZ:0`, which clears
// them; RESET is refused.
func replicasClause(desired, current Replicas, always bool) (string, bool) {
	switch {
	case !desired.None() && (always || !desired.Equal(current)):
		return fmt.Sprintf("READ_REPLICAS_SETTINGS = %q", desired.String()), true
	case desired.None() && !current.None():
		return `READ_REPLICAS_SETTINGS = "PER_AZ:0"`, true
	default:
		return "", false
	}
}

// switchSpelling spells a switch the way YDB's settings take it.
var switchSpelling = map[bool]string{true: "ENABLED", false: "DISABLED"}
