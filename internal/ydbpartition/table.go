package ydbpartition

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// The attributes only a table declaration reads, beside the shared ones.
const (
	AttributeKeyBloomFilter    = "key_bloom_filter"
	AttributeUniformPartitions = "uniform_partitions"
	AttributePartitionAtKeys   = "partition_at_keys"
)

// TableAttributes lists every attribute a table declaration reads, in the
// order its errors are reported.
func TableAttributes() []string {
	return append(Attributes(), AttributeKeyBloomFilter, AttributeUniformPartitions, AttributePartitionAtKeys)
}

// TableSettings is a row table's settings with every setting resolved: its
// splitting and read replicas, and its key bloom filter. The starting layout
// is not here, because YDB keeps no record of it.
type TableSettings struct {
	Settings
	// KeyBloomFilter is KEY_BLOOM_FILTER.
	KeyBloomFilter bool
}

// DefaultTableSettings is what YDB's documentation gives a new table:
// [DefaultSettings] and no key bloom filter. Like [DefaultSettings], it is the
// baseline a reader's report is written against, never what a setting a
// declaration leaves out means. Measured on 25.1.4.7 and 26.2.1.14, a new
// table describes its key bloom filter as unspecified, and one created with
// KEY_BLOOM_FILTER = DISABLED as disabled; both keep no filter, and both read
// as false here.
func DefaultTableSettings() TableSettings {
	return TableSettings{Settings: DefaultSettings()}
}

// errTwoLayouts is YDB's refusal of a table that names both starting
// layouts, measured on 25.1.4.7 and 26.2.1.14.
var errTwoLayouts = errors.New("uniform_partitions and partition_at_keys are both declared, which YDB refuses " +
	"(`Uniform partitions and partitions at keys settings are mutually exclusive`)")

// ParseTableDeclaration reads a table declaration's settings out of values,
// keyed by attribute name, and ignores every other key. It returns nil where
// none is present. The shared attributes are read as [ParseDeclared] reads
// them; key_bloom_filter takes ENABLED or DISABLED, uniform_partitions a count
// of at least 1, and partition_at_keys the split points [ParseSplitPoints]
// reads.
func ParseTableDeclaration(values map[string]string) (*ast.YDBTablePartitioningSpec, error) {
	declared, present, err := ParseDeclared(values)
	if err != nil {
		return nil, err
	}
	spec := tableSpecOf(declared)
	if value, ok := values[AttributeKeyBloomFilter]; ok {
		present = true
		if spec.KeyBloomFilter, err = parseSwitch(AttributeKeyBloomFilter, strings.TrimSpace(value)); err != nil {
			return nil, err
		}
	}
	if value, ok := values[AttributeUniformPartitions]; ok {
		present = true
		if err := parseCount(AttributeUniformPartitions, strings.TrimSpace(value), &spec.UniformPartitions); err != nil {
			return nil, err
		}
	}
	if value, ok := values[AttributePartitionAtKeys]; ok {
		present = true
		points, err := ParseSplitPoints(value)
		if err != nil {
			return nil, &DeclarationError{Attribute: AttributePartitionAtKeys, Value: value, Reason: err.Error()}
		}
		spec.PartitionAtKeys = points
	}
	if !present {
		return nil, nil
	}
	return spec, nil
}

// Requirement is one capability key a declaration needs, with the settings
// that need it as a refusal names them.
type Requirement struct {
	// Key is the capability.
	Key capability.Capability
	// Settings names what the declaration sets that needs Key, as a noun:
	// "partitioning", "read replicas" or "key bloom filter".
	Settings string
}

// Requirements lists the capability keys a declaration needs, in a fixed
// order: [capability.PartitioningOptions] for how the table splits and the
// partitions it starts with, [capability.ReadReplicas] for its read replicas,
// and [capability.KeyBloomFilter] for its key bloom filter. A target without
// one of them cannot hold what the declaration names, and a renderer or a
// planner that wrote the table without it would leave it at YDB's defaults
// with nothing reporting the difference.
func Requirements(spec *ast.YDBTablePartitioningSpec) []Requirement {
	if spec.IsZero() {
		return nil
	}
	var requirements []Requirement
	if spec.BySize != nil || spec.PartitionSizeMB != 0 || spec.ByLoad != nil || spec.MinPartitions != 0 ||
		spec.MaxPartitions != 0 || spec.UniformPartitions != 0 || len(spec.PartitionAtKeys) != 0 {
		requirements = append(requirements, Requirement{Key: capability.PartitioningOptions, Settings: "partitioning"})
	}
	if spec.ReadReplicas != "" {
		requirements = append(requirements, Requirement{Key: capability.ReadReplicas, Settings: "read replicas"})
	}
	if spec.KeyBloomFilter != nil {
		requirements = append(requirements, Requirement{Key: capability.KeyBloomFilter, Settings: "key bloom filter"})
	}
	return requirements
}

// tableSpecOf writes the shared settings as a table's declaration.
func tableSpecOf(declared Declared) *ast.YDBTablePartitioningSpec {
	return &ast.YDBTablePartitioningSpec{
		BySize:          declared.BySize,
		PartitionSizeMB: declared.PartitionSizeMB,
		ByLoad:          declared.ByLoad,
		MinPartitions:   declared.MinPartitions,
		MaxPartitions:   declared.MaxPartitions,
		ReadReplicas:    declared.ReadReplicas,
	}
}

// declaredOf reads the shared settings out of a table's declaration.
func declaredOf(spec *ast.YDBTablePartitioningSpec) Declared {
	return Declared{
		BySize:          spec.BySize,
		PartitionSizeMB: spec.PartitionSizeMB,
		ByLoad:          spec.ByLoad,
		MinPartitions:   spec.MinPartitions,
		MaxPartitions:   spec.MaxPartitions,
		ReadReplicas:    spec.ReadReplicas,
	}
}

// ResolveTable reads a declaration against the settings the table holds: each
// setting it names, and the held value of each it leaves out (see
// [Declared.Over]). A nil declaration resolves to held, so removing a
// declaration changes nothing.
//
// A starting layout sets the minimum partition count the way YDB does when it
// creates the table, measured on 25.1.4.7 and 26.2.1.14: the count the
// declaration names, else the partitions its starting layout creates.
// UNIFORM_PARTITIONS = 4 leaves a minimum of 4, PARTITION_AT_KEYS with three
// split points leaves 4, and either one beside
// AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2 leaves 2.
//
// A declaration YDB would refuse whatever the table's columns are is an error
// saying why; one whose layout does not fit the table's key is refused where
// the key is known, by [LayoutClause].
func ResolveTable(spec *ast.YDBTablePartitioningSpec, held TableSettings) (TableSettings, error) {
	if spec.IsZero() {
		return held, nil
	}
	if spec.UniformPartitions != 0 && len(spec.PartitionAtKeys) != 0 {
		return TableSettings{}, errTwoLayouts
	}
	settings, err := declaredOf(spec).Over(held.Settings)
	if err != nil {
		return TableSettings{}, err
	}
	if starting := LayoutPartitions(spec); spec.MinPartitions == 0 && starting != 0 {
		settings.MinPartitions = starting
	}
	table := TableSettings{Settings: settings, KeyBloomFilter: held.KeyBloomFilter}
	if spec.KeyBloomFilter != nil {
		table.KeyBloomFilter = *spec.KeyBloomFilter
	}
	return table, nil
}

// HeldTable reads a reader's report of what a table holds, written by
// [TableSpec] as the settings that differ from [DefaultTableSettings]. A nil
// report is a table holding the defaults.
func HeldTable(spec *ast.YDBTablePartitioningSpec) (TableSettings, error) {
	return ResolveTable(spec, DefaultTableSettings())
}

// LayoutPartitions is how many partitions a declaration's starting layout
// creates, and 0 where it declares none.
func LayoutPartitions(spec *ast.YDBTablePartitioningSpec) uint64 {
	switch {
	case spec == nil:
		return 0
	case spec.UniformPartitions != 0:
		return spec.UniformPartitions
	case len(spec.PartitionAtKeys) != 0:
		return uint64(len(spec.PartitionAtKeys)) + 1
	default:
		return 0
	}
}

// layoutSetting names a declaration's starting layout as YDB spells it.
func layoutSetting(spec *ast.YDBTablePartitioningSpec) string {
	if spec.UniformPartitions != 0 {
		return fmt.Sprintf("UNIFORM_PARTITIONS = %d", spec.UniformPartitions)
	}
	if len(spec.PartitionAtKeys) == 1 {
		return "PARTITION_AT_KEYS with one split point"
	}
	return fmt.Sprintf("PARTITION_AT_KEYS with %d split points", len(spec.PartitionAtKeys))
}

// TableSpec writes settings as what differs from [DefaultTableSettings], which
// is what a reader reports for a table: nil for a table holding the defaults.
// [HeldTable] gives the settings back.
func TableSpec(settings TableSettings) *ast.YDBTablePartitioningSpec {
	spec := tableSpecOf(settings.Declared())
	if settings.KeyBloomFilter {
		spec.KeyBloomFilter = new(true)
	}
	if spec.IsZero() {
		return nil
	}
	return spec
}

// Explicit writes settings as a table declaration that names every setting,
// so it resolves to them over whatever a table holds; see [Settings.Explicit].
// It names no starting layout, which a table does not hold.
func (s TableSettings) Explicit() *ast.YDBTablePartitioningSpec {
	spec := tableSpecOf(s.Settings.Explicit())
	spec.KeyBloomFilter = new(s.KeyBloomFilter)
	return spec
}

// Equal reports whether two tables hold the same settings.
func (s TableSettings) Equal(other TableSettings) bool {
	return s.Settings.Equal(other.Settings) && s.KeyBloomFilter == other.KeyBloomFilter
}

// TableChangeRefusal says why a table holding current cannot take the
// declaration desired in place, and answers "" where it can: a starting layout
// on a table YDB created without it. resolved is desired read over current
// ([ResolveTable]).
//
// YDB takes a starting layout only when it creates a table: measured on
// 25.1.4.7 and 26.2.1.14, `ALTER TABLE t SET (UNIFORM_PARTITIONS = 4)` answers
// `UNIFORM_PARTITIONS alter is not supported`, and PARTITION_AT_KEYS answers
// the same. YDB keeps no record of a layout either, only the minimum partition
// count it set, so that minimum is the one witness a table has: a declaration
// whose layout sets the minimum (it names none of its own) and whose minimum
// differs from the table's asks for a layout the table was not created with.
// A layout beside a declared minimum leaves no witness, and is not compared.
func TableChangeRefusal(desired *ast.YDBTablePartitioningSpec, resolved, current TableSettings) string {
	if desired.IsZero() || desired.MinPartitions != 0 || LayoutPartitions(desired) == 0 {
		return ""
	}
	if resolved.MinPartitions == current.MinPartitions {
		return ""
	}
	return fmt.Sprintf("it declares %s, which YDB takes only when it creates a table (`%s alter is not supported`) and "+
		"which gives a new table a minimum of %d partitions, where this table holds %d. Declare "+
		"auto_partitioning_min_partitions_count to change the minimum in place",
		layoutSetting(desired), layoutKeyword(desired), resolved.MinPartitions, current.MinPartitions)
}

// layoutKeyword is the setting a declaration's starting layout uses.
func layoutKeyword(spec *ast.YDBTablePartitioningSpec) string {
	if spec.UniformPartitions != 0 {
		return "UNIFORM_PARTITIONS"
	}
	return "PARTITION_AT_KEYS"
}

// TableClause writes the settings of `ALTER TABLE ... SET (...)` that take a
// table holding current to desired, and nil where the two are
// [TableSettings.Equal]. desired is resolved over current ([ResolveTable]), and
// a caller asks [TableChangeRefusal] first.
//
// The settings fall into three groups, and a change names every setting of
// each group it touches. The splitting settings are one group, because setting
// one resets others (see [Clause]); the read replicas and the key bloom filter
// are a group each, because setting them resets nothing: measured on 25.1.4.7
// and 26.2.1.14, on a table holding a size of 100 MB, a minimum of 6 and a
// maximum of 20, `SET (READ_REPLICAS_SETTINGS = "ANY_AZ:2")` and `SET
// (KEY_BLOOM_FILTER = DISABLED)` leave every other setting as it was. No
// setting can be reset (`... reset is not supported` for each), so replicas
// going away are written as `PER_AZ:0` and a filter going away as DISABLED.
func TableClause(desired, current TableSettings) []string {
	var settings []string
	if !desired.sameSplitting(current.Settings) {
		settings = desired.splittingClause()
	}
	if replicas, named := replicasClause(desired.ReadReplicas, current.ReadReplicas, false); named {
		settings = append(settings, replicas)
	}
	if desired.KeyBloomFilter != current.KeyBloomFilter {
		settings = append(settings, "KEY_BLOOM_FILTER = "+switchSpelling[desired.KeyBloomFilter])
	}
	return settings
}

// CreateClause writes the settings a CREATE TABLE ... WITH (...) names for a
// declaration, in a fixed order: each one the declaration names, as it names
// it (see [Declared.CreateClause]), and its key bloom filter. YDB creates the
// table with every setting at once, so no setting resets another there; the
// starting layout is written by [LayoutClause], which needs the table's key. A
// caller resolves the declaration first, so what is written here is a
// declaration YDB takes.
func CreateClause(spec *ast.YDBTablePartitioningSpec) []string {
	if spec.IsZero() {
		return nil
	}
	settings := declaredOf(spec).CreateClause()
	if spec.KeyBloomFilter != nil {
		settings = append(settings, "KEY_BLOOM_FILTER = "+switchSpelling[*spec.KeyBloomFilter])
	}
	return settings
}
