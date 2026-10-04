package ydbindex

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
)

// The attributes that declare a global index's partitioning, spelled as YDB
// spells the settings, in lower case. The annotation parser, the YAML reader
// and the annotation registry all read these names, so an attribute one of
// them accepts is one the others read.
const (
	AttributeBySize          = "auto_partitioning_by_size"
	AttributePartitionSizeMB = "auto_partitioning_partition_size_mb"
	AttributeByLoad          = "auto_partitioning_by_load"
	AttributeMinPartitions   = "auto_partitioning_min_partitions_count"
	AttributeMaxPartitions   = "auto_partitioning_max_partitions_count"
	AttributeReadReplicas    = "read_replicas_settings"
)

// Attributes lists the partitioning attributes in the order a declaration's
// errors are reported.
func Attributes() []string {
	return []string{
		AttributeBySize, AttributePartitionSizeMB, AttributeByLoad,
		AttributeMinPartitions, AttributeMaxPartitions, AttributeReadReplicas,
	}
}

// DeclarationError is an attribute whose value a declaration cannot carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Value is the value it was given.
	Value string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Attribute, e.Value, e.Reason)
}

// ParseDeclaration reads the partitioning attributes of one index declaration
// out of values, keyed by attribute name, and ignores every other key. It
// returns nil where none is present.
//
// Each value is checked for the form YDB takes, so a typo is refused where it
// was written rather than by the server: `ENABLED` or `DISABLED` in either
// case for the two switches, a count of at least 1 for the numbers -- YDB
// refuses every count of zero, measured on 25.1.4.7 and 26.2.1.14 as `Can't set
// min partition count to 0` and its siblings -- and `PER_AZ:<n>` or
// `ANY_AZ:<n>` for the read replicas, which are written back in capitals, and
// as nothing for a count of zero, which is no replicas. A combination YDB
// refuses, such as a size on an index that does not split by size, is left to
// [Resolve], which every renderer and planner asks.
func ParseDeclaration(values map[string]string) (*ast.IndexPartitioningSpec, error) {
	var spec ast.IndexPartitioningSpec
	present := false
	for _, attribute := range Attributes() {
		value, ok := values[attribute]
		if !ok {
			continue
		}
		present = true
		if err := setAttribute(&spec, attribute, strings.TrimSpace(value)); err != nil {
			return nil, err
		}
	}
	if !present {
		return nil, nil
	}
	return &spec, nil
}

// setAttribute reads one attribute's value into spec.
func setAttribute(spec *ast.IndexPartitioningSpec, attribute, value string) error {
	switch attribute {
	case AttributeBySize:
		enabled, err := parseSwitch(attribute, value)
		spec.BySize = enabled
		return err
	case AttributeByLoad:
		enabled, err := parseSwitch(attribute, value)
		spec.ByLoad = enabled
		return err
	case AttributePartitionSizeMB:
		return parseCount(attribute, value, &spec.PartitionSizeMB)
	case AttributeMinPartitions:
		return parseCount(attribute, value, &spec.MinPartitions)
	case AttributeMaxPartitions:
		return parseCount(attribute, value, &spec.MaxPartitions)
	default:
		replicas, err := ParseReplicas(value)
		if err != nil {
			return &DeclarationError{Attribute: attribute, Value: value,
				Reason: "write PER_AZ:<n> for n replicas in every availability zone, or ANY_AZ:<n> for n in all of them together"}
		}
		if !replicas.None() {
			spec.ReadReplicas = replicas.String()
		}
		return nil
	}
}

// parseSwitch reads ENABLED or DISABLED, in either case, as YDB spells the two
// settings that switch splitting on and off.
func parseSwitch(attribute, value string) (*bool, error) {
	switch strings.ToUpper(value) {
	case "ENABLED":
		return new(true), nil
	case "DISABLED":
		return new(false), nil
	default:
		return nil, &DeclarationError{Attribute: attribute, Value: value, Reason: "write ENABLED or DISABLED"}
	}
}

// parseCount reads a count of at least 1 into target.
func parseCount(attribute, value string, target *uint64) error {
	count, err := strconv.ParseUint(value, 10, 64)
	if err != nil || count == 0 {
		return &DeclarationError{Attribute: attribute, Value: value,
			Reason: "write a whole number of at least 1; YDB refuses a count of zero"}
	}
	*target = count
	return nil
}
