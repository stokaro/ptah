package ydbpartition

import (
	"fmt"
	"strconv"
	"strings"
)

// The attributes that declare the settings a table and a global index share,
// spelled as YDB spells the settings, in lower case. The annotation parser, the
// YAML reader and the annotation registry all read these names, so an
// attribute one of them accepts is one the others read.
const (
	AttributeBySize          = "auto_partitioning_by_size"
	AttributePartitionSizeMB = "auto_partitioning_partition_size_mb"
	AttributeByLoad          = "auto_partitioning_by_load"
	AttributeMinPartitions   = "auto_partitioning_min_partitions_count"
	AttributeMaxPartitions   = "auto_partitioning_max_partitions_count"
	AttributeReadReplicas    = "read_replicas_settings"
)

// Attributes lists the shared attributes in the order a declaration's errors
// are reported.
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

// ParseDeclared reads the shared attributes out of values, keyed by attribute
// name, and ignores every other key. It reports whether any of them is
// present.
//
// Each value is checked for the form YDB takes, so a typo is refused where it
// was written rather than by the server: `ENABLED` or `DISABLED` in either
// case for the two switches, a count of at least 1 for the numbers -- YDB
// refuses every count of zero, measured on 25.1.4.7 and 26.2.1.14 as `Can't set
// min partition count to 0` and its siblings -- and `PER_AZ:<n>` or
// `ANY_AZ:<n>` for the read replicas, which are written back in capitals. A
// count of zero declares no replicas, which is how a table that holds some
// loses them. A combination YDB refuses, such as a size on a table that does
// not split by size, is left to [Declared.Over], which every renderer and
// planner asks.
func ParseDeclared(values map[string]string) (Declared, bool, error) {
	var declared Declared
	present := false
	for _, attribute := range Attributes() {
		value, ok := values[attribute]
		if !ok {
			continue
		}
		present = true
		if err := declared.set(attribute, strings.TrimSpace(value)); err != nil {
			return Declared{}, false, err
		}
	}
	return declared, present, nil
}

// set reads one shared attribute's value into the declaration.
func (d *Declared) set(attribute, value string) error {
	switch attribute {
	case AttributeBySize:
		enabled, err := parseSwitch(attribute, value)
		d.BySize = enabled
		return err
	case AttributeByLoad:
		enabled, err := parseSwitch(attribute, value)
		d.ByLoad = enabled
		return err
	case AttributePartitionSizeMB:
		return parseCount(attribute, value, &d.PartitionSizeMB)
	case AttributeMinPartitions:
		return parseCount(attribute, value, &d.MinPartitions)
	case AttributeMaxPartitions:
		return parseCount(attribute, value, &d.MaxPartitions)
	default:
		replicas, err := ParseReplicas(value)
		if err != nil {
			return &DeclarationError{Attribute: attribute, Value: value,
				Reason: "write PER_AZ:<n> for n replicas in every availability zone, or ANY_AZ:<n> for n in all of them together"}
		}
		d.ReadReplicas = replicas.String()
		return nil
	}
}

// parseSwitch reads ENABLED or DISABLED, in either case, as YDB spells the
// settings that switch something on and off.
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
