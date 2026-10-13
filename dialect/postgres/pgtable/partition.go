package pgtable

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// PartitionKind identifies a PostgreSQL table's partition strategy and key.
const PartitionKind schemaext.Kind = "ptah.run/postgres/table-partition"

// Partition declares an ordered PostgreSQL partition key. A missing facet
// denotes an ordinary table only when source coverage establishes absence.
type Partition struct {
	// Type is RANGE, LIST, or HASH.
	Type string `json:"type"`
	// Parts are ordered column or expression keys. LIST takes exactly one.
	Parts []PartitionPart `json:"parts"`
}

// PartitionPart is a column or expression followed by optional key modifiers.
// Name is the column's catalog spelling, including any literal dots. Expr is
// SQL text. Exactly one of Name and Expr is nonempty.
type PartitionPart struct {
	// Name names a column, without SQL quotes.
	Name string `json:"name,omitempty"`
	// Expr is a SQL expression, without its partition-key parentheses.
	Expr string `json:"expr,omitempty"`
	// Collation names a collation in SQL identifier syntax, or selects the default.
	Collation string `json:"collation,omitempty"`
	// OperatorClass names an operator class in SQL identifier syntax, or selects the default.
	OperatorClass string `json:"operator_class,omitempty"`
}

// DesiredPartition declares a table's partition strategy and key.
type DesiredPartition struct {
	// Type is RANGE, LIST, or HASH.
	Type string `json:"type"`
	// Parts are the ordered column or expression keys.
	Parts []PartitionPart `json:"parts"`
	// Normalized holds a server-derived comparison key; source encoders omit it.
	Normalized *NormalizedPartition `json:"normalized,omitempty"`
}

// NormalizedPartition binds the server's key to the declaration it normalizes.
// SourceKey and Key are produced by PartitionComparisonKey. A probe sets them
// together; changing the declaration invalidates the captured answer.
type NormalizedPartition struct {
	// SourceKey identifies the exact declaration sent to the server.
	SourceKey string `json:"source_key"`
	// Key identifies the partition key returned by the server.
	Key string `json:"key"`
}

// ObservedPartition records a table's inspected partition strategy and key.
type ObservedPartition Partition

// Kind returns the partition facet identity.
func (*DesiredPartition) Kind() schemaext.Kind { return PartitionKind }

// Kind returns the partition facet identity.
func (*ObservedPartition) Kind() schemaext.Kind { return PartitionKind }

// Clone returns an independent declaration, preserving a typed nil receiver.
func (p *DesiredPartition) Clone() schemaext.Value {
	if p == nil {
		return (*DesiredPartition)(nil)
	}
	cloned := *p
	cloned.Parts = slices.Clone(p.Parts)
	if p.Normalized != nil {
		cloned.Normalized = new(*p.Normalized)
	}
	return &cloned
}

// Clone returns an independent observation, preserving a typed nil receiver.
func (p *ObservedPartition) Clone() schemaext.Value {
	if p == nil {
		return (*ObservedPartition)(nil)
	}
	cloned := *p
	cloned.Parts = slices.Clone(p.Parts)
	return &cloned
}

// Equal compares declarations and key order. Other representations are unequal.
func (p *DesiredPartition) Equal(value schemaext.Value) bool {
	other, ok := value.(*DesiredPartition)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return p.Type == other.Type && slices.Equal(p.Parts, other.Parts) && equalNormalizedPartition(p.Normalized, other.Normalized)
}

// Equal compares observations and key order. Other representations are unequal.
func (p *ObservedPartition) Equal(value schemaext.Value) bool {
	other, ok := value.(*ObservedPartition)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return p.Type == other.Type && slices.Equal(p.Parts, other.Parts)
}

// ValidateDesiredPartition rejects nil, unknown strategies, empty keys, and
// ambiguous parts. Errors wrap schemaext.ErrInvalidValue. It performs no SQL
// evaluation; the selected PostgreSQL server validates expression semantics.
func ValidateDesiredPartition(p *DesiredPartition) error {
	if p == nil {
		return validatePartition(nil, schemaext.Desired)
	}
	if err := validatePartition(&Partition{Type: p.Type, Parts: p.Parts}, schemaext.Desired); err != nil {
		return err
	}
	if p.Normalized != nil && (p.Normalized.Key == "" || p.Normalized.SourceKey != PartitionComparisonKey(&Partition{Type: p.Type, Parts: p.Parts})) {
		return fmt.Errorf("%w: normalized partition key does not describe the declaration", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidateObservedPartition validates captured partition data under the same
// structural rules as ValidateDesiredPartition.
func ValidateObservedPartition(p *ObservedPartition) error {
	return validatePartition((*Partition)(p), schemaext.Observed)
}

func validatePartition(p *Partition, representation schemaext.Representation) error {
	fail := func(message string) error {
		return &schemaext.InvalidModelError{Kind: PartitionKind, Representation: representation, Message: message}
	}
	if p == nil {
		return fail("table partition requires a non-nil value")
	}
	if p.Type != "RANGE" && p.Type != "LIST" && p.Type != "HASH" {
		return fail("partition type must be RANGE, LIST, or HASH")
	}
	if len(p.Parts) == 0 {
		return fail("partition requires at least one key")
	}
	if p.Type == "LIST" && len(p.Parts) != 1 {
		return fail("LIST partitioning requires exactly one key")
	}
	for i, part := range p.Parts {
		if (part.Name == "") == (part.Expr == "") || part.Expr != "" && strings.TrimSpace(part.Expr) == "" {
			return fail(fmt.Sprintf("partition key %d requires exactly one column or expression", i+1))
		}
	}
	return nil
}

// PartitionOf reads either representation without exposing its backing slice.
// Absence returns nil, false, nil. Invalid values return no partition and an
// error wrapping schemaext.ErrInvalidValue.
func PartitionOf(facets schemaext.Facets) (*Partition, bool, error) {
	value, found, err := facets.Get(PartitionKind)
	if err != nil || !found {
		return nil, false, err
	}
	var p Partition
	switch value := value.(type) {
	case *DesiredPartition:
		if err := ValidateDesiredPartition(value); err != nil {
			return nil, true, err
		}
		p = Partition{Type: value.Type, Parts: value.Parts}
	case *ObservedPartition:
		if err := ValidateObservedPartition(value); err != nil {
			return nil, true, err
		}
		p = Partition{Type: value.Type, Parts: value.Parts}
	default:
		return nil, true, fmt.Errorf("%w: unexpected table partition value %T", schemaext.ErrInvalidValue, value)
	}
	p.Parts = slices.Clone(p.Parts)
	return &p, true, nil
}

const partitionDefinition = `{"type":"object","required":["type","parts"],"additionalProperties":false,
"properties":{"type":{"enum":["RANGE","LIST","HASH"]},"parts":{"type":"array","minItems":1,"items":{"type":"object","additionalProperties":false,
"properties":{"name":{"type":"string"},"expr":{"type":"string"},
"collation":{"type":"string"},"operator_class":{"type":"string"}}}}}}`

// PartitionCodecs returns version-one declaration and observation codecs.
// They reject unknown fields, duplicate JSON keys, and structurally invalid keys.
func PartitionCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredPartition]{Prototype: &DesiredPartition{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(desiredPartitionDefinition), Shape: desiredPartitionShape, Validate: ValidateDesiredPartition}.Codec(),
		schemaext.ModelCodec[*ObservedPartition]{Prototype: &ObservedPartition{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(partitionDefinition), Shape: partitionShape, Validate: ValidateObservedPartition}.Codec(),
	}
}

func partitionShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table partition", Allowed: []string{"type", "parts"}, Required: []string{"type", "parts"}})
	if err != nil {
		return err
	}
	parts, err := schemaext.DecodeJSON[[]json.RawMessage](fields["parts"])
	if err != nil {
		return err
	}
	for _, part := range parts {
		fields, err := schemaext.DecodeObject(part, schemaext.ObjectShape{Name: "partition key", Allowed: []string{"name", "expr", "collation", "operator_class"}})
		if err != nil {
			return err
		}
		for name, raw := range fields {
			if string(raw) == "null" {
				return fmt.Errorf("%w: partition key %s requires a string", schemaext.ErrInvalidValue, name)
			}
		}
	}
	return nil
}

var partitionCoverage = schemaext.OwnedCoverageSource(Owner, PartitionCodecs)

// PartitionCoverage describes partition knowledge independently of facet values.
// Complete coverage makes an absent facet mean an ordinary table. Subject
// records retain knowledge limits for individual tables.
func PartitionCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return partitionCoverage(PartitionKind, representation, knowledge, subjects)
}

// SupportsPartition reports whether the target uses PostgreSQL partition keys.
// PostgreSQL and YugabyteDB retain RANGE, LIST and HASH declarations.
func SupportsPartition(target string) bool {
	return slices.Contains([]string{platform.Postgres, platform.YugabyteDB}, platform.NormalizeDialect(target))
}

func equalNormalizedPartition(a, b *NormalizedPartition) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

var desiredPartitionDefinition = strings.TrimSuffix(partitionDefinition, "}}") + `,"normalized":{"type":"object","required":["source_key","key"],"additionalProperties":false,
"properties":{"source_key":{"type":"string"},"key":{"type":"string"}}}}}`

func desiredPartitionShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table partition", Allowed: []string{"type", "parts", "normalized"}, Required: []string{"type", "parts"}})
	if err != nil {
		return err
	}
	if normalized, found := fields["normalized"]; found {
		if _, err := schemaext.DecodeObject(normalized, schemaext.ObjectShape{Name: "normalized partition", Allowed: []string{"source_key", "key"}, Required: []string{"source_key", "key"}}); err != nil {
			return err
		}
	}
	delete(fields, "normalized")
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return partitionShape(raw)
}
