package ydbschema

import (
	_ "embed" // Embed the definition that identifies the table partitioning wire format.
	"encoding/json"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed table-partitioning-codecs.json
var tablePartitioningDefinition []byte

// TablePartitioningWireDefinition returns an independent description of the
// table partitioning wire model. The definition covers both representations.
func TablePartitioningWireDefinition() json.RawMessage {
	return slices.Clone(tablePartitioningDefinition)
}

// tablePartitioningShape is the value object's keys, from the JSON tags the
// encoder writes.
var tablePartitioningShape = wireShape[TablePartitioning]("YDB table partitioning")

// TablePartitioningCodecs returns the version-one desired and observed table
// partitioning codecs, in that order. Each call returns independent
// definitions. The value is one object of the settings it states. A decoder
// accepts only the spelling the encoder writes: a key in another letter case,
// a null, and an omitted value written out, such as a zero count, are
// refused, while a false switch is a value. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue]. The
// codecs validate representation invariants; they claim no server support.
func TablePartitioningCodecs() []schemaext.Codec {
	return settingsCodecs(TablePartitioningWireDefinition, tablePartitioningShape,
		schemaext.ModelCodec[*DesiredTablePartitioning]{
			Prototype: &DesiredTablePartitioning{}, Validate: ValidateDesiredTablePartitioning, Clone: (*DesiredTablePartitioning).Copy,
		},
		schemaext.ModelCodec[*ObservedTablePartitioning]{
			Prototype: &ObservedTablePartitioning{}, Validate: ValidateObservedTablePartitioning, Clone: (*ObservedTablePartitioning).Copy,
		},
	)
}

// TablePartitioningCoverage records what one source knows about YDB table
// partitioning. knowledge is the claim for every table the source describes;
// subjects override it for individual tables. A desired source that can
// declare the settings records complete knowledge. A read records complete
// knowledge for the tables it returned.
func TablePartitioningCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return tablePartitioningCoverage(TablePartitioningKind, representation, knowledge, subjects)
}

// tablePartitioningCoverage builds the table partitioning model registry once.
var tablePartitioningCoverage = schemaext.OwnedCoverageSource(Owner, TablePartitioningCodecs)
