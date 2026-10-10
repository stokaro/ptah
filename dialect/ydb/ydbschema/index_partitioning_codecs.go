package ydbschema

import (
	_ "embed" // Embed the definition that identifies the index partitioning wire format.
	"encoding/json"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed index-partitioning-codecs.json
var indexPartitioningDefinition []byte

// IndexPartitioningWireDefinition returns an independent description of the
// index partitioning wire model. The definition covers both representations.
func IndexPartitioningWireDefinition() json.RawMessage {
	return slices.Clone(indexPartitioningDefinition)
}

var indexPartitioningShape = wireShape[IndexPartitioning]("YDB index partitioning")

// IndexPartitioningCodecs returns the version-one desired and observed index
// partitioning codecs, in that order, decoding only the spelling the encoder
// writes, as [TablePartitioningCodecs] does.
func IndexPartitioningCodecs() []schemaext.Codec {
	return settingsCodecs(IndexPartitioningWireDefinition, indexPartitioningShape,
		schemaext.ModelCodec[*DesiredIndexPartitioning]{
			Prototype: &DesiredIndexPartitioning{}, Validate: ValidateDesiredIndexPartitioning, Clone: (*DesiredIndexPartitioning).Copy,
		},
		schemaext.ModelCodec[*ObservedIndexPartitioning]{
			Prototype: &ObservedIndexPartitioning{}, Validate: ValidateObservedIndexPartitioning, Clone: (*ObservedIndexPartitioning).Copy,
		},
	)
}

// IndexPartitioningCoverage records what one source knows about YDB index
// partitioning. knowledge is the claim for every index the source describes;
// subjects override it for individual indexes.
func IndexPartitioningCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return indexPartitioningCoverage(IndexPartitioningKind, representation, knowledge, subjects)
}

var indexPartitioningCoverage = schemaext.OwnedCoverageSource(Owner, IndexPartitioningCodecs)
