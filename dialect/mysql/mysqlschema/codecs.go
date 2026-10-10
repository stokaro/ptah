package mysqlschema

import (
	_ "embed" // Embed the definition that identifies the table options wire format.
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

//go:embed table-codecs.json
var tableDefinition []byte

// TableWireDefinition returns an independent description of the table options
// wire model. The definition covers both representations.
func TableWireDefinition() json.RawMessage { return slices.Clone(tableDefinition) }

// The wire shapes come from the JSON tags of the model types, which are what
// the encoder writes, so the keys are spelled in one place.
var (
	desiredTableShape  = wireShape[DesiredTable]("MySQL table options")
	observedTableShape = wireShape[ObservedTable]("MySQL table options")
)

// TableCodecs returns the version-one desired and observed table options
// codecs, in that order. Each call returns independent definitions. A decoder
// accepts only the spelling the encoder writes: a key in another letter case,
// a null, and an omitted option written out as empty are refused. Every
// refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue]. The codecs validate representation invariants;
// they claim no server support.
func TableCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredTable]{
			Prototype: &DesiredTable{}, Representation: schemaext.Desired, Version: 1, Definition: TableWireDefinition(),
			Shape: shape(desiredTableShape), Validate: ValidateDesiredTable,
		}.Codec(),
		schemaext.ModelCodec[*ObservedTable]{
			Prototype: &ObservedTable{}, Representation: schemaext.Observed, Version: 1, Definition: TableWireDefinition(),
			Shape: shape(observedTableShape), Validate: ValidateObservedTable,
		}.Codec(),
	}
}

// TableCoverage records what one source knows about MySQL table options.
// knowledge is the claim for every table the source describes; subjects
// override it for individual tables.
func TableCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return coverage(TableKind, TableCodecs(), representation, knowledge, subjects)
}

// coverage records one claim about kind, whose models codecs define.
func coverage(kind schemaext.Kind, codecs []schemaext.Codec, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, len(codecs))
	for _, codec := range codecs {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == kind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: %s coverage requires a schema representation", schemaext.ErrInvalidValue, kind)
}

func shape(object schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, object)
		return err
	}
}

// wireShape derives the strict shape of T's JSON object from its tags. Every
// tagged field is allowed. One without omitempty is required, and one with it
// is NonEmpty, since its encoder never writes its empty value.
func wireShape[T any](name string) schemaext.ObjectShape {
	shape := schemaext.ObjectShape{Name: name}
	for field := range reflect.TypeFor[T]().Fields() {
		key, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		shape.Allowed = append(shape.Allowed, key)
		if options == "omitempty" {
			shape.NonEmpty = append(shape.NonEmpty, key)
			continue
		}
		shape.Required = append(shape.Required, key)
	}
	return shape
}

//go:embed index-codecs.json
var indexDefinition []byte

// IndexWireDefinition returns an independent description of the index options
// wire model. The definition covers both representations.
func IndexWireDefinition() json.RawMessage { return slices.Clone(indexDefinition) }

var (
	desiredIndexShape  = wireShape[DesiredIndex]("MySQL index options")
	observedIndexShape = wireShape[ObservedIndex]("MySQL index options")
)

// IndexCodecs returns the version-one desired and observed index options
// codecs, in that order, with the strictness [TableCodecs] has.
func IndexCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredIndex]{
			Prototype: &DesiredIndex{}, Representation: schemaext.Desired, Version: 1, Definition: IndexWireDefinition(),
			Shape: shape(desiredIndexShape), Validate: ValidateDesiredIndex,
		}.Codec(),
		schemaext.ModelCodec[*ObservedIndex]{
			Prototype: &ObservedIndex{}, Representation: schemaext.Observed, Version: 1, Definition: IndexWireDefinition(),
			Shape: shape(observedIndexShape), Validate: ValidateObservedIndex,
		}.Codec(),
	}
}

//go:embed index-block-size-codecs.json
var indexBlockSizeDefinition []byte

// IndexBlockSizeWireDefinition returns an independent description of the
// index block-size wire model. The definition covers both representations.
func IndexBlockSizeWireDefinition() json.RawMessage { return slices.Clone(indexBlockSizeDefinition) }

var (
	desiredIndexBlockSizeShape  = wireShape[DesiredIndexBlockSize]("MySQL index block size")
	observedIndexBlockSizeShape = wireShape[ObservedIndexBlockSize]("MySQL index block size")
)

// IndexBlockSizeCodecs returns the version-one desired and observed index
// block-size codecs, in that order, with the strictness [TableCodecs] has.
func IndexBlockSizeCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredIndexBlockSize]{
			Prototype: &DesiredIndexBlockSize{}, Representation: schemaext.Desired, Version: 1, Definition: IndexBlockSizeWireDefinition(),
			Shape: shape(desiredIndexBlockSizeShape), Validate: ValidateDesiredIndexBlockSize,
		}.Codec(),
		schemaext.ModelCodec[*ObservedIndexBlockSize]{
			Prototype: &ObservedIndexBlockSize{}, Representation: schemaext.Observed, Version: 1, Definition: IndexBlockSizeWireDefinition(),
			Shape: shape(observedIndexBlockSizeShape), Validate: ValidateObservedIndexBlockSize,
		}.Codec(),
	}
}

// IndexBlockSizeCoverage records what one source knows about index block
// sizes. knowledge is the claim for every index the source describes;
// subjects override it for individual indexes. A source that can spell the
// hint, or that removes it on apply as an HCL file does, claims
// [schemaext.Complete]: an index it declares without one requests none.
func IndexBlockSizeCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return coverage(IndexBlockSizeKind, IndexBlockSizeCodecs(), representation, knowledge, subjects)
}
