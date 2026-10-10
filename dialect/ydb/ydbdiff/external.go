package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalDataSourceKind identifies a change to one YDB external data source.
const ExternalDataSourceKind schemaext.Kind = "ptah.run/ydb/external-data-source-change"

// ExternalTableKind identifies a change to one YDB external table.
const ExternalTableKind schemaext.Kind = "ptah.run/ydb/external-table-change"

// ExternalDataSource captures both operands of a change to one data source. A
// nil Before means the database holds no data source at the path; a nil After
// means the declaration drops it. A nil side is established absence.
type ExternalDataSource struct {
	Before *ydbexternal.ObservedSource `json:"before"`
	After  *ydbexternal.DesiredSource  `json:"after"`
}

// ExternalTable captures both operands of a change to one external table, as
// [ExternalDataSource] does. Both operands may describe the same table: the
// comparison asks for such a table when the plan drops the data source it
// reads and creates it again, which takes the table along.
type ExternalTable struct {
	Before *ydbexternal.ObservedTable `json:"before"`
	After  *ydbexternal.DesiredTable  `json:"after"`
}

// Kind returns the stable change identity.
func (*ExternalDataSource) Kind() schemaext.Kind { return ExternalDataSourceKind }

// Kind returns the stable change identity.
func (*ExternalTable) Kind() schemaext.Kind { return ExternalTableKind }

// CloneChange returns independent before and after snapshots.
func (v *ExternalDataSource) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*ExternalDataSource)(nil)
	}
	cloned := &ExternalDataSource{}
	if v.Before != nil {
		cloned.Before, _ = v.Before.Clone().(*ydbexternal.ObservedSource)
	}
	if v.After != nil {
		cloned.After, _ = v.After.Clone().(*ydbexternal.DesiredSource)
	}
	return cloned
}

// CloneChange returns independent before and after snapshots.
func (v *ExternalTable) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*ExternalTable)(nil)
	}
	cloned := &ExternalTable{}
	if v.Before != nil {
		cloned.Before, _ = v.Before.Clone().(*ydbexternal.ObservedTable)
	}
	if v.After != nil {
		cloned.After, _ = v.After.Clone().(*ydbexternal.DesiredTable)
	}
	return cloned
}

// Validate refuses a change without operands and an operand no statement can
// carry.
func (v *ExternalDataSource) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an external data source change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := v.Before.Spec.Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
	}
	return nil
}

// Validate refuses a change without operands and an operand no statement can
// carry.
func (v *ExternalTable) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an external table change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := v.Before.Spec.Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
	}
	return nil
}

// Displaces reports a change to a data source both sides hold that the
// external tables over it cannot stay through, so the plan drops each one
// before the change and creates it again after: on a target without
// [capability.ExternalObjectReplace] the plan drops the source and creates it
// again, and YDB keeps no table over a dropped source; with it, CREATE OR
// REPLACE refuses a new source type while a table reads the source (measured
// on 26.2.1.14). The comparison and the planner ask this one predicate, since
// the comparison asks for the tables the planner recreates.
func (v *ExternalDataSource) Displaces(caps capability.Capabilities) bool {
	return v != nil && v.Before != nil && v.After != nil &&
		(!caps.Has(capability.ExternalObjectReplace) || v.Before.Spec.SourceType != v.After.Spec.SourceType)
}

// Effect records what the change does. No external object holds data in YDB,
// so a creation adds and a drop or a replacement changes what queries that
// read the object read.
func (v *ExternalDataSource) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE EXTERNAL DATA SOURCE adds a data source"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.DropSourceReason}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.ReplaceReason}
	}
}

// Effect records what the change does, as [ExternalDataSource.Effect] does.
func (v *ExternalTable) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE EXTERNAL TABLE adds an external table"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.DropTableReason}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.ReplaceReason}
	}
}

// ExternalDataSourceCodec describes the complete before/after wire for one
// data source change.
func ExternalDataSourceCodec() schemaext.Codec {
	models := ydbexternal.Codecs()[:2]
	return externalChangeCodec(&ExternalDataSource{}, models, "external data source",
		func(data json.RawMessage) (schemaext.Payload, error) {
			before, after, err := decodeStandaloneOperands[*ydbexternal.ObservedSource, *ydbexternal.DesiredSource](data, "external data source", models)
			if err != nil {
				return nil, err
			}
			return validatedChange(&ExternalDataSource{Before: before, After: after})
		})
}

// ExternalTableCodec describes the complete before/after wire for one
// external table change.
func ExternalTableCodec() schemaext.Codec {
	models := ydbexternal.Codecs()[2:]
	return externalChangeCodec(&ExternalTable{}, models, "external table",
		func(data json.RawMessage) (schemaext.Payload, error) {
			before, after, err := decodeStandaloneOperands[*ydbexternal.ObservedTable, *ydbexternal.DesiredTable](data, "external table", models)
			if err != nil {
				return nil, err
			}
			return validatedChange(&ExternalTable{Before: before, After: after})
		})
}

// externalChange is a change value that validates itself.
type externalChange interface {
	schemaext.ChangeValue
	Validate() error
}

func externalChangeCodec(prototype externalChange, models []schemaext.Codec, family string, decode func(json.RawMessage) (schemaext.Payload, error)) schemaext.Codec {
	value := func(payload schemaext.Payload) (externalChange, error) {
		change, ok := payload.(externalChange)
		if !ok || change.Kind() != prototype.Kind() {
			return nil, fmt.Errorf("%w: expected an %s change, got %T", schemaext.ErrInvalidValue, family, payload)
		}
		if err := change.Validate(); err != nil {
			return nil, err
		}
		return change, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		change, err := value(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(change)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			change, err := value(payload)
			if err != nil {
				return nil, err
			}
			return change.CloneChange(), nil
		},
		Decode: decode,
	}
}

func validatedChange(change externalChange) (schemaext.Payload, error) {
	if err := change.Validate(); err != nil {
		return nil, err
	}
	return change, nil
}
