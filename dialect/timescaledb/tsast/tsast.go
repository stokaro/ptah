// Package tsast defines TimescaleDB operation payloads carried through the
// common AST extension envelope. A payload holds complete operands and its
// safety effect; it does not render SQL, select a provider or access a database.
package tsast

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
)

const (
	// CreateHypertableKind identifies the call that partitions an existing table.
	CreateHypertableKind schemaext.Kind = "ptah.run/timescaledb/create-hypertable"
	// ContinuousAggregateKind identifies a continuous aggregate transition.
	ContinuousAggregateKind schemaext.Kind = "ptah.run/timescaledb/continuous-aggregate-operation"
)

// CreateHypertable is the `create_hypertable` call for one table. It is a
// statement rather than an ALTER TABLE fragment, because TimescaleDB has no
// HYPERTABLE grammar: the call takes the table as a REGCLASS argument. Table is
// the name as the source spelled it, schema-qualified when the source was.
type CreateHypertable struct {
	Table      string                     `json:"table"`
	Hypertable tsschema.DesiredHypertable `json:"hypertable"`
}

// Kind returns the stable operation identity.
func (*CreateHypertable) Kind() schemaext.Kind { return CreateHypertableKind }

// CloneExtension returns an independent operation.
func (v *CreateHypertable) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*CreateHypertable)(nil)
	}
	return new(*v)
}

// Validate refuses an unnamed table and an invalid declaration.
func (v *CreateHypertable) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: hypertable operation is nil", schemaext.ErrInvalidValue)
	}
	if strings.TrimSpace(v.Table) == "" {
		return fmt.Errorf("%w: a hypertable operation requires its table", schemaext.ErrInvalidValue)
	}
	return tsschema.ValidateDesiredHypertable(&v.Hypertable)
}

// RequiredCapability names the capability a target needs for the call. A
// PostgreSQL-family target without it writes a skip line instead: the call
// fails on `function create_hypertable(unknown, unknown) does not exist` where
// the extension is not installed.
func (*CreateHypertable) RequiredCapability() capability.Capability { return capability.Hypertables }

// OmissionSubject names the object a skip line and an omission record name.
func (v *CreateHypertable) OmissionSubject() (kind, name string) { return "hypertable", v.Table }

// Effect is the partitioning change's own effect.
func (v *CreateHypertable) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return (&tsdiff.Hypertable{After: new(v.Hypertable)}).Effect()
}

// ContinuousAggregate creates, drops or replaces one aggregate. A nil Before
// requests CREATE; a nil After requests DROP; both request a replacement, which
// is a drop followed by a create because there is no CREATE OR REPLACE for one.
// Schema is empty when the source left the aggregate in the default schema.
type ContinuousAggregate struct {
	Schema string                     `json:"schema"`
	Name   string                     `json:"name"`
	Change tsdiff.ContinuousAggregate `json:"change"`
}

// Kind returns the stable operation identity.
func (*ContinuousAggregate) Kind() schemaext.Kind { return ContinuousAggregateKind }

// CloneExtension returns an independent operation and both operands.
func (v *ContinuousAggregate) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ContinuousAggregate)(nil)
	}
	return &ContinuousAggregate{Schema: v.Schema, Name: v.Name, Change: *v.Change.Copy()}
}

// Subject returns the aggregate's schema-scoped identity under PostgreSQL's
// identifier rules.
func (v *ContinuousAggregate) Subject() objectidentity.ID {
	return tsschema.ContinuousAggregateRef(v.Schema, v.Name)
}

// QualifiedName is the name a statement writes: schema.name when the source
// wrote the schema, the bare name otherwise.
func (v *ContinuousAggregate) QualifiedName() string {
	if strings.TrimSpace(v.Schema) == "" {
		return v.Name
	}
	return v.Schema + "." + v.Name
}

// Validate refuses an invalid identity or operands.
func (v *ContinuousAggregate) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: continuous aggregate operation is nil", schemaext.ErrInvalidValue)
	}
	if err := tsschema.ValidateContinuousAggregateRef(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// RequiredCapability names the capability a target needs for the statement.
func (*ContinuousAggregate) RequiredCapability() capability.Capability {
	return capability.ContinuousAggregates
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *ContinuousAggregate) OmissionSubject() (kind, name string) {
	return "continuous aggregate", v.Name
}

// Effect is the change's own effect on the aggregate's materialized data.
func (v *ContinuousAggregate) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// SchemaChange reports the logical action for schema-change reports.
func (v *ContinuousAggregate) SchemaChange() ast.ExtensionChange {
	switch {
	case v.Change.Before == nil:
		return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.QualifiedName()}
	case v.Change.After == nil:
		return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.QualifiedName()}
	default:
		return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.QualifiedName()}
	}
}

// Codecs returns the version-one operation codecs.
func Codecs() []schemaext.Codec {
	hypertable := tsschema.HypertableCodecs()[0]
	change := tsdiff.ContinuousAggregateCodec()
	return []schemaext.Codec{
		operationCodec(&CreateHypertable{},
			fmt.Sprintf(`{"type":"object","required":["table","hypertable"],"additionalProperties":false,`+
				`"properties":{"table":{"type":"string","minLength":1},"hypertable":%s}}`, hypertable.Definition),
			func(payload schemaext.Payload) error {
				value, ok := payload.(*CreateHypertable)
				if !ok {
					return fmt.Errorf("%w: expected a hypertable operation, got %T", schemaext.ErrInvalidValue, payload)
				}
				return value.Validate()
			}, decodeCreateHypertable),
		operationCodec(&ContinuousAggregate{},
			fmt.Sprintf(`{"type":"object","required":["schema","name","change"],"additionalProperties":false,`+
				`"properties":{"schema":{"type":"string"},"name":{"type":"string","minLength":1},"change":%s}}`, change.Definition),
			func(payload schemaext.Payload) error {
				value, ok := payload.(*ContinuousAggregate)
				if !ok {
					return fmt.Errorf("%w: expected a continuous aggregate operation, got %T", schemaext.ErrInvalidValue, payload)
				}
				return value.Validate()
			}, decodeContinuousAggregate),
	}
}

func operationCodec(prototype ast.ExtensionPayload, definition string, validate func(schemaext.Payload) error,
	decode func(json.RawMessage) (schemaext.Payload, error),
) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		if err := validate(payload); err != nil {
			return nil, err
		}
		return json.Marshal(payload)
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1, Definition: json.RawMessage(definition),
		Encode: encode, Canonical: encode, Decode: decode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(payload); err != nil {
				return nil, err
			}
			return payload.(ast.ExtensionPayload).CloneExtension(), nil
		},
	}
}

func decodeCreateHypertable(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := exactFields(data, "hypertable operation", "table", "hypertable")
	if err != nil {
		return nil, err
	}
	declared, err := tsschema.HypertableCodecs()[0].Decode(fields["hypertable"])
	if err != nil {
		return nil, err
	}
	table, err := schemaext.DecodeJSON[string](fields["table"])
	if err != nil {
		return nil, err
	}
	value := &CreateHypertable{Table: table, Hypertable: *declared.(*tsschema.DesiredHypertable)}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeContinuousAggregate(data json.RawMessage) (schemaext.Payload, error) {
	fields, err := exactFields(data, "continuous aggregate operation", "schema", "name", "change")
	if err != nil {
		return nil, err
	}
	operands, err := tsdiff.ContinuousAggregateCodec().Decode(fields["change"])
	if err != nil {
		return nil, err
	}
	schema, err := schemaext.DecodeJSON[string](fields["schema"])
	if err != nil {
		return nil, err
	}
	name, err := schemaext.DecodeJSON[string](fields["name"])
	if err != nil {
		return nil, err
	}
	value := &ContinuousAggregate{Schema: schema, Name: name, Change: *operands.(*tsdiff.ContinuousAggregate)}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func exactFields(data json.RawMessage, model string, names ...string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if len(fields) != len(names) {
		return nil, fmt.Errorf("%w: a TimescaleDB %s requires exactly %s", schemaext.ErrInvalidValue, model, strings.Join(names, ", "))
	}
	for _, name := range names {
		if len(fields[name]) == 0 || strings.TrimSpace(string(fields[name])) == "null" {
			return nil, fmt.Errorf("%w: a TimescaleDB %s requires %s", schemaext.ErrInvalidValue, model, name)
		}
	}
	return fields, nil
}
