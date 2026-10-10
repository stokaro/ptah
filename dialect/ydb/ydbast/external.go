package ydbast

import (
	"encoding/json"
	"fmt"
	"reflect"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalDataSourceKind identifies one statement on a YDB external data
// source.
const ExternalDataSourceKind schemaext.Kind = "ptah.run/ydb/external-data-source-operation"

// ExternalTableKind identifies one statement on a YDB external table.
const ExternalTableKind schemaext.Kind = "ptah.run/ydb/external-table-operation"

// ExternalOperation selects the statement on an external object.
type ExternalOperation string

const (
	// ExternalCreate writes CREATE EXTERNAL ...
	ExternalCreate ExternalOperation = "create"
	// ExternalReplace writes CREATE OR REPLACE EXTERNAL ..., which replaces
	// the object at the path in one statement.
	ExternalReplace ExternalOperation = "replace"
	// ExternalDrop writes DROP EXTERNAL ...
	ExternalDrop ExternalOperation = "drop"
	// ExternalRecreate writes DROP EXTERNAL DATA SOURCE and then CREATE
	// EXTERNAL DATA SOURCE, which a target without CREATE OR REPLACE changes a
	// data source with. It is one statement on the source to every reader:
	// one that uses the source as it was runs before it, and one that uses the
	// new source after it. An external table takes no recreation: the tables
	// over a recreated source are dropped before it and created after it.
	ExternalRecreate ExternalOperation = "recreate"
)

// ExternalDataSource is one statement on the data source Name in the
// directory Schema, relative to the database root. A drop carries no Spec. A
// zero value is invalid.
type ExternalDataSource struct {
	Operation ExternalOperation      `json:"operation"`
	Schema    string                 `json:"schema"`
	Name      string                 `json:"name"`
	Spec      ydbexternal.DataSource `json:"spec"`
}

// ExternalTable is one statement on the external table Name in the directory
// Schema, as [ExternalDataSource] is on a data source.
type ExternalTable struct {
	Operation ExternalOperation `json:"operation"`
	Schema    string            `json:"schema"`
	Name      string            `json:"name"`
	Spec      ydbexternal.Table `json:"spec"`
}

// Kind returns the stable operation identity.
func (*ExternalDataSource) Kind() schemaext.Kind { return ExternalDataSourceKind }

// Kind returns the stable operation identity.
func (*ExternalTable) Kind() schemaext.Kind { return ExternalTableKind }

// CloneExtension returns an independent operation.
func (v *ExternalDataSource) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ExternalDataSource)(nil)
	}
	cloned := *v
	cloned.Spec = v.Spec.Clone()
	return &cloned
}

// CloneExtension returns an independent operation.
func (v *ExternalTable) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ExternalTable)(nil)
	}
	cloned := *v
	cloned.Spec = v.Spec.Clone()
	return &cloned
}

// Subject returns the data source's schema-scoped identity.
func (v *ExternalDataSource) Subject() objectidentity.ID {
	return ydbexternal.SourceRef(v.Schema, v.Name)
}

// Subject returns the external table's schema-scoped identity.
func (v *ExternalTable) Subject() objectidentity.ID {
	return ydbexternal.TableRef(v.Schema, v.Name)
}

// Validate refuses an invalid path, an unknown operation, a creation whose
// spec no statement can carry, and a drop that carries a spec.
func (v *ExternalDataSource) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: external data source operation is nil", schemaext.ErrInvalidValue)
	}
	return validateExternal(v.Operation, v.Subject(), v.Spec.Validate, v.Spec)
}

// Validate refuses what [ExternalDataSource.Validate] refuses, and a
// recreation, which only a data source takes.
func (v *ExternalTable) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: external table operation is nil", schemaext.ErrInvalidValue)
	}
	if v.Operation == ExternalRecreate {
		return fmt.Errorf("%w: an external table is dropped and created in two operations, not recreated in one", schemaext.ErrInvalidValue)
	}
	return validateExternal(v.Operation, v.Subject(), v.Spec.Validate, v.Spec)
}

// validateExternal holds the statement operation on the object ref, whose
// spec is value, to what the operation needs of the spec: a creation one
// validate accepts, and a drop none.
func validateExternal(operation ExternalOperation, ref objectidentity.ID, validate func() error, value any) error {
	if err := ydbexternal.ValidateIdentity(ref); err != nil {
		return err
	}
	switch operation {
	case ExternalCreate, ExternalReplace, ExternalRecreate:
		if err := validate(); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		return nil
	case ExternalDrop:
		if !reflect.ValueOf(value).IsZero() {
			return fmt.Errorf("%w: a drop of %s carries no spec", schemaext.ErrInvalidValue, ref)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown external object operation %q", schemaext.ErrInvalidValue, operation)
	}
}

// Effect classifies the statement: a creation adds, and a replacement or a
// drop changes what queries reading the object read. No external object
// holds data in YDB. An invalid operation has unknown effects.
func (v *ExternalDataSource) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return externalEffect(v.Operation, "CREATE EXTERNAL DATA SOURCE adds a data source", ydbexternal.DropSourceReason)
}

// Effect classifies the statement as [ExternalDataSource.Effect] does.
func (v *ExternalTable) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return externalEffect(v.Operation, "CREATE EXTERNAL TABLE adds an external table", ydbexternal.DropTableReason)
}

func externalEffect(operation ExternalOperation, create, drop string) schemaext.Effect {
	switch operation {
	case ExternalCreate:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: create}
	case ExternalReplace, ExternalRecreate:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbexternal.ReplaceReason}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: drop}
	}
}

// ExternalDataSourceCodec describes the version-one operation wire: the
// statement, the separate directory and leaf names, and the spec, which a
// drop writes empty. It refuses missing, null and unknown fields.
func ExternalDataSourceCodec() schemaext.Codec {
	return externalOperationCodec(&ExternalDataSource{}, "external data source")
}

// ExternalTableCodec describes the version-one operation wire of an external
// table, as [ExternalDataSourceCodec] does.
func ExternalTableCodec() schemaext.Codec {
	return externalOperationCodec(&ExternalTable{}, "external table")
}

// externalOperation is the statement payload of either kind.
type externalOperation interface {
	ast.ExtensionPayload
	Validate() error
}

func externalOperationCodec(prototype externalOperation, family string) schemaext.Codec {
	validated := func(payload schemaext.Payload) (externalOperation, error) {
		value, ok := payload.(externalOperation)
		if !ok || value.Kind() != prototype.Kind() {
			return nil, fmt.Errorf("%w: expected an %s operation, got %T", schemaext.ErrInvalidValue, family, payload)
		}
		if err := value.Validate(); err != nil {
			return nil, &schemaext.InvalidModelError{Kind: prototype.Kind(), Representation: schemaext.Operation, Message: err.Error()}
		}
		return value, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["operation","schema","name","spec"],"additionalProperties":false,"properties":{` +
			`"operation":{"enum":["create","replace","recreate","drop"]},"schema":{"type":"string"},"name":{"type":"string","minLength":1},` +
			`"spec":{"type":"object"}}}`),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			for _, field := range []string{"operation", "schema", "name", "spec"} {
				if len(fields[field]) == 0 || string(fields[field]) == "null" {
					return nil, fmt.Errorf("%w: %s operation requires %s", schemaext.ErrInvalidValue, family, field)
				}
			}
			var value externalOperation
			switch prototype.(type) {
			case *ExternalDataSource:
				decoded, err := schemaext.DecodeJSON[*ExternalDataSource](data)
				if err != nil {
					return nil, err
				}
				value = decoded
			default:
				decoded, err := schemaext.DecodeJSON[*ExternalTable](data)
				if err != nil {
					return nil, err
				}
				value = decoded
			}
			return validated(value)
		},
	}
}
