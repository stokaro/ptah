package pgpolicy

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
)

const (
	// PolicyOperationKind identifies a statement that creates, drops, changes
	// or comments one policy.
	PolicyOperationKind schemaext.Kind = "ptah.run/pgpolicy/policy-operation"
	// PolicyCommentOperationKind identifies the statement that sets one
	// policy's comment.
	PolicyCommentOperationKind schemaext.Kind = "ptah.run/pgpolicy/policy-comment-operation"
	// TableStateOperationKind identifies the statements that set a table's
	// row-security switches.
	TableStateOperationKind schemaext.Kind = "ptah.run/pgpolicy/table-state-operation"
)

// PolicyOperation carries one policy transition through the common AST
// extension envelope: CREATE POLICY, DROP POLICY, or a replacement that drops
// the policy and creates it again. Schema, Table and Name are the names as the
// source spelled them; Schema is empty when the source left the table in the
// default schema. The change holds both operands and the access assessment the
// plan reports for the statements this operation renders. A policy's comment is
// a [PolicyCommentOperation] of its own, because a target can hold policies and
// not their comments.
type PolicyOperation struct {
	Schema string       `json:"schema"`
	Table  string       `json:"table"`
	Name   string       `json:"name"`
	Change PolicyChange `json:"change"`
}

// Kind returns the stable operation identity.
func (*PolicyOperation) Kind() schemaext.Kind { return PolicyOperationKind }

// CloneExtension returns an independent operation and both operands.
func (v *PolicyOperation) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*PolicyOperation)(nil)
	}
	return &PolicyOperation{Schema: v.Schema, Table: v.Table, Name: v.Name, Change: *v.Change.Copy()}
}

// Subject returns the policy's identity under PostgreSQL's identifier rules.
func (v *PolicyOperation) Subject() objectidentity.ID { return PolicyRef(v.Schema, v.Table, v.Name) }

// QualifiedTable is the table a statement names: schema.table when the source
// wrote the schema, the bare table otherwise.
func (v *PolicyOperation) QualifiedTable() string { return qualified(v.Schema, v.Table) }

// Validate refuses an invalid identity or operands.
func (v *PolicyOperation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: policy operation is nil", schemaext.ErrInvalidValue)
	}
	if err := validTexts("schema", v.Schema, "table", v.Table, "policy name", v.Name); err != nil {
		return err
	}
	if err := ValidatePolicyRef(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// RequiredCapability names the capability a target needs for the statement.
func (*PolicyOperation) RequiredCapability() capability.Capability {
	return capability.RowLevelSecurity
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *PolicyOperation) OmissionSubject() (kind, name string) {
	return "policy", v.Name + " on " + v.QualifiedTable()
}

// Effect is the change's own effect on the policy object.
func (v *PolicyOperation) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// AccessEffect is the assessment the plan reports for this operation.
func (v *PolicyOperation) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Change.Access
}

// SchemaChange reports the logical action for schema-change reports.
func (v *PolicyOperation) SchemaChange() ast.ExtensionChange {
	name := v.Name + " on " + v.QualifiedTable()
	switch {
	case v.Change.Before == nil:
		return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: name}
	case v.Change.After == nil:
		return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: name}
	default:
		return ast.ExtensionChange{Action: ast.ExtensionModify, Name: name}
	}
}

// PolicyCommentOperation sets one policy's comment: COMMENT ON POLICY, with
// NULL for an empty Comment. It changes who may read nothing, so its access
// effect is unchanged.
type PolicyCommentOperation struct {
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	Name    string `json:"name"`
	Comment string `json:"comment"`
}

// Kind returns the stable operation identity.
func (*PolicyCommentOperation) Kind() schemaext.Kind { return PolicyCommentOperationKind }

// CloneExtension returns an independent operation.
func (v *PolicyCommentOperation) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*PolicyCommentOperation)(nil)
	}
	return new(*v)
}

// Subject returns the policy's identity under PostgreSQL's identifier rules.
func (v *PolicyCommentOperation) Subject() objectidentity.ID {
	return PolicyRef(v.Schema, v.Table, v.Name)
}

// QualifiedTable is the table a statement names.
func (v *PolicyCommentOperation) QualifiedTable() string { return qualified(v.Schema, v.Table) }

// Validate refuses an invalid identity or comment text.
func (v *PolicyCommentOperation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: policy comment operation is nil", schemaext.ErrInvalidValue)
	}
	if err := validTexts("schema", v.Schema, "table", v.Table, "policy name", v.Name, "comment", v.Comment); err != nil {
		return err
	}
	return ValidatePolicyRef(v.Subject())
}

// RequiredCapability names the capability a target needs for the statement.
// A target that holds policies but not their comments writes a skip line.
func (*PolicyCommentOperation) RequiredCapability() capability.Capability {
	return capability.PolicyComments
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *PolicyCommentOperation) OmissionSubject() (kind, name string) {
	return "policy comment", v.Name + " on " + v.QualifiedTable()
}

// Effect records that only documentation changes.
func (v *PolicyCommentOperation) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "sets the comment of a row-security policy"}
}

// AccessEffect is unchanged: a comment admits and hides no row.
func (v *PolicyCommentOperation) AccessEffect() schemaext.AccessEffect {
	return schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: reasonCommentOnly}
}

// TableStateOperation sets a table's row-security switches. Schema and Table
// are the names as the source spelled them.
type TableStateOperation struct {
	Schema string           `json:"schema"`
	Table  string           `json:"table"`
	Change TableStateChange `json:"change"`
}

// Kind returns the stable operation identity.
func (*TableStateOperation) Kind() schemaext.Kind { return TableStateOperationKind }

// CloneExtension returns an independent operation and both operands.
func (v *TableStateOperation) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*TableStateOperation)(nil)
	}
	return &TableStateOperation{Schema: v.Schema, Table: v.Table, Change: *v.Change.Copy()}
}

// QualifiedTable is the table a statement names.
func (v *TableStateOperation) QualifiedTable() string { return qualified(v.Schema, v.Table) }

// Validate refuses an unnamed table and invalid operands.
func (v *TableStateOperation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: row-security table operation is nil", schemaext.ErrInvalidValue)
	}
	if strings.TrimSpace(v.Table) == "" {
		return fmt.Errorf("%w: a row-security table operation requires its table", schemaext.ErrInvalidValue)
	}
	if err := validTexts("schema", v.Schema, "table", v.Table); err != nil {
		return err
	}
	return v.Change.Validate()
}

// RequiredCapability names the capability a target needs for the statements.
func (*TableStateOperation) RequiredCapability() capability.Capability {
	return capability.RowLevelSecurity
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *TableStateOperation) OmissionSubject() (kind, name string) {
	return "row-level security", "on " + v.QualifiedTable()
}

// Effect is the change's own effect on the table.
func (v *TableStateOperation) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// AccessEffect is the assessment the plan reports for this operation.
func (v *TableStateOperation) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Change.Access
}

func qualified(schema, name string) string {
	if strings.TrimSpace(schema) == "" {
		return name
	}
	return schema + "." + name
}

// OperationCodecs returns the version-one operation codecs.
func OperationCodecs() []schemaext.Codec {
	return []schemaext.Codec{policyOperationCodec(), policyCommentOperationCodec(), tableStateOperationCodec()}
}

func policyOperationCodec() schemaext.Codec {
	policyChange := PolicyChangeCodec()
	return operationCodec(&PolicyOperation{},
		fmt.Sprintf(`{"type":"object","required":["schema","table","name","change"],"additionalProperties":false,`+
			`"properties":{"schema":{"type":"string"},"table":{"type":"string","minLength":1},"name":{"type":"string","minLength":1},"change":%s}}`,
			policyChange.Definition),
		func(payload schemaext.Payload) (json.RawMessage, error) {
			value, ok := payload.(*PolicyOperation)
			if !ok {
				return nil, fmt.Errorf("%w: expected a policy operation, got %T", schemaext.ErrInvalidValue, payload)
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			change, err := policyChange.Encode(&value.Change)
			if err != nil {
				return nil, err
			}
			return json.Marshal(struct {
				Schema string          `json:"schema"`
				Table  string          `json:"table"`
				Name   string          `json:"name"`
				Change json.RawMessage `json:"change"`
			}{value.Schema, value.Table, value.Name, change})
		},
		func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := operationFields(data, []string{"schema", "table", "name", "change"})
			if err != nil {
				return nil, err
			}
			change, err := policyChange.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			value := &PolicyOperation{Change: *change.(*PolicyChange)}
			if err := decodeNames(fields, map[string]*string{"schema": &value.Schema, "table": &value.Table, "name": &value.Name}); err != nil {
				return nil, err
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return value, nil
		})
}

func policyCommentOperationCodec() schemaext.Codec {
	return operationCodec(&PolicyCommentOperation{},
		`{"type":"object","required":["schema","table","name","comment"],"additionalProperties":false,`+
			`"properties":{"schema":{"type":"string"},"table":{"type":"string","minLength":1},"name":{"type":"string","minLength":1},"comment":{"type":"string"}}}`,
		func(payload schemaext.Payload) (json.RawMessage, error) {
			value, ok := payload.(*PolicyCommentOperation)
			if !ok {
				return nil, fmt.Errorf("%w: expected a policy comment operation, got %T", schemaext.ErrInvalidValue, payload)
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
		func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := operationFields(data, []string{"schema", "table", "name", "comment"})
			if err != nil {
				return nil, err
			}
			value := &PolicyCommentOperation{}
			if err := decodeNames(fields, map[string]*string{"schema": &value.Schema, "table": &value.Table, "name": &value.Name, "comment": &value.Comment}); err != nil {
				return nil, err
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return value, nil
		})
}

func tableStateOperationCodec() schemaext.Codec {
	tableChange := TableStateChangeCodec()
	return operationCodec(&TableStateOperation{},
		fmt.Sprintf(`{"type":"object","required":["schema","table","change"],"additionalProperties":false,`+
			`"properties":{"schema":{"type":"string"},"table":{"type":"string","minLength":1},"change":%s}}`, tableChange.Definition),
		func(payload schemaext.Payload) (json.RawMessage, error) {
			value, ok := payload.(*TableStateOperation)
			if !ok {
				return nil, fmt.Errorf("%w: expected a row-security table operation, got %T", schemaext.ErrInvalidValue, payload)
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
		func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := operationFields(data, []string{"schema", "table", "change"})
			if err != nil {
				return nil, err
			}
			change, err := tableChange.Decode(fields["change"])
			if err != nil {
				return nil, err
			}
			value := &TableStateOperation{Change: *change.(*TableStateChange)}
			if err := decodeNames(fields, map[string]*string{"schema": &value.Schema, "table": &value.Table}); err != nil {
				return nil, err
			}
			if err := value.Validate(); err != nil {
				return nil, err
			}
			return value, nil
		})
}

func operationCodec(prototype ast.ExtensionPayload, definition string, encode func(schemaext.Payload) (json.RawMessage, error),
	decode func(json.RawMessage) (schemaext.Payload, error),
) schemaext.Codec {
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1, Definition: json.RawMessage(definition),
		Encode: encode, Canonical: encode, Decode: decode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := encode(payload); err != nil {
				return nil, err
			}
			return payload.(ast.ExtensionPayload).CloneExtension(), nil
		},
	}
}

// operationFields decodes an operation object and requires exactly keys.
func operationFields(data json.RawMessage, keys []string) (map[string]json.RawMessage, error) {
	return schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "row-security operation", Allowed: keys, Required: keys})
}

func decodeNames(fields map[string]json.RawMessage, names map[string]*string) error {
	for key, target := range names {
		value, err := schemaext.DecodeJSON[string](fields[key])
		if err != nil {
			return err
		}
		*target = value
	}
	return nil
}
