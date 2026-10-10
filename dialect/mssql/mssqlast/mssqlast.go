// Package mssqlast holds the operation that carries one SQL Server security
// policy change to rendering, and its codec. The operation names the policy
// and carries the whole change, so the renderer decides the statements from
// both sides rather than from a verb.
package mssqlast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// SecurityPolicyKind identifies the security policy operation.
const SecurityPolicyKind schemaext.Kind = "ptah.run/mssql/security-policy-operation"

// SecurityPolicy creates, alters, replaces or drops the policy Schema.Name as
// Change describes.
type SecurityPolicy struct {
	Schema string                   `json:"schema"`
	Name   string                   `json:"name"`
	Change mssqldiff.SecurityPolicy `json:"change"`
}

// Kind returns the operation identity.
func (*SecurityPolicy) Kind() schemaext.Kind { return SecurityPolicyKind }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *SecurityPolicy) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [SecurityPolicy.CloneExtension] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *SecurityPolicy) Copy() *SecurityPolicy {
	if v == nil {
		return nil
	}
	return &SecurityPolicy{Schema: v.Schema, Name: v.Name, Change: *v.Change.Copy()}
}

// Subject returns the policy identity the operation is about.
func (v *SecurityPolicy) Subject() objectidentity.ID {
	return mssqlschema.SecurityPolicyRef(v.Schema, v.Name)
}

// Validate requires a policy identity [mssqlschema.ValidateSecurityPolicyRef]
// accepts and a valid change. Every refusal wraps [schemaext.ErrInvalidValue].
func (v *SecurityPolicy) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: security policy operation is nil", schemaext.ErrInvalidValue)
	}
	if v.Schema == "" {
		return fmt.Errorf("%w: a security policy operation names its schema", schemaext.ErrInvalidValue)
	}
	if err := mssqlschema.ValidateSecurityPolicyRef(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// RequiredCapability is the key a target must claim to render the operation.
func (*SecurityPolicy) RequiredCapability() capability.Capability { return capability.RowLevelSecurity }

// OmissionSubject names the operation in a skip line.
func (v *SecurityPolicy) OmissionSubject() (kind, name string) {
	return "security policy", mssqlschema.ObjectName{Schema: v.Schema, Name: v.Name}.String()
}

// Effect is the change's lifecycle effect.
func (v *SecurityPolicy) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// AccessEffect is the change's access assessment.
func (v *SecurityPolicy) AccessEffect() schemaext.AccessEffect {
	if v == nil {
		return schemaext.AccessEffect{}
	}
	return v.Change.Access
}

// SchemaChange describes the operation for change reports.
func (v *SecurityPolicy) SchemaChange() ast.ExtensionChange {
	name := mssqlschema.ObjectName{Schema: v.Schema, Name: v.Name}.String()
	switch {
	case v.Change.Before == nil:
		return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: name}
	case v.Change.After == nil:
		return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: name}
	default:
		return ast.ExtensionChange{Action: ast.ExtensionModify, Name: name}
	}
}

var operationShape = schemaext.ObjectShape{Name: "security policy operation",
	Allowed: []string{"schema", "name", "change"}, Required: []string{"schema", "name", "change"}}

// Codecs returns the version-one operation codec. The change takes its own
// codec's wire form, checked by that codec. Every refusal is a
// [schemaext.InvalidModelError].
func Codecs() []schemaext.Codec {
	change := mssqldiff.Codec()
	definition := `{"type":"object","required":["schema","name","change"],"additionalProperties":false,` +
		`"properties":{"schema":{"type":"string","pattern":"\\S"},"name":{"type":"string","pattern":"\\S"},"change":` +
		string(change.Definition) + `}}`
	return []schemaext.Codec{schemaext.ModelCodec[*SecurityPolicy]{
		Prototype: &SecurityPolicy{}, Representation: schemaext.Operation, Version: 1, Definition: json.RawMessage(definition),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, operationShape)
			if err != nil {
				return err
			}
			_, err = change.Decode(fields["change"])
			return err
		},
		Validate: (*SecurityPolicy).Validate,
		Clone:    (*SecurityPolicy).Copy,
	}.Codec()}
}
