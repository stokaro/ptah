package pgdefaultprivilege

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/internal/pgprivilege"
)

// OperationKind identifies one ALTER DEFAULT PRIVILEGES statement.
const OperationKind schemaext.Kind = "ptah.run/pgdefaultprivilege/default-privilege-operation"

// Action identifies a grant, a revoke, or a grant-option-only revoke.
type Action string

const (
	// GrantRights adds rights, with their grant options when requested.
	GrantRights Action = "grant"
	// RevokeRights removes rights and their grant options.
	RevokeRights Action = "revoke"
	// RevokeGrantOptions removes grant options while retaining the rights.
	RevokeGrantOptions Action = "revoke-grant-options"
)

// Operation carries one typed statement. All listed privileges share one grant
// option; mixed grants use separate operations. Identity never defaults a schema.
type Operation struct {
	Identity   Identity `json:"identity"`
	Action     Action   `json:"action"`
	Privileges []string `json:"privileges"`
	WithOption bool     `json:"with_option,omitempty"`
}

// Kind returns the stable operation identity.
func (*Operation) Kind() schemaext.Kind { return OperationKind }

// CloneExtension returns an independent operation, preserving a typed nil.
func (v *Operation) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy returns an operation with an independent privilege list. Nil stays nil.
func (v *Operation) Copy() *Operation {
	if v == nil {
		return nil
	}
	result := *v
	result.Privileges = slices.Clone(v.Privileges)
	return &result
}

// Validate requires an identity, a known action, and privilege keywords accepted
// for the object class. Identifiers are data; privilege names are SQL keywords.
func (v *Operation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil default privilege operation", schemaext.ErrInvalidValue)
	}
	if err := v.Identity.Validate(); err != nil {
		return err
	}
	if v.Action != GrantRights && v.Action != RevokeRights && v.Action != RevokeGrantOptions {
		return fmt.Errorf("%w: unknown default privilege action %q", schemaext.ErrInvalidValue, v.Action)
	}
	if v.WithOption && v.Action != GrantRights {
		return fmt.Errorf("%w: only a grant takes WITH GRANT OPTION", schemaext.ErrInvalidValue)
	}
	if len(v.Privileges) == 0 {
		return fmt.Errorf("%w: default privilege operation needs privileges", schemaext.ErrInvalidValue)
	}
	for _, name := range canonicalNames(v.Privileges) {
		if name != "ALL" && !slices.Contains(pgprivilege.All(v.Identity.ObjectType), name) {
			return fmt.Errorf("%w: default privilege %q is not valid on %s", schemaext.ErrInvalidValue, name, v.Identity.ObjectType)
		}
	}
	return nil
}

// RequiredCapability names the target capability that gates this statement.
func (*Operation) RequiredCapability() capability.Capability { return capability.RoleManagement }

// OmissionSubject names the default ACL omitted by capability gating.
func (v *Operation) OmissionSubject() (kind, name string) {
	return "default privilege", v.Identity.String()
}

// SchemaChange reports an alteration of the rights on future objects.
func (v *Operation) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Identity.String()}
}

// Effect records that the statement changes access to future objects.
func (v *Operation) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes privileges on future objects"}
}

const identityDefinition = `{"type":"object","required":["grantor","schema","object_type","grantee"],"additionalProperties":false,"properties":{` +
	`"grantor":{"type":"string","minLength":1},"schema":{"type":"string"},"object_type":{"type":"string","minLength":1},"grantee":{"type":"string","minLength":1}}}`

// OperationCodec encodes one statement with strict nested identity keys.
func OperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*Operation]{Prototype: &Operation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["identity","action","privileges"],"additionalProperties":false,"properties":{` +
			`"identity":` + identityDefinition + `,"action":{"enum":["grant","revoke","revoke-grant-options"]},` +
			`"privileges":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},"with_option":{"const":true}}}`),
		Shape: operationShape, Validate: (*Operation).Validate, Canonical: canonicalOperation, Clone: (*Operation).Copy}.Codec()
}

func operationShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "default privilege operation",
		Allowed: []string{"identity", "action", "privileges", "with_option"}, Required: []string{"identity", "action", "privileges"}, NonEmpty: []string{"privileges", "with_option"}})
	if err != nil {
		return err
	}
	keys := []string{"grantor", "schema", "object_type", "grantee"}
	_, err = schemaext.DecodeObject(fields["identity"], schemaext.ObjectShape{Name: "default privilege identity", Allowed: keys, Required: keys})
	return err
}

func canonicalOperation(value *Operation) *Operation {
	result := value.Copy()
	result.Identity = value.Identity.canonical()
	result.Privileges = canonicalNames(value.Privileges)
	return result
}
