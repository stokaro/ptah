package oratype

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// The operation kinds.
const (
	// CreateDomainKind identifies CREATE DOMAIN.
	CreateDomainKind schemaext.Kind = "ptah.run/oratype/create-domain"
	// ReplaceObjectTypeKind identifies CREATE OR REPLACE TYPE ... AS OBJECT.
	ReplaceObjectTypeKind schemaext.Kind = "ptah.run/oratype/replace-object-type"
	// DropKind identifies DROP DOMAIN and DROP TYPE.
	DropKind schemaext.Kind = "ptah.run/oratype/drop"
)

// OperationKinds are the three operation kinds, in the order the package
// declares them.
func OperationKinds() []schemaext.Kind {
	return []schemaext.Kind{CreateDomainKind, ReplaceObjectTypeKind, DropKind}
}

// Target names the type one operation is about: the kind of its model, its
// schema as the declaration wrote it, empty where it left the schema to the
// connection, and its name.
type Target struct {
	Kind   schemaext.Kind `json:"kind"`
	Schema string         `json:"schema"`
	Name   string         `json:"name"`
}

// TargetOf names the type ref identifies.
func TargetOf(ref objectidentity.ID) Target {
	return Target{Kind: schemaext.Kind(ref.Kind), Schema: AuthoredSchema(ref), Name: ref.Name.Source}
}

// Subject returns the type's identity under Oracle's identifier rules.
func (t Target) Subject() objectidentity.ID { return Ref(t.Kind, t.Schema, t.Name) }

// QualifiedName is the type's name before an Oracle statement spells it:
// schema.name where the schema was written, and the bare name otherwise.
func (t Target) QualifiedName() string { return tableref.Canonical(t.Schema, t.Name) }

// validTarget refuses a target of another kind than want, or of no owned kind
// where want is empty, and an invalid identity.
func validTarget(target Target, want ...schemaext.Kind) error {
	if len(want) > 0 && !slices.Contains(want, target.Kind) {
		return fmt.Errorf("%w: the operation names %q, not the %s kind", schemaext.ErrInvalidValue, target.Kind, labels[want[0]])
	}
	if err := validTexts("type", "schema", target.Schema); err != nil {
		return err
	}
	return ValidateRef(target.Subject())
}

// CreateDomain writes CREATE DOMAIN: the base type, NOT NULL, a default and a
// CHECK, after a comment line where Comment holds one. A default expression is
// written as it is, and a literal default as a SQL literal where it already
// reads as one and as a quoted string otherwise; a creation that names both
// writes the expression.
type CreateDomain struct {
	Target      Target `json:"target"`
	BaseType    string `json:"base_type"`
	NotNull     bool   `json:"not_null"`
	Default     string `json:"default"`
	DefaultExpr string `json:"default_expr"`
	Check       string `json:"check"`
	Comment     string `json:"comment"`
}

// CreateDomainOf is the creation of the domain declared at ref.
func CreateDomainOf(ref objectidentity.ID, domain *DesiredDomain) *CreateDomain {
	return &CreateDomain{Target: TargetOf(ref), BaseType: domain.BaseType, NotNull: domain.NotNull, Default: domain.Default,
		DefaultExpr: domain.DefaultExpr, Check: domain.Check, Comment: domain.Comment}
}

// ReplaceObjectType writes CREATE OR REPLACE TYPE ... AS OBJECT with the
// attributes in order, after a comment line where Comment holds one. One
// statement creates a type that is not there and rewrites one that is, so
// Replaces only says which of the two a plan meant: Oracle refuses to rewrite
// a type a column uses, with ORA-02303, and changes nothing.
type ReplaceObjectType struct {
	Target     Target      `json:"target"`
	Attributes []Attribute `json:"attributes"`
	Comment    string      `json:"comment"`
	Replaces   bool        `json:"replaces"`
}

// ReplaceObjectTypeOf is the statement that creates, or replaces where
// replaces is true, the object type declared at ref.
func ReplaceObjectTypeOf(ref objectidentity.ID, objectType *DesiredObjectType, replaces bool) *ReplaceObjectType {
	return &ReplaceObjectType{Target: TargetOf(ref), Attributes: slices.Clone(objectType.Attributes), Comment: objectType.Comment,
		Replaces: replaces}
}

// Drop writes DROP DOMAIN for a domain and DROP TYPE otherwise, without FORCE:
// Oracle refuses to drop a type a column still uses, and the drop fails
// rather than leaving the column naming a type the server has no definition
// of.
type Drop struct {
	Target Target `json:"target"`
}

// Kind returns the stable operation identity.
func (*CreateDomain) Kind() schemaext.Kind { return CreateDomainKind }

// Kind returns the stable operation identity.
func (*ReplaceObjectType) Kind() schemaext.Kind { return ReplaceObjectTypeKind }

// Kind returns the stable operation identity.
func (*Drop) Kind() schemaext.Kind { return DropKind }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *CreateDomain) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *ReplaceObjectType) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *Drop) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [CreateDomain.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *CreateDomain) Copy() *CreateDomain { return copyOf(v) }

// Copy is [ReplaceObjectType.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *ReplaceObjectType) Copy() *ReplaceObjectType {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Attributes = slices.Clone(v.Attributes)
	return &copied
}

// Copy is [Drop.CloneExtension] without the interface. A nil receiver returns
// nil.
func (v *Drop) Copy() *Drop { return copyOf(v) }

// Subject returns the domain's identity.
func (v *CreateDomain) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the object type's identity.
func (v *ReplaceObjectType) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the dropped type's identity.
func (v *Drop) Subject() objectidentity.ID { return v.Target.Subject() }

// Validate refuses a target that is not a domain, an empty base type and
// unrepresentable text.
func (v *CreateDomain) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: domain creation is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, DomainKind); err != nil {
		return err
	}
	if strings.TrimSpace(v.BaseType) == "" {
		return fmt.Errorf("%w: a domain creation names a base type", schemaext.ErrInvalidValue)
	}
	return validTexts("domain", "base type", v.BaseType, "default", v.Default, "default expression", v.DefaultExpr,
		"check", v.Check, "comment", v.Comment)
}

// Validate refuses a target that is not an object type, attributes a
// declaration could not hold and unrepresentable text.
func (v *ReplaceObjectType) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: object type statement is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, ObjectTypeKind); err != nil {
		return err
	}
	if err := validAttributes(v.Attributes); err != nil {
		return err
	}
	return validTexts("object type", "comment", v.Comment)
}

// Validate refuses a target of no owned kind.
func (v *Drop) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: type drop is nil", schemaext.ErrInvalidValue)
	}
	return validTarget(v.Target)
}

// RequiredCapability names the capability a target needs for the statement:
// Oracle 21 has no CREATE DOMAIN.
func (*CreateDomain) RequiredCapability() capability.Capability { return capability.DomainTypes }

// RequiredCapability names the capability a target needs for the statement.
func (*ReplaceObjectType) RequiredCapability() capability.Capability {
	return capability.CompositeTypes
}

// RequiredCapability names the capability a target needs to hold the kind
// the statement drops.
func (v *Drop) RequiredCapability() capability.Capability {
	if v.Target.Kind == DomainKind {
		return capability.DomainTypes
	}
	return capability.CompositeTypes
}

// Effect is the statement's effect: a new domain changes nothing that exists.
func (v *CreateDomain) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Additive, "creates a domain")
}

// Effect is the statement's effect: a new type changes nothing that exists,
// and a replaced one changes the shape of its values.
func (v *ReplaceObjectType) Effect() schemaext.Effect {
	if v != nil && v.Replaces {
		return effectOf(v, schemaext.Behavioral, "CREATE OR REPLACE TYPE rewrites the attributes of an object type")
	}
	return effectOf(v, schemaext.Additive, "creates an object type")
}

// Effect is the statement's effect: a drop removes the type.
func (v *Drop) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Destructive, "DROP removes the type")
}

// effectOf answers impact for a valid operation and nothing for an invalid
// one.
func effectOf(operation interface{ Validate() error }, impact schemaext.Impact, reason string) schemaext.Effect {
	if operation.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: impact, Reason: reason}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *CreateDomain) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports: an
// addition, or a modification of a type the statement replaces.
func (v *ReplaceObjectType) SchemaChange() ast.ExtensionChange {
	action := ast.ExtensionAdd
	if v.Replaces {
		action = ast.ExtensionModify
	}
	return ast.ExtensionChange{Action: action, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *Drop) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.Target.QualifiedName()}
}

// The operation wires. Every key is required.
var (
	targetShape = schemaext.ObjectShape{Name: "type target",
		Allowed: []string{"kind", "schema", "name"}, Required: []string{"kind", "schema", "name"}}
	createDomainShape = schemaext.ObjectShape{Name: "domain creation",
		Allowed:  []string{"target", "base_type", "not_null", "default", "default_expr", "check", "comment"},
		Required: []string{"target", "base_type", "not_null", "default", "default_expr", "check", "comment"}}
	replaceObjectTypeShape = schemaext.ObjectShape{Name: "object type statement",
		Allowed: []string{"target", "attributes", "comment", "replaces"}, Required: []string{"target", "attributes", "comment", "replaces"}}
	dropShape = schemaext.ObjectShape{Name: "type drop", Allowed: []string{"target"}, Required: []string{"target"}}
)

const targetDefinition = `{"type":"object","required":["kind","schema","name"],"additionalProperties":false,"properties":{` +
	`"kind":{"enum":["ptah.run/oratype/domain","ptah.run/oratype/object-type"]},` +
	`"schema":{"type":"string"},"name":` + textDefinition + `}}`

// OperationCodecs returns the version-one codecs of the three operations, in
// the order [OperationKinds] names them. Each call returns independent
// definitions.
func OperationCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		operationCodec(&CreateDomain{}, createDomainShape, (*CreateDomain).Validate, (*CreateDomain).Copy,
			`"base_type":`+textDefinition+`,"not_null":{"type":"boolean"},"default":{"type":"string"},`+
				`"default_expr":{"type":"string"},"check":{"type":"string"},"comment":{"type":"string"}`, nil),
		operationCodec(&ReplaceObjectType{}, replaceObjectTypeShape, (*ReplaceObjectType).Validate, (*ReplaceObjectType).Copy,
			`"attributes":`+attributesDefinition+`,"comment":{"type":"string"},"replaces":{"type":"boolean"}`,
			func(fields map[string]json.RawMessage) error { return eachObject(fields["attributes"], attributeShape) }),
		operationCodec(&Drop{}, dropShape, (*Drop).Validate, (*Drop).Copy, "", nil),
	}
}

// operationCodec builds one operation's codec: a target beside properties,
// every key present. rest checks the operation's own nested values.
func operationCodec[T interface {
	ast.ExtensionPayload
	Validate() error
}](prototype T, shape schemaext.ObjectShape, validate func(T) error, clone func(T) T, properties string,
	rest func(map[string]json.RawMessage) error,
) schemaext.Codec {
	if properties != "" {
		properties = "," + properties
	}
	return schemaext.ModelCodec[T]{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","additionalProperties":false,"required":` + requiredList(shape.Required) +
			`,"properties":{"target":` + targetDefinition + properties + `}}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			if _, err := schemaext.DecodeObject(fields["target"], targetShape); err != nil {
				return err
			}
			if rest != nil {
				return rest(fields)
			}
			return nil
		},
		Validate: validate, Clone: clone,
	}.Codec()
}

// requiredList writes keys as a JSON array of strings. A key is a plain
// identifier, so none needs escaping.
func requiredList(keys []string) string {
	return `["` + strings.Join(keys, `","`) + `"]`
}
