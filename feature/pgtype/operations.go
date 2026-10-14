package pgtype

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
)

// The operation kinds. A type's comment is an operation of its own, because a
// target can hold a type and not its comment.
const (
	// CreateDomainKind identifies CREATE DOMAIN.
	CreateDomainKind schemaext.Kind = "ptah.run/pgtype/create-domain"
	// AlterDomainKind identifies the ALTER DOMAIN statements of one domain.
	AlterDomainKind schemaext.Kind = "ptah.run/pgtype/alter-domain"
	// CreateCompositeKind identifies CREATE TYPE ... AS (...).
	CreateCompositeKind schemaext.Kind = "ptah.run/pgtype/create-composite"
	// AlterCompositeKind identifies the ALTER TYPE ... ATTRIBUTE statements of
	// one composite type.
	AlterCompositeKind schemaext.Kind = "ptah.run/pgtype/alter-composite"
	// CreateRangeKind identifies CREATE TYPE ... AS RANGE.
	CreateRangeKind schemaext.Kind = "ptah.run/pgtype/create-range"
	// DropKind identifies DROP DOMAIN and DROP TYPE.
	DropKind schemaext.Kind = "ptah.run/pgtype/drop"
	// CommentKind identifies COMMENT ON DOMAIN and COMMENT ON TYPE.
	CommentKind schemaext.Kind = "ptah.run/pgtype/comment"
)

// OperationKinds are the seven operation kinds, in the order the package
// declares them.
func OperationKinds() []schemaext.Kind {
	return []schemaext.Kind{CreateDomainKind, AlterDomainKind, CreateCompositeKind, AlterCompositeKind, CreateRangeKind, DropKind, CommentKind}
}

// Target names the type one operation is about: the kind of its model, its
// schema as the declaration wrote it, empty where it left the schema to the
// connection's default, and its name.
type Target struct {
	Kind   schemaext.Kind `json:"kind"`
	Schema string         `json:"schema"`
	Name   string         `json:"name"`
}

// TargetOf names the type ref identifies.
func TargetOf(ref objectidentity.ID) Target {
	return Target{Kind: schemaext.Kind(ref.Kind), Schema: AuthoredSchema(ref), Name: ref.Name.Source}
}

// Subject returns the type's identity under PostgreSQL's identifier rules.
func (t Target) Subject() objectidentity.ID { return Ref(t.Kind, t.Schema, t.Name) }

// QualifiedName is the type's name as a statement writes it before quoting:
// schema.name where the schema was written, and the bare name otherwise.
func (t Target) QualifiedName() string { return qualified(t.Schema, t.Name) }

// validTarget refuses a target of another kind than want, or of no owned
// kind where want is empty, and an invalid identity.
func validTarget(target Target, want ...schemaext.Kind) error {
	if len(want) > 0 && !slices.Contains(want, target.Kind) {
		return fmt.Errorf("%w: the operation is about a %s, not %q", schemaext.ErrInvalidValue, labels[want[0]], target.Kind)
	}
	if err := validTexts("type", "schema", target.Schema); err != nil {
		return err
	}
	return ValidateRef(target.Subject())
}

// CreateDomain writes CREATE DOMAIN: the base type, NOT NULL, a default and
// a CHECK. A default expression is written as it is, and a literal default as
// a literal of the base type; a creation that names both writes the
// expression.
type CreateDomain struct {
	Target      Target `json:"target"`
	BaseType    string `json:"base_type"`
	NotNull     bool   `json:"not_null"`
	Default     string `json:"default"`
	DefaultExpr string `json:"default_expr"`
	Check       string `json:"check"`
}

// CreateDomainOf is the creation of the domain declared at ref.
func CreateDomainOf(ref objectidentity.ID, domain *DesiredDomain) *CreateDomain {
	return &CreateDomain{Target: TargetOf(ref), BaseType: domain.BaseType, NotNull: domain.NotNull, Default: domain.Default,
		DefaultExpr: domain.DefaultExpr, Check: domain.Check}
}

// AlterDomain writes ALTER DOMAIN for the changes a surviving domain takes in
// place, in the order they have to run: each CHECK constraint DropChecks
// names is dropped, AddCheck is added with its optional name, SetDefault becomes
// the default, and NotNull sets or drops NOT NULL. An empty field changes
// nothing.
type AlterDomain struct {
	Target     Target   `json:"target"`
	DropChecks []string `json:"drop_checks"`
	AddCheck   string   `json:"add_check"`
	// CheckName names AddCheck so a reverse plan can remove that constraint.
	CheckName string `json:"check_name"`
	// SetDefault is the default as SQL: an expression, or a literal already
	// quoted.
	SetDefault string `json:"set_default"`
	// DropDefault removes the default and is mutually exclusive with SetDefault.
	DropDefault bool  `json:"drop_default"`
	NotNull     *bool `json:"not_null"`
}

// CreateComposite writes CREATE TYPE ... AS with the attributes in order.
type CreateComposite struct {
	Target     Target      `json:"target"`
	Attributes []Attribute `json:"attributes"`
}

// AlterComposite writes ALTER TYPE for the attributes a surviving composite
// type loses and gains in place: every attribute Drop names is dropped first,
// so a reused name does not collide with the one still there, and each
// attribute Add names is then appended.
type AlterComposite struct {
	Target Target      `json:"target"`
	Drop   []string    `json:"drop"`
	Add    []Attribute `json:"add"`
}

// CreateRange writes CREATE TYPE ... AS RANGE with the subtype and the
// optional attributes the creation names.
type CreateRange struct {
	Target         Target `json:"target"`
	Subtype        string `json:"subtype"`
	SubtypeOpClass string `json:"subtype_opclass"`
	Collation      string `json:"collation"`
	Canonical      string `json:"canonical"`
	SubtypeDiff    string `json:"subtype_diff"`
}

// CreateRangeOf is the creation of the range type declared at ref.
func CreateRangeOf(ref objectidentity.ID, declared *DesiredRange) *CreateRange {
	return &CreateRange{Target: TargetOf(ref), Subtype: declared.Subtype, SubtypeOpClass: declared.SubtypeOpClass,
		Collation: declared.Collation, Canonical: declared.Canonical, SubtypeDiff: declared.SubtypeDiff}
}

// Drop writes DROP DOMAIN for a domain and DROP TYPE otherwise, IF EXISTS,
// and CASCADE where the operation says so. A type removed for good is dropped
// with CASCADE; one dropped to be created again is not, so the drop fails
// while anything still uses it rather than taking the users with it.
type Drop struct {
	Target  Target `json:"target"`
	Cascade bool   `json:"cascade"`
}

// Comment writes COMMENT ON DOMAIN for a domain and COMMENT ON TYPE
// otherwise, with NULL for an empty comment.
type Comment struct {
	Target  Target `json:"target"`
	Comment string `json:"comment"`
}

// Kind returns the stable operation identity.
func (*CreateDomain) Kind() schemaext.Kind { return CreateDomainKind }

// Kind returns the stable operation identity.
func (*AlterDomain) Kind() schemaext.Kind { return AlterDomainKind }

// Kind returns the stable operation identity.
func (*CreateComposite) Kind() schemaext.Kind { return CreateCompositeKind }

// Kind returns the stable operation identity.
func (*AlterComposite) Kind() schemaext.Kind { return AlterCompositeKind }

// Kind returns the stable operation identity.
func (*CreateRange) Kind() schemaext.Kind { return CreateRangeKind }

// Kind returns the stable operation identity.
func (*Drop) Kind() schemaext.Kind { return DropKind }

// Kind returns the stable operation identity.
func (*Comment) Kind() schemaext.Kind { return CommentKind }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *CreateDomain) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *AlterDomain) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *CreateComposite) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *AlterComposite) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *CreateRange) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *Drop) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// CloneExtension returns an independent operation. A nil receiver remains
// typed nil.
func (v *Comment) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [CreateDomain.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *CreateDomain) Copy() *CreateDomain { return copyOf(v) }

// Copy is [AlterDomain.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *AlterDomain) Copy() *AlterDomain {
	if v == nil {
		return nil
	}
	copied := *v
	copied.DropChecks = slices.Clone(v.DropChecks)
	if v.NotNull != nil {
		copied.NotNull = new(*v.NotNull)
	}
	return &copied
}

// Copy is [CreateComposite.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *CreateComposite) Copy() *CreateComposite {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Attributes = slices.Clone(v.Attributes)
	return &copied
}

// Copy is [AlterComposite.CloneExtension] without the interface. A nil
// receiver returns nil.
func (v *AlterComposite) Copy() *AlterComposite {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Drop = slices.Clone(v.Drop)
	copied.Add = slices.Clone(v.Add)
	return &copied
}

// Copy is [CreateRange.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *CreateRange) Copy() *CreateRange { return copyOf(v) }

// Copy is [Drop.CloneExtension] without the interface. A nil receiver returns
// nil.
func (v *Drop) Copy() *Drop { return copyOf(v) }

// Copy is [Comment.CloneExtension] without the interface. A nil receiver
// returns nil.
func (v *Comment) Copy() *Comment { return copyOf(v) }

// copyOf copies a payload that holds no reference.
func copyOf[T any](v *T) *T {
	if v == nil {
		return nil
	}
	copied := *v
	return &copied
}

// Subject returns the domain's identity.
func (v *CreateDomain) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the domain's identity.
func (v *AlterDomain) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the composite type's identity.
func (v *CreateComposite) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the composite type's identity.
func (v *AlterComposite) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the range type's identity.
func (v *CreateRange) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the dropped type's identity.
func (v *Drop) Subject() objectidentity.ID { return v.Target.Subject() }

// Subject returns the commented type's identity.
func (v *Comment) Subject() objectidentity.ID { return v.Target.Subject() }

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
	return validTexts("domain", "base type", v.BaseType, "default", v.Default, "default expression", v.DefaultExpr, "check", v.Check)
}

// Validate refuses a target that is not a domain, an alteration that changes
// nothing, an empty or repeated constraint name, and unrepresentable text.
func (v *AlterDomain) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: domain alteration is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, DomainKind); err != nil {
		return err
	}
	if len(v.DropChecks) == 0 && v.AddCheck == "" && v.SetDefault == "" && !v.DropDefault && v.NotNull == nil {
		return fmt.Errorf("%w: a domain alteration changes something", schemaext.ErrInvalidValue)
	}
	if err := validNames("domain check", v.DropChecks); err != nil {
		return err
	}
	if v.DropDefault && v.SetDefault != "" {
		return fmt.Errorf("%w: a domain alteration cannot set and drop its default", schemaext.ErrInvalidValue)
	}
	if v.CheckName != "" && v.AddCheck == "" {
		return fmt.Errorf("%w: a constraint name requires a CHECK", schemaext.ErrInvalidValue)
	}
	return validTexts("domain", "check", v.AddCheck, "check name", v.CheckName, "default", v.SetDefault)
}

// Validate refuses a target that is not a composite type and attributes a
// declaration could not hold.
func (v *CreateComposite) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: composite type creation is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, CompositeKind); err != nil {
		return err
	}
	return validAttributes(v.Attributes)
}

// Validate refuses a target that is not a composite type, an alteration that
// changes nothing, and attributes or names a declaration could not hold.
func (v *AlterComposite) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: composite type alteration is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, CompositeKind); err != nil {
		return err
	}
	if len(v.Drop) == 0 && len(v.Add) == 0 {
		return fmt.Errorf("%w: a composite type alteration changes something", schemaext.ErrInvalidValue)
	}
	if err := validNames("composite type attribute", v.Drop); err != nil {
		return err
	}
	if len(v.Add) > 0 {
		return validAttributes(v.Add)
	}
	return nil
}

// Validate refuses a target that is not a range type, an empty subtype and
// unrepresentable text.
func (v *CreateRange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: range type creation is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target, RangeKind); err != nil {
		return err
	}
	if strings.TrimSpace(v.Subtype) == "" {
		return fmt.Errorf("%w: a range type creation names a subtype", schemaext.ErrInvalidValue)
	}
	return validTexts("range type", "subtype", v.Subtype, RangeSubtypeOpClass, v.SubtypeOpClass, RangeCollation, v.Collation,
		RangeCanonical, v.Canonical, RangeSubtypeDiff, v.SubtypeDiff)
}

// Validate refuses a target of no owned kind.
func (v *Drop) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: type drop is nil", schemaext.ErrInvalidValue)
	}
	return validTarget(v.Target)
}

// Validate refuses a target of no owned kind and an unrepresentable comment.
func (v *Comment) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: type comment is nil", schemaext.ErrInvalidValue)
	}
	if err := validTarget(v.Target); err != nil {
		return err
	}
	return validTexts("type", "comment", v.Comment)
}

// RequiredCapability names the capability a target needs for the statement.
// A target without it writes a skip line naming [CreateDomain.OmissionSubject].
func (*CreateDomain) RequiredCapability() capability.Capability { return capability.DomainTypes }

// RequiredCapability names the capability a target needs for the statement.
func (*AlterDomain) RequiredCapability() capability.Capability { return capability.DomainTypes }

// RequiredCapability names the capability a target needs for the statement.
func (*CreateComposite) RequiredCapability() capability.Capability { return capability.CompositeTypes }

// RequiredCapability names the capability a target needs for the statement.
func (*AlterComposite) RequiredCapability() capability.Capability { return capability.CompositeTypes }

// RequiredCapability names the capability a target needs for the statement.
func (*CreateRange) RequiredCapability() capability.Capability { return capability.RangeTypes }

// RequiredCapability names the capability a target needs to store the
// comment: a domain's or another type's.
func (v *Comment) RequiredCapability() capability.Capability {
	if v.Target.Kind == DomainKind {
		return capability.DomainComments
	}
	return capability.TypeComments
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *CreateDomain) OmissionSubject() (kind, name string) {
	return labels[DomainKind], v.Target.QualifiedName()
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *AlterDomain) OmissionSubject() (kind, name string) {
	return labels[DomainKind], v.Target.QualifiedName()
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *CreateComposite) OmissionSubject() (kind, name string) {
	return labels[CompositeKind], v.Target.QualifiedName()
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *AlterComposite) OmissionSubject() (kind, name string) {
	return labels[CompositeKind], v.Target.QualifiedName()
}

// OmissionSubject names the object a skip line and an omission record name.
func (v *CreateRange) OmissionSubject() (kind, name string) {
	return labels[RangeKind], v.Target.QualifiedName()
}

// OmissionSubject names the object a skip line and an omission record name:
// a domain's comment or a type's.
func (v *Comment) OmissionSubject() (kind, name string) {
	if v.Target.Kind == DomainKind {
		return "domain comment", v.Target.QualifiedName()
	}
	return "type comment", v.Target.QualifiedName()
}

// Effect is the statement's effect: a new type changes nothing that exists.
func (v *CreateDomain) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Additive, "creates a domain")
}

// Effect is the statement's effect: a changed constraint or default changes
// what the domain admits.
func (v *AlterDomain) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Behavioral, "changes the constraints or the default of a domain")
}

// Effect is the statement's effect: a new type changes nothing that exists.
func (v *CreateComposite) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Additive, "creates a composite type")
}

// Effect is the statement's effect: a dropped attribute takes its values
// from every column of the type.
func (v *AlterComposite) Effect() schemaext.Effect {
	if v != nil && len(v.Drop) > 0 {
		return effectOf(v, schemaext.Destructive, "ALTER TYPE ... DROP ATTRIBUTE removes the attribute's values from every column of the type")
	}
	return effectOf(v, schemaext.Additive, "adds attributes to a composite type")
}

// Effect is the statement's effect: a new type changes nothing that exists.
func (v *CreateRange) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Additive, "creates a range type")
}

// Effect is the statement's effect: a drop removes the type, and with
// CASCADE every column of it.
func (v *Drop) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Destructive, "DROP removes the type, and with CASCADE the columns and objects that use it")
}

// Effect is the statement's effect: a comment changes no object's behavior.
func (v *Comment) Effect() schemaext.Effect {
	return effectOf(v, schemaext.Additive, "changes only the comment of a type")
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

// SchemaChange reports the logical action for schema-change reports.
func (v *AlterDomain) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *CreateComposite) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *AlterComposite) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *CreateRange) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionAdd, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *Drop) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionDrop, Name: v.Target.QualifiedName()}
}

// SchemaChange reports the logical action for schema-change reports.
func (v *Comment) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: v.Target.QualifiedName()}
}

// The operation wires. Every key is required; a list the operation does not
// hold and an unset NOT NULL are written null.
var (
	targetShape = schemaext.ObjectShape{Name: "type target",
		Allowed: []string{"kind", "schema", "name"}, Required: []string{"kind", "schema", "name"}}
	createDomainShape = schemaext.ObjectShape{Name: "domain creation",
		Allowed:  []string{"target", "base_type", "not_null", "default", "default_expr", "check"},
		Required: []string{"target", "base_type", "not_null", "default", "default_expr", "check"}}
	alterDomainShape = schemaext.ObjectShape{Name: "domain alteration",
		Allowed:  []string{"target", "drop_checks", "add_check", "check_name", "set_default", "drop_default", "not_null"},
		Required: []string{"target", "drop_checks", "add_check", "check_name", "set_default", "drop_default", "not_null"}, Nullable: []string{"drop_checks", "not_null"}}
	createCompositeShape = schemaext.ObjectShape{Name: "composite type creation",
		Allowed: []string{"target", "attributes"}, Required: []string{"target", "attributes"}}
	alterCompositeShape = schemaext.ObjectShape{Name: "composite type alteration",
		Allowed: []string{"target", "drop", "add"}, Required: []string{"target", "drop", "add"}, Nullable: []string{"drop", "add"}}
	createRangeShape = schemaext.ObjectShape{Name: "range type creation",
		Allowed:  []string{"target", "subtype", "subtype_opclass", "collation", "canonical", "subtype_diff"},
		Required: []string{"target", "subtype", "subtype_opclass", "collation", "canonical", "subtype_diff"}}
	dropShape = schemaext.ObjectShape{Name: "type drop",
		Allowed: []string{"target", "cascade"}, Required: []string{"target", "cascade"}}
	commentShape = schemaext.ObjectShape{Name: "type comment",
		Allowed: []string{"target", "comment"}, Required: []string{"target", "comment"}}
)

const (
	targetDefinition = `{"type":"object","required":["kind","schema","name"],"additionalProperties":false,"properties":{` +
		`"kind":{"enum":["ptah.run/pgtype/domain","ptah.run/pgtype/composite","ptah.run/pgtype/range"]},` +
		`"schema":{"type":"string"},"name":` + textDefinition + `}}`
	nullableNames = `{"anyOf":[{"type":"null"},{"type":"array","minItems":1,"items":` + textDefinition + `}]}`
)

// OperationCodecs returns the version-one codecs of the seven operations, in
// the order [OperationKinds] names them. Each call returns independent
// definitions.
func OperationCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		operationCodec(&CreateDomain{}, createDomainShape, (*CreateDomain).Validate, (*CreateDomain).Copy, nil,
			`"base_type":`+textDefinition+`,"not_null":{"type":"boolean"},"default":{"type":"string"},`+
				`"default_expr":{"type":"string"},"check":{"type":"string"}`, nil),
		operationCodec(&AlterDomain{}, alterDomainShape, (*AlterDomain).Validate, (*AlterDomain).Copy, canonicalAlterDomain,
			`"drop_checks":`+nullableNames+`,"add_check":{"type":"string"},"check_name":{"type":"string"},"set_default":{"type":"string"},"drop_default":{"type":"boolean"},`+
				`"not_null":{"anyOf":[{"type":"null"},{"type":"boolean"}]}`, nil),
		operationCodec(&CreateComposite{}, createCompositeShape, (*CreateComposite).Validate, (*CreateComposite).Copy, nil,
			`"attributes":`+attributesDefinition, func(fields map[string]json.RawMessage) error {
				return eachObject(fields["attributes"], attributeShape)
			}),
		operationCodec(&AlterComposite{}, alterCompositeShape, (*AlterComposite).Validate, (*AlterComposite).Copy, canonicalAlterComposite,
			`"drop":`+nullableNames+`,"add":{"anyOf":[{"type":"null"},`+attributesDefinition+`]}`,
			func(fields map[string]json.RawMessage) error {
				return eachObject(nonNull(fields["add"]), attributeShape)
			}),
		operationCodec(&CreateRange{}, createRangeShape, (*CreateRange).Validate, (*CreateRange).Copy, nil,
			`"subtype":`+textDefinition+`,"subtype_opclass":{"type":"string"},"collation":{"type":"string"},`+
				`"canonical":{"type":"string"},"subtype_diff":{"type":"string"}`, nil),
		operationCodec(&Drop{}, dropShape, (*Drop).Validate, (*Drop).Copy, nil, `"cascade":{"type":"boolean"}`, nil),
		operationCodec(&Comment{}, commentShape, (*Comment).Validate, (*Comment).Copy, nil, `"comment":{"type":"string"}`, nil),
	}
}

// canonicalAlterDomain writes an empty constraint list as null, the one
// spelling the decoder accepts.
func canonicalAlterDomain(value *AlterDomain) *AlterDomain {
	canonical := value.Copy()
	canonical.DropChecks = nilWhenEmpty(canonical.DropChecks)
	return canonical
}

// canonicalAlterComposite writes an empty attribute list as null.
func canonicalAlterComposite(value *AlterComposite) *AlterComposite {
	canonical := value.Copy()
	canonical.Drop = nilWhenEmpty(canonical.Drop)
	canonical.Add = nilWhenEmpty(canonical.Add)
	return canonical
}

// nilWhenEmpty is values, or nil where it holds nothing.
func nilWhenEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return values
}

// operationCodec builds one operation's codec: a target beside properties,
// every key present. canonical is the value as it is encoded, nil for the
// value itself. rest checks the operation's own nested values.
func operationCodec[T interface {
	ast.ExtensionPayload
	Validate() error
}](prototype T, shape schemaext.ObjectShape, validate func(T) error, clone, canonical func(T) T, properties string,
	rest func(map[string]json.RawMessage) error,
) schemaext.Codec {
	return schemaext.ModelCodec[T]{
		Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","additionalProperties":false,"required":` + requiredList(shape.Required) +
			`,"properties":{"target":` + targetDefinition + `,` + properties + `}}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			if err := noEmptyLists(shape, fields); err != nil {
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
		Validate: validate, Clone: clone, Canonical: canonical,
	}.Codec()
}

// requiredList writes keys as a JSON array of strings. A key is a plain
// identifier, so none needs escaping.
func requiredList(keys []string) string {
	return `["` + strings.Join(keys, `","`) + `"]`
}
