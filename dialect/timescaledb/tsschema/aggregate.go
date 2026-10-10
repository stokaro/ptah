package tsschema

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// DesiredContinuousAggregate declares a TimescaleDB continuous aggregate: a
// materialized view over a hypertable that the extension keeps up to date.
//
// It is its own model rather than a materialized view with a flag, because
// describing one as a materialized view is wrong in both directions, and both
// were measured on 2.29.2 / PostgreSQL 17.11: a plan that dropped it emitted
// `DROP VIEW` and the server answered `cannot drop continuous aggregate using
// DROP VIEW`, and a plan that created it emitted the body `pg_get_viewdef`
// answers, which selects from the materialization hypertable in a schema the
// extension owns (stokaro/ptah#1026).
//
// The aggregate's schema and name are its object identity, never fields here.
type DesiredContinuousAggregate struct {
	// Body is the SELECT the aggregate materializes as it was WRITTEN, without
	// the CREATE prefix and without a trailing semicolon. The catalog stores a
	// rewritten one, so the two are compared through Normalized rather than by
	// folding the text.
	Body string `json:"body"`
	// MaterializedOnly renders `timescaledb.materialized_only`, which decides
	// whether a query reads only materialized data or combines it with the raw
	// rows since the last refresh.
	//
	// Nil is not false: it is a declaration that did not choose, and it takes
	// whatever the server defaults to. The default is not a constant --
	// measured on 2.29.2, an aggregate created without the option is reported
	// `materialized_only = t` -- so comparing an unset declaration against the
	// catalog as if it said false would report a change on every run, and the
	// plan for that change drops the aggregate and its materialization.
	MaterializedOnly *bool `json:"materialized_only,omitempty"`
	// Comment is written before the statement. It is source documentation and
	// never takes part in a comparison.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has no
	// server counterpart and does not change the aggregate.
	StructName string `json:"struct_name,omitempty"`
	// Normalized is the connected server's own spelling of Body, attached by a
	// live comparison before the comparison runs. Nil means no server rewrote
	// this declaration: it was not compared live, or the server refused the
	// probe. A source never writes it.
	Normalized *NormalizedBody `json:"normalized,omitempty"`
}

// NormalizedBody is the definition a server stored for a probe of a declared
// body: the declaration put through the rewrite the catalog form went through.
type NormalizedBody struct {
	Body string `json:"body"`
}

// ObservedContinuousAggregate is a continuous aggregate as the extension's
// catalog reports it.
type ObservedContinuousAggregate struct {
	// Definition is the catalog's own view_definition: the SELECT as it was
	// written and then rewritten by the server, not pg_get_viewdef's answer,
	// which selects from the materialization hypertable.
	Definition string `json:"definition"`
	// MaterializedOnly is the option the catalog reports. A read always reports
	// it; nil appears only in a projection of a declaration that did not
	// choose, and means the value was not established.
	MaterializedOnly *bool `json:"materialized_only,omitempty"`
	// HypertableSchema and HypertableName name the hypertable the aggregate
	// materializes from, which is the relation it depends on. A projection of
	// a declaration leaves them empty, because a body is not parsed for them.
	HypertableSchema string `json:"hypertable_schema,omitempty"`
	HypertableName   string `json:"hypertable_name,omitempty"`
}

// Kind returns the continuous-aggregate model identity.
func (*DesiredContinuousAggregate) Kind() schemaext.Kind { return ContinuousAggregateKind }

// Kind returns the continuous-aggregate model identity.
func (*ObservedContinuousAggregate) Kind() schemaext.Kind { return ContinuousAggregateKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredContinuousAggregate) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredContinuousAggregate.Clone] without the interface: it shares
// no pointer with v, and a nil receiver returns nil.
func (v *DesiredContinuousAggregate) Copy() *DesiredContinuousAggregate {
	if v == nil {
		return nil
	}
	cloned := *v
	if v.MaterializedOnly != nil {
		cloned.MaterializedOnly = new(*v.MaterializedOnly)
	}
	if v.Normalized != nil {
		cloned.Normalized = new(*v.Normalized)
	}
	return &cloned
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedContinuousAggregate) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedContinuousAggregate.Clone] without the interface: it shares
// no pointer with v, and a nil receiver returns nil.
func (v *ObservedContinuousAggregate) Copy() *ObservedContinuousAggregate {
	if v == nil {
		return nil
	}
	cloned := *v
	if v.MaterializedOnly != nil {
		cloned.MaterializedOnly = new(*v.MaterializedOnly)
	}
	return &cloned
}

// Equal compares declarations field by field, including an attached
// normalization, without resolving server defaults.
func (v *DesiredContinuousAggregate) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredContinuousAggregate)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return v.Body == right.Body && v.Comment == right.Comment && v.StructName == right.StructName &&
		equalBool(v.MaterializedOnly, right.MaterializedOnly) && equalNormalized(v.Normalized, right.Normalized)
}

// Equal compares observations field by field.
func (v *ObservedContinuousAggregate) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedContinuousAggregate)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return v.Definition == right.Definition && v.HypertableSchema == right.HypertableSchema &&
		v.HypertableName == right.HypertableName && equalBool(v.MaterializedOnly, right.MaterializedOnly)
}

// Desired declares the observed definition and option. The catalog definition
// is the SELECT a server keeps, which replays as the same aggregate, and a
// definite option recreates the value that was there rather than the server's
// default at the time of the replay.
func (v *ObservedContinuousAggregate) Desired() (*DesiredContinuousAggregate, error) {
	if err := ValidateObservedContinuousAggregate(v); err != nil {
		return nil, err
	}
	declared := &DesiredContinuousAggregate{Body: v.Definition}
	if v.MaterializedOnly != nil {
		declared.MaterializedOnly = new(*v.MaterializedOnly)
	}
	return declared, nil
}

// Observed predicts the aggregate a declaration creates. The definition is the
// declared body, which a server would rewrite, so offline comparisons leave it
// unanswered; the hypertable is not parsed out of the body.
func (v *DesiredContinuousAggregate) Observed() (*ObservedContinuousAggregate, error) {
	if err := ValidateDesiredContinuousAggregate(v); err != nil {
		return nil, err
	}
	observed := &ObservedContinuousAggregate{Definition: v.Body}
	if v.MaterializedOnly != nil {
		observed.MaterializedOnly = new(*v.MaterializedOnly)
	}
	return observed, nil
}

// FoldBody trims what a catalog adds around a definition it returns: the
// trailing semicolon and the surrounding whitespace are the catalog's
// punctuation, not the author's. A statement that kept the semicolon would put
// it before `WITH NO DATA`, which the server refuses.
func FoldBody(body string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(body), ";"))
}

// ContinuousAggregateRef builds a schema-scoped identity from separate schema
// and name parts. An empty schema takes PostgreSQL's default and is marked as
// defaulted, so a comparison can resolve it to the connection's own schema.
func ContinuousAggregateRef(schema, name string) objectidentity.ID {
	return ContinuousAggregateRefWith(identifier.ForDialect(platform.Postgres), schema, name)
}

// ContinuousAggregateRefWith builds the identity under explicit identifier
// rules, which is how a comparison pairs a declaration and an observation that
// spelled the same schema differently.
func ContinuousAggregateRefWith(semantics identifier.Semantics, schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(semantics).SchemaScopedParts(objectidentity.Kind(ContinuousAggregateKind), schema, name)
}

// AuthoredSchema is the schema a reference names, or empty when the reference
// took the default. A statement writes exactly that, as the source did.
func AuthoredSchema(ref objectidentity.ID) string { return ref.Schema.Authored() }

// QualifiedName renders a reference the way the source spelled it: schema.name
// when the schema was written, and the bare name when it was defaulted.
func QualifiedName(ref objectidentity.ID) string {
	if schema := AuthoredSchema(ref); schema != "" {
		return schema + "." + ref.Name.Source
	}
	return ref.Name.Source
}

// DesiredContinuousAggregateObject captures one authored aggregate.
func DesiredContinuousAggregateObject(schema, name string, aggregate DesiredContinuousAggregate) schemaext.Object {
	return schemaext.Object{Ref: ContinuousAggregateRef(schema, name), Value: aggregate.Clone()}
}

// ObservedContinuousAggregateObject captures one aggregate a read reported.
func ObservedContinuousAggregateObject(schema, name string, aggregate ObservedContinuousAggregate) schemaext.Object {
	return schemaext.Object{Ref: ContinuousAggregateRef(schema, name), Value: aggregate.Clone()}
}

// ValidateContinuousAggregateRef requires a schema-scoped identity of this
// kind with a name and no table parent or signature.
func ValidateContinuousAggregateRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(ContinuousAggregateKind) || strings.TrimSpace(ref.Name.Source) == "" ||
		ref.Name.Normalized == "" || !ref.Parent.Empty() || !ref.Catalog.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: a continuous aggregate requires a schema-scoped identity", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidateDesiredContinuousAggregate checks representation invariants: a body
// is required, because a continuous aggregate is its SELECT.
func ValidateDesiredContinuousAggregate(v *DesiredContinuousAggregate) error {
	if v == nil {
		return modelError(ContinuousAggregateKind, schemaext.Desired, fmt.Errorf("%w: nil continuous aggregate declaration", schemaext.ErrInvalidValue))
	}
	if FoldBody(v.Body) == "" {
		return modelError(ContinuousAggregateKind, schemaext.Desired, fmt.Errorf("%w: a continuous aggregate requires a body", schemaext.ErrInvalidValue))
	}
	normalized := ""
	if v.Normalized != nil {
		normalized = v.Normalized.Body
	}
	if err := validTexts("body", v.Body, "comment", v.Comment, "struct name", v.StructName, "normalized body", normalized); err != nil {
		return modelError(ContinuousAggregateKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedContinuousAggregate checks that an observation carries a
// definition and representable text.
func ValidateObservedContinuousAggregate(v *ObservedContinuousAggregate) error {
	if v == nil {
		return modelError(ContinuousAggregateKind, schemaext.Observed, fmt.Errorf("%w: nil continuous aggregate observation", schemaext.ErrInvalidValue))
	}
	if FoldBody(v.Definition) == "" {
		return modelError(ContinuousAggregateKind, schemaext.Observed, fmt.Errorf("%w: a continuous aggregate observation requires a definition", schemaext.ErrInvalidValue))
	}
	if err := validTexts("definition", v.Definition, "hypertable schema", v.HypertableSchema, "hypertable name", v.HypertableName); err != nil {
		return modelError(ContinuousAggregateKind, schemaext.Observed, err)
	}
	return nil
}

func equalBool(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalNormalized(left, right *NormalizedBody) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
