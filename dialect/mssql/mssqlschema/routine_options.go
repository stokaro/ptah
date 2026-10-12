package mssqlschema

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// RoutineOptionsKind identifies the options a SQL Server routine's header
// writes in its WITH list and the model has no common field for: the
// principal it runs as, SCHEMABINDING, RECOMPILE and RETURNS NULL ON NULL
// INPUT. It is a facet of the routine, keyed by the routine's identity
// ([objectidentity.Builder.Function] or [objectidentity.Builder.Procedure]).
//
// Whose rights a routine runs with is the common security mode: EXECUTE AS
// CALLER is INVOKER, and every other EXECUTE AS is DEFINER. EXECUTE AS OWNER
// runs as the routine's owner and names no principal. A named principal is
// what this facet adds, because the catalog reports it and a routine that runs
// as one is not the routine that runs as its owner, although the security
// mode calls both DEFINER.
//
// Each option is one the catalog reports in sys.sql_modules, measured on SQL
// Server 2025 RTM-CU9: execute_as_principal_id, is_schema_bound,
// is_recompiled and null_on_null_input. EXECUTE AS SELF has no place here:
// the catalog stores the principal that created the routine in its place, so a
// declared SELF would compare equal only to whichever account ran the apply.
const RoutineOptionsKind schemaext.Kind = "ptah.run/mssql/routine-options"

// RoutineOwner is the provider identity that owns the routine options facet:
// the SQL Server target's own provider, which also reads the routine's SQL
// syntax. It differs from [Owner], the security policy owner.
const RoutineOwner = "ptah.run/sqlserver"

// HoldsRoutineOptions reports whether subject is one that holds routine
// options: a function or a procedure.
func HoldsRoutineOptions(subject objectidentity.ID) bool {
	return (subject.Kind == objectidentity.KindFunction || subject.Kind == objectidentity.KindProcedure) && !subject.Name.Empty()
}

// RoutineOptions are the options of a routine's header. The zero value states
// none, which is a routine that runs as its caller or its owner, as its
// security mode says, with every other option at the server's default. A
// facet always states at least one.
//
// The server decides which routine takes which option: SCHEMABINDING and
// RETURNS NULL ON NULL INPUT are a function's, and RECOMPILE is a
// procedure's. A routine stating another is created as written, and the
// server refuses it.
type RoutineOptions struct {
	// Principal is the database user the routine runs as, as EXECUTE AS
	// 'name' writes it, without quotes. Empty names none.
	Principal string `json:"principal,omitempty"`
	// SchemaBinding binds the routine to the objects its body names, so they
	// cannot be altered or dropped under it.
	SchemaBinding bool `json:"schema_binding,omitempty"`
	// Recompile compiles a new plan on every run of a procedure.
	Recompile bool `json:"recompile,omitempty"`
	// ReturnsNullOnNullInput returns NULL for a NULL argument without running
	// the body of a scalar function.
	ReturnsNullOnNullInput bool `json:"returns_null_on_null_input,omitempty"`
}

// IsZero reports whether o states no option.
func (o RoutineOptions) IsZero() bool { return o == RoutineOptions{} }

// Same reports whether o and other state the same options, the principals
// compared as [SamePrincipal] compares them.
func (o RoutineOptions) Same(other RoutineOptions) bool {
	return SamePrincipal(o.Principal, other.Principal) && o.SchemaBinding == other.SchemaBinding &&
		o.Recompile == other.Recompile && o.ReturnsNullOnNullInput == other.ReturnsNullOnNullInput
}

// Clauses returns the WITH list entries o writes, in the order the renderer
// writes them, without EXECUTE AS: SCHEMABINDING, RETURNS NULL ON NULL INPUT,
// RECOMPILE.
func (o RoutineOptions) Clauses() []string {
	var clauses []string
	if o.SchemaBinding {
		clauses = append(clauses, "SCHEMABINDING")
	}
	if o.ReturnsNullOnNullInput {
		clauses = append(clauses, "RETURNS NULL ON NULL INPUT")
	}
	if o.Recompile {
		clauses = append(clauses, "RECOMPILE")
	}
	return clauses
}

// DesiredRoutineOptions are the options a declaration writes in a routine's
// header.
type DesiredRoutineOptions RoutineOptions

// ObservedRoutineOptions are the options a read of a routine reports.
type ObservedRoutineOptions RoutineOptions

// Kind returns the owned routine options identity.
func (*DesiredRoutineOptions) Kind() schemaext.Kind { return RoutineOptionsKind }

// Kind returns the owned routine options identity.
func (*ObservedRoutineOptions) Kind() schemaext.Kind { return RoutineOptionsKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRoutineOptions) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredRoutineOptions)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRoutineOptions) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedRoutineOptions)(nil)
	}
	return new(*v)
}

// Equal compares declarations by their options, the principal as written.
func (v *DesiredRoutineOptions) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredRoutineOptions)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations by their options, the principal as reported.
func (v *ObservedRoutineOptions) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedRoutineOptions)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Observed projects a declaration as a read of the routine created from it
// reports it. Nil and invalid declarations are refused with
// schemaext.ErrInvalidValue.
func (v *DesiredRoutineOptions) Observed() (*ObservedRoutineOptions, error) {
	if err := ValidateDesiredRoutineOptions(v); err != nil {
		return nil, err
	}
	return new(ObservedRoutineOptions(*v)), nil
}

// Desired captures an observation as a declaration of the options it holds.
// Nil and invalid observations are refused with schemaext.ErrInvalidValue.
func (v *ObservedRoutineOptions) Desired() (*DesiredRoutineOptions, error) {
	if err := ValidateObservedRoutineOptions(v); err != nil {
		return nil, err
	}
	return new(DesiredRoutineOptions(*v)), nil
}

// SamePrincipal reports identical principal spelling, ignoring trailing spaces.
// It preserves case because distinct users can differ only by case. The
// comparison service applies the connected catalog's identifier equivalence.
func SamePrincipal(a, b string) bool {
	return strings.TrimRight(a, " ") == strings.TrimRight(b, " ")
}

// ValidateDesiredRoutineOptions refuses a nil declaration, one that states no
// option, and a principal that is blank or that text a statement cannot
// carry. Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredRoutineOptions(v *DesiredRoutineOptions) error {
	if v == nil {
		return routineOptionsError(schemaext.Desired, fmt.Errorf("%w: nil routine options declaration", schemaext.ErrInvalidValue))
	}
	return routineOptionsError(schemaext.Desired, validRoutineOptions(RoutineOptions(*v)))
}

// ValidateObservedRoutineOptions refuses what [ValidateDesiredRoutineOptions]
// refuses.
func ValidateObservedRoutineOptions(v *ObservedRoutineOptions) error {
	if v == nil {
		return routineOptionsError(schemaext.Observed, fmt.Errorf("%w: nil routine options observation", schemaext.ErrInvalidValue))
	}
	return routineOptionsError(schemaext.Observed, validRoutineOptions(RoutineOptions(*v)))
}

func validRoutineOptions(options RoutineOptions) error {
	switch {
	case options.IsZero():
		return fmt.Errorf("%w: routine options must state an option; a routine with none carries no facet", schemaext.ErrInvalidValue)
	case options.Principal == "":
		return nil
	case strings.TrimSpace(options.Principal) == "":
		return fmt.Errorf("%w: an EXECUTE AS principal must name a database user", schemaext.ErrInvalidValue)
	}
	return schemaext.ValidText("EXECUTE AS principal", options.Principal)
}

func routineOptionsError(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: RoutineOptionsKind, Representation: representation, Message: err.Error()}
}

// WithRoutineOptions returns facets with a declaration of options added, as a
// SQL statement writes them in a routine's header. A collection that already
// holds the kind, and invalid options, are refused.
func WithRoutineOptions(facets schemaext.Facets, options RoutineOptions) (schemaext.Facets, error) {
	declared := new(DesiredRoutineOptions(options))
	if err := ValidateDesiredRoutineOptions(declared); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(declared)
}

// WithObservedRoutineOptions returns facets with an observation of options
// added, as a read of a SQL Server routine records it. A collection that
// already holds the kind, and invalid options, are refused.
func WithObservedRoutineOptions(facets schemaext.Facets, options RoutineOptions) (schemaext.Facets, error) {
	observed := new(ObservedRoutineOptions(options))
	if err := ValidateObservedRoutineOptions(observed); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(observed)
}

// RoutineOptionsOf returns the options facets hold in either representation,
// and false when they hold none. A value of another type, and an invalid
// value, are refused with schemaext.ErrInvalidValue.
func RoutineOptionsOf(facets schemaext.Facets) (RoutineOptions, bool, error) {
	value, found, err := facets.Get(RoutineOptionsKind)
	if err != nil || !found {
		return RoutineOptions{}, false, err
	}
	switch typed := value.(type) {
	case *DesiredRoutineOptions:
		if err := ValidateDesiredRoutineOptions(typed); err != nil {
			return RoutineOptions{}, true, err
		}
		return RoutineOptions(*typed), true, nil
	case *ObservedRoutineOptions:
		if err := ValidateObservedRoutineOptions(typed); err != nil {
			return RoutineOptions{}, true, err
		}
		return RoutineOptions(*typed), true, nil
	}
	return RoutineOptions{}, true, fmt.Errorf("%w: facet %q has unexpected type %T", schemaext.ErrInvalidValue, RoutineOptionsKind, value)
}

const routineOptionsDefinition = `{"type":"object","minProperties":1,"additionalProperties":false,"properties":{` +
	`"principal":` + text + `,"schema_binding":{"const":true},"recompile":{"const":true},` +
	`"returns_null_on_null_input":{"const":true}}}`

var (
	desiredRoutineOptionsShape  = wireShape[DesiredRoutineOptions]("routine options")
	observedRoutineOptionsShape = wireShape[ObservedRoutineOptions]("routine options")
)

// RoutineOptionsCodecs returns the version-one desired and observed routine
// options codecs, in that order. Each call returns independent definitions.
// An option at its default is left out of the object, and a decoder accepts
// only the object the encoder writes: every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func RoutineOptionsCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredRoutineOptions]{
			Prototype: &DesiredRoutineOptions{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(routineOptionsDefinition),
			Shape: routineOptionsShape(desiredRoutineOptionsShape), Validate: ValidateDesiredRoutineOptions,
		}.Codec(),
		schemaext.ModelCodec[*ObservedRoutineOptions]{
			Prototype: &ObservedRoutineOptions{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(routineOptionsDefinition),
			Shape: routineOptionsShape(observedRoutineOptionsShape), Validate: ValidateObservedRoutineOptions,
		}.Codec(),
	}
}

func routineOptionsShape(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, shape)
		return err
	}
}

// ownedRoutineOptionsCoverage builds the routine options codec registry once,
// on first use.
var ownedRoutineOptionsCoverage = schemaext.OwnedCoverageSource(RoutineOwner, RoutineOptionsCodecs)

// RoutineOptionsCoverage enrolls the routine options facet in a source's
// claims. knowledge is the claim for every routine the source describes;
// subjects override it for individual routines. A source that can write the
// options, such as a SQL document or a read of the catalog, claims
// [schemaext.Complete]: a routine it describes with no facet states none. A
// source that cannot write them claims nothing, and the options the server
// holds are left to the routine. A read that meets an option the facet cannot
// hold, such as ENCRYPTION, records the routine as
// [schemaext.Unrepresentable] with the option named in the reason.
func RoutineOptionsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedRoutineOptionsCoverage(RoutineOptionsKind, representation, knowledge, subjects)
}

// IdentifierNames returns the declared principal for catalog name resolution.
// A nil receiver or an option set without a principal names no identifiers.
func (v *DesiredRoutineOptions) IdentifierNames() []string {
	if v == nil || v.Principal == "" {
		return nil
	}
	return []string{v.Principal}
}

// IdentifierNames returns the observed principal for catalog name resolution.
// A nil receiver or an option set without a principal names no identifiers.
func (v *ObservedRoutineOptions) IdentifierNames() []string {
	if v == nil || v.Principal == "" {
		return nil
	}
	return []string{v.Principal}
}
