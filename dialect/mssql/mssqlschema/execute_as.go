package mssqlschema

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// ExecuteAsKind identifies the database principal a SQL Server routine runs
// as when its header names one, `WITH EXECUTE AS 'auditor'`. It is a facet of
// the routine, keyed by the routine's identity ([objectidentity.Builder.Function]
// or [objectidentity.Builder.Procedure]).
//
// Whose rights a routine runs with is the common security mode: EXECUTE AS
// CALLER is INVOKER, and every other EXECUTE AS is DEFINER. EXECUTE AS OWNER
// runs as the routine's owner and carries no facet. A named principal is
// what this facet adds, because the catalog reports it and a routine that runs
// as one is not the routine that runs as its owner: compared by the security
// mode alone, the two are equal, and replacing one with the other changes the
// principal the body runs as with nothing reporting it.
//
// EXECUTE AS SELF has no facet. Measured on SQL Server 2025 RTM-CU9, the
// catalog stores the principal that created the routine in its place, so a
// declared SELF would compare equal only to whichever account ran the apply.
const ExecuteAsKind schemaext.Kind = "ptah.run/mssql/routine-execute-as"

// RoutineOwner is the provider identity that owns the EXECUTE AS facet: the
// SQL Server target's own provider, which also reads the routine's SQL
// syntax. It differs from [Owner], the security policy owner.
const RoutineOwner = "ptah.run/sqlserver"

// HoldsExecuteAs reports whether subject is one that holds an EXECUTE AS
// principal: a function or a procedure.
func HoldsExecuteAs(subject objectidentity.ID) bool {
	return (subject.Kind == objectidentity.KindFunction || subject.Kind == objectidentity.KindProcedure) && !subject.Name.Empty()
}

// DesiredExecuteAs is the principal a declaration names in a routine's
// EXECUTE AS clause.
type DesiredExecuteAs struct {
	// Principal is the database user the routine runs as, as the declaration
	// writes it, without quotes.
	Principal string `json:"principal"`
}

// ObservedExecuteAs is the principal a routine runs as, as a read reports it.
type ObservedExecuteAs struct {
	// Principal is the name of the database principal sys.sql_modules
	// names in execute_as_principal_id.
	Principal string `json:"principal"`
}

// Kind returns the owned EXECUTE AS identity.
func (*DesiredExecuteAs) Kind() schemaext.Kind { return ExecuteAsKind }

// Kind returns the owned EXECUTE AS identity.
func (*ObservedExecuteAs) Kind() schemaext.Kind { return ExecuteAsKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredExecuteAs) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredExecuteAs)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedExecuteAs) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedExecuteAs)(nil)
	}
	return new(*v)
}

// Equal compares declarations by their principal, as written.
func (v *DesiredExecuteAs) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredExecuteAs)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations by their principal, as reported.
func (v *ObservedExecuteAs) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedExecuteAs)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Observed projects a declaration as a read of the routine created from it
// reports it. Nil and invalid declarations are refused with
// schemaext.ErrInvalidValue.
func (v *DesiredExecuteAs) Observed() (*ObservedExecuteAs, error) {
	if err := ValidateDesiredExecuteAs(v); err != nil {
		return nil, err
	}
	return &ObservedExecuteAs{Principal: v.Principal}, nil
}

// Desired captures an observation as a declaration of the principal it names.
// Nil and invalid observations are refused with schemaext.ErrInvalidValue.
func (v *ObservedExecuteAs) Desired() (*DesiredExecuteAs, error) {
	if err := ValidateObservedExecuteAs(v); err != nil {
		return nil, err
	}
	return &DesiredExecuteAs{Principal: v.Principal}, nil
}

// SamePrincipal reports identical principal spelling, ignoring trailing spaces.
// It preserves case because distinct users can differ only by case. The
// comparison service applies the connected catalog's identifier equivalence.
func SamePrincipal(a, b string) bool {
	return strings.TrimRight(a, " ") == strings.TrimRight(b, " ")
}

// ValidateDesiredExecuteAs refuses a nil declaration and a principal that is
// blank or that text a statement cannot carry. Errors are
// schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
func ValidateDesiredExecuteAs(v *DesiredExecuteAs) error {
	if v == nil {
		return executeAsError(schemaext.Desired, fmt.Errorf("%w: nil EXECUTE AS declaration", schemaext.ErrInvalidValue))
	}
	return executeAsError(schemaext.Desired, validPrincipal(v.Principal))
}

// ValidateObservedExecuteAs refuses what [ValidateDesiredExecuteAs] refuses.
func ValidateObservedExecuteAs(v *ObservedExecuteAs) error {
	if v == nil {
		return executeAsError(schemaext.Observed, fmt.Errorf("%w: nil EXECUTE AS observation", schemaext.ErrInvalidValue))
	}
	return executeAsError(schemaext.Observed, validPrincipal(v.Principal))
}

func validPrincipal(principal string) error {
	if strings.TrimSpace(principal) == "" {
		return fmt.Errorf("%w: an EXECUTE AS principal must name a database user", schemaext.ErrInvalidValue)
	}
	return schemaext.ValidText("EXECUTE AS principal", principal)
}

func executeAsError(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: ExecuteAsKind, Representation: representation, Message: err.Error()}
}

// WithExecuteAs returns facets with a declaration of principal added, as a
// SQL statement writes `WITH EXECUTE AS 'principal'`. A collection that
// already holds the kind, and an invalid principal, are refused.
func WithExecuteAs(facets schemaext.Facets, principal string) (schemaext.Facets, error) {
	declared := &DesiredExecuteAs{Principal: principal}
	if err := ValidateDesiredExecuteAs(declared); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(declared)
}

// WithObservedExecuteAs returns facets with an observation of principal added,
// as a read of a SQL Server routine records it. A collection that already
// holds the kind, and an invalid principal, are refused.
func WithObservedExecuteAs(facets schemaext.Facets, principal string) (schemaext.Facets, error) {
	observed := &ObservedExecuteAs{Principal: principal}
	if err := ValidateObservedExecuteAs(observed); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(observed)
}

// ExecuteAsPrincipal returns the principal facets hold in either
// representation, and false when they hold none. A value of another type, and
// an invalid value, are refused with schemaext.ErrInvalidValue.
func ExecuteAsPrincipal(facets schemaext.Facets) (string, bool, error) {
	value, found, err := facets.Get(ExecuteAsKind)
	if err != nil || !found {
		return "", false, err
	}
	switch typed := value.(type) {
	case *DesiredExecuteAs:
		return typed.Principal, true, ValidateDesiredExecuteAs(typed)
	case *ObservedExecuteAs:
		return typed.Principal, true, ValidateObservedExecuteAs(typed)
	}
	return "", true, fmt.Errorf("%w: facet %q has unexpected type %T", schemaext.ErrInvalidValue, ExecuteAsKind, value)
}

const executeAsDefinition = `{"type":"object","required":["principal"],"additionalProperties":false,"properties":{` +
	`"principal":` + text + `}}`

var (
	desiredExecuteAsShape  = wireShape[DesiredExecuteAs]("EXECUTE AS principal")
	observedExecuteAsShape = wireShape[ObservedExecuteAs]("EXECUTE AS principal")
)

// ExecuteAsCodecs returns the version-one desired and observed EXECUTE AS
// codecs, in that order. Each call returns independent definitions. A decoder
// accepts only the object the encoder writes, and every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func ExecuteAsCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredExecuteAs]{
			Prototype: &DesiredExecuteAs{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(executeAsDefinition),
			Shape: executeAsShape(desiredExecuteAsShape), Validate: ValidateDesiredExecuteAs,
		}.Codec(),
		schemaext.ModelCodec[*ObservedExecuteAs]{
			Prototype: &ObservedExecuteAs{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(executeAsDefinition),
			Shape: executeAsShape(observedExecuteAsShape), Validate: ValidateObservedExecuteAs,
		}.Codec(),
	}
}

func executeAsShape(shape schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, shape)
		return err
	}
}

// ownedExecuteAsCoverage builds the EXECUTE AS codec registry once, on first use.
var ownedExecuteAsCoverage = schemaext.OwnedCoverageSource(RoutineOwner, ExecuteAsCodecs)

// ExecuteAsCoverage enrolls the EXECUTE AS facet in a source's claims.
// knowledge is the claim for every routine the source describes; subjects
// override it for individual routines. A source that can write the clause,
// such as a SQL document or a read of the catalog, claims
// [schemaext.Complete]: a routine it describes with no principal runs as no
// named principal. A source that cannot write it claims nothing, and a
// principal the server holds is then left undecided rather than removed.
func ExecuteAsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return ownedExecuteAsCoverage(ExecuteAsKind, representation, knowledge, subjects)
}

// IdentifierNames returns the declared principal for catalog name resolution.
// A nil receiver names no identifiers.
func (v *DesiredExecuteAs) IdentifierNames() []string {
	if v == nil {
		return nil
	}
	return []string{v.Principal}
}

// IdentifierNames returns the observed principal for catalog name resolution.
// A nil receiver names no identifiers.
func (v *ObservedExecuteAs) IdentifierNames() []string {
	if v == nil {
		return nil
	}
	return []string{v.Principal}
}
