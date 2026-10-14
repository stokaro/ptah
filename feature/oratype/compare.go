package oratype

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// checkTarget refuses a request without a context, a canceled one, and one
// for another target than Oracle.
func checkTarget(ctx context.Context, target, stage string) error {
	if ctx == nil {
		return fmt.Errorf("%w: %s requires a context", schemaext.ErrInvalidValue, stage)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !supported(target) {
		return fmt.Errorf("%w: Oracle user type %s on %q", ptaherr.ErrUnsupportedDialect, stage, target)
	}
	return nil
}

// CompareService compares declared and read user types without database
// access. Its zero value is ready for concurrent use.
//
// A type is identified by its kind, schema and name under the target's
// identifier rules, the connection's schema included, so a declaration that
// names the connection's schema and an observation that names none agree.
// A domain is compared by its base type, with both spellings reduced to the
// one ALL_DOMAIN_COLS reports, so INTEGER and NUMBER(*,0) or TIMESTAMP and
// TIMESTAMP(6) agree, and by its NOT NULL. Its CHECK and default are written when it is created
// and never compared: the catalog stores its own spelling of both, and no
// probe here asks the server how it would spell a declaration. An object
// type is compared by its attribute list, in order, names without regard to
// case and types in the spelling the catalog reports, see [SameAttributes]. A
// comment is never compared, since Oracle stores none for either kind.
//
// The comparison does not create a type whose presence the read could not
// establish, and reports it undecided instead: CREATE DOMAIN has no
// conditional form a plan could fall back on. Nor does it create a type the
// read found and could not describe, such as an object type with methods: CREATE
// OR REPLACE TYPE would replace it with the declaration. A read's claim about
// one type is matched under the comparison's identifier rules. It does not
// drop a type the desired source did not describe or asked to default.
type CompareService struct{}

// CompareObjects returns one change per type that differs. Inputs remain
// unchanged.
func (CompareService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if err := checkTarget(ctx, request.Target, "comparison"); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if len(request.Kinds) == 0 || slices.ContainsFunc(request.Kinds, func(kind schemaext.Kind) bool { return !slices.Contains(Kinds(), kind) }) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported Oracle user type comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: Oracle user type comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	pairs, err := typePairs(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	claims := subjectClaims(request.Identifiers, request.Current.Coverage)
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		compare(request, claims, pair, &result)
	}
	return result, ctx.Err()
}

// pair is one type's declaration and observation, either missing. Only the
// fields of the model its identities name are set.
type pair struct {
	desiredRef, observedRef objectidentity.ID
	desiredDomain           *DesiredDomain
	observedDomain          *ObservedDomain
	desiredObjectType       *DesiredObjectType
	observedObjectType      *ObservedObjectType
}

// declared and held report whether each side holds the type.
func (p pair) declared() bool { return p.desiredDomain != nil || p.desiredObjectType != nil }
func (p pair) held() bool     { return p.observedDomain != nil || p.observedObjectType != nil }

// ref is the identity a change names: the declaration's where there is one,
// since a statement writes the name the source spelled.
func (p pair) ref() objectidentity.ID {
	if p.declared() {
		return p.desiredRef
	}
	return p.observedRef
}

func compare(request schemaext.ObjectComparisonRequest, claims map[objectidentity.Key]schemaext.Knowledge, pair pair,
	result *schemaext.ObjectComparisonResult,
) {
	ref := pair.ref()
	kind := schemaext.Kind(ref.Kind)
	switch {
	case !pair.held():
		// A target that cannot hold the kind holds none of it, whatever the
		// read recorded, so the creation reaches the plan, which refuses it
		// by name. Oracle 21 has no CREATE DOMAIN, and its read asks nothing
		// about domains.
		current, claimed := claims[identityKey(request.Identifiers, ref)]
		if !claimed {
			current = request.Current.Coverage.Lookup(kind, ref)
		}
		if unknown(current) && request.Capabilities.Has(capabilityOf(kind)) {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: kind, Subject: ref,
				Reason: undecidedReason(Label(ref), current)})
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: ref, Value: pair.creation()})
	case !pair.declared():
		desired := request.Desired.Coverage.Lookup(kind, ref)
		if unknown(desired) || desired.State == schemaext.Defaulted {
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: ref, Value: pair.removal()})
	default:
		if change := pair.difference(); change != nil {
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: ref, Value: change})
		}
	}
}

// capabilityOf is the capability a target needs to hold a type of kind.
func capabilityOf(kind schemaext.Kind) capability.Capability {
	if kind == DomainKind {
		return capability.DomainTypes
	}
	return capability.CompositeTypes
}

// creation is the change that creates the declared type.
func (p pair) creation() schemaext.ChangeValue {
	if p.desiredDomain != nil {
		return &DomainChange{After: p.desiredDomain}
	}
	return &ObjectTypeChange{After: p.desiredObjectType}
}

// removal is the change that drops a type nothing declares.
func (p pair) removal() schemaext.ChangeValue {
	if p.observedDomain != nil {
		return &DomainChange{Before: p.observedDomain}
	}
	return &ObjectTypeChange{Before: p.observedObjectType}
}

// difference is the change of a type both sides hold, or nil where they agree.
// The comparison is one-directional for a domain: a declaration that states no
// CHECK and no default is not read as a request to remove the ones the
// catalog holds.
func (p pair) difference() schemaext.ChangeValue {
	if declared, held := p.desiredDomain, p.observedDomain; declared != nil {
		var differs []string
		if !sameBaseType(declared.BaseType, held.BaseType) {
			differs = append(differs, DiffBaseType)
		}
		if declared.NotNull != held.NotNull {
			differs = append(differs, DiffNotNull)
		}
		if len(differs) == 0 {
			return nil
		}
		return &DomainChange{Before: held, After: declared, Differs: differs}
	}
	if SameAttributes(p.desiredObjectType.Attributes, p.observedObjectType.Attributes) {
		return nil
	}
	return &ObjectTypeChange{Before: p.observedObjectType, After: p.desiredObjectType, Differs: []string{DiffAttributes}}
}

// subjectClaims keys each claim the read made about one type by the type's
// identity under the comparison's identifier rules, as the pairing keys the
// types. A read names a type without its schema, and a declaration may name
// the connection's schema; both name one type.
func subjectClaims(semantics identifier.Semantics, coverage schemaext.Coverage) map[objectidentity.Key]schemaext.Knowledge {
	claims := make(map[objectidentity.Key]schemaext.Knowledge)
	for _, record := range coverage.SubjectRecords() {
		if slices.Contains(Kinds(), record.Kind) && record.Subject.Kind == objectidentity.Kind(record.Kind) {
			claims[identityKey(semantics, record.Subject)] = record.Knowledge
		}
	}
	return claims
}

// undecidedReason says why the comparison cannot decide a declared type the
// read did not describe: the read found it and cannot describe it, or it did
// not establish whether the type exists.
func undecidedReason(label string, knowledge schemaext.Knowledge) string {
	if knowledge.State == schemaext.Unrepresentable {
		return fmt.Sprintf("the %s exists and the read cannot describe it: %s", label, knowledge.Reason)
	}
	return fmt.Sprintf("whether the %s exists was not established: %s", label, knowledge.Reason)
}

// unknown reports knowledge that establishes neither presence nor absence.
func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

// typePairs pairs declarations with observations by identity under the
// comparison's identifier rules, ordered by the qualified name a change names
// and then by kind. A value of another model than its identity names, a
// declaration among the observations and the reverse are refused.
func typePairs(request schemaext.ObjectComparisonRequest) ([]pair, error) {
	pairs := make(map[objectidentity.Key]*pair)
	for _, side := range []struct {
		state          schemaext.ObjectState
		representation schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := side.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := validOperand(object, side.representation); err != nil {
				return nil, err
			}
			key := identityKey(request.Identifiers, object.Ref)
			entry := pairs[key]
			if entry == nil {
				entry = &pair{}
				pairs[key] = entry
			}
			capture(entry, object)
		}
	}
	result := make([]pair, 0, len(pairs))
	for _, entry := range pairs {
		result = append(result, *entry)
	}
	slices.SortFunc(result, func(a, b pair) int {
		return cmp.Or(cmp.Compare(QualifiedName(a.ref()), QualifiedName(b.ref())), cmp.Compare(a.ref().Kind, b.ref().Kind))
	})
	return result, nil
}

// capture records one validated operand of entry.
func capture(entry *pair, object schemaext.Object) {
	switch value := object.Value.(type) {
	case *DesiredDomain:
		entry.desiredRef, entry.desiredDomain = object.Ref, value
	case *DesiredObjectType:
		entry.desiredRef, entry.desiredObjectType = object.Ref, value
	case *ObservedDomain:
		entry.observedRef, entry.observedDomain = object.Ref, value
	case *ObservedObjectType:
		entry.observedRef, entry.observedObjectType = object.Ref, value
	}
}

// identityKey is a type's identity under the comparison's identifier rules,
// which know the connection's schema: an empty schema takes it.
func identityKey(semantics identifier.Semantics, ref objectidentity.ID) objectidentity.Key {
	schema := ref.Schema.Source
	if ref.Schema.Defaulted {
		schema = ""
	}
	return RefWith(semantics, schemaext.Kind(ref.Kind), schema, ref.Name.Source).Key()
}

// validOperand refuses an invalid identity, a value of another model than the
// identity names, and a value of another representation than its side's.
func validOperand(object schemaext.Object, representation schemaext.Representation) error {
	if err := ValidateRef(object.Ref); err != nil {
		return err
	}
	if object.Value == nil || object.Value.Kind() != schemaext.Kind(object.Ref.Kind) {
		return fmt.Errorf("%w: unexpected Oracle user type operand %T for %s", schemaext.ErrInvalidValue, object.Value, object.Ref)
	}
	return validValue(object.Value, representation)
}
