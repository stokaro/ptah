package oratype

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// ownedCoverage builds this owner's codec registry once, on first use.
var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls one type model in a source's claims. knowledge is the
// database-wide claim and subjects override it for individual types.
// Enrollment names the model's exact definition, so a source captured before
// a runtime gained another provider never claims it.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge,
	subjects []schemaext.SubjectCoverage,
) (schemaext.Coverage, error) {
	return ownedCoverage(kind, representation, knowledge, subjects)
}

// CompleteCoverage enrolls the two models with complete knowledge: the source
// describes every domain and object type it holds, so a type it leaves out is
// absent.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	domains, err := Coverage(DomainKind, representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	objectTypes, err := Coverage(ObjectTypeKind, representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return domains.Combine(objectTypes)
}

// DesiredObject validates a declaration and its identity, which must be of the
// declaration's model, and returns it as a feature object.
func DesiredObject(ref objectidentity.ID, value schemaext.Value) (schemaext.Object, error) {
	return object(ref, value, schemaext.Desired)
}

// ObservedObject validates an observation and its identity, which must be of
// the observation's model, and returns it as a feature object.
func ObservedObject(ref objectidentity.ID, value schemaext.Value) (schemaext.Object, error) {
	return object(ref, value, schemaext.Observed)
}

func object(ref objectidentity.ID, value schemaext.Value, representation schemaext.Representation) (schemaext.Object, error) {
	if err := validValue(value, representation); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateRef(ref); err != nil || ref.Kind != objectidentity.Kind(value.Kind()) {
		if err == nil {
			err = fmt.Errorf("%w: the %s object is identified as %q", schemaext.ErrInvalidValue, labels[value.Kind()], ref.Kind)
		}
		return schemaext.Object{}, modelError(value.Kind(), representation, err)
	}
	return schemaext.Object{Ref: ref, Value: value.Clone()}, nil
}

// validValue validates value as one of the four models in representation.
func validValue(value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *DesiredDomain:
		return wantRepresentation(representation, schemaext.Desired, DomainKind, ValidateDesiredDomain(typed))
	case *ObservedDomain:
		return wantRepresentation(representation, schemaext.Observed, DomainKind, ValidateObservedDomain(typed))
	case *DesiredObjectType:
		return wantRepresentation(representation, schemaext.Desired, ObjectTypeKind, ValidateDesiredObjectType(typed))
	case *ObservedObjectType:
		return wantRepresentation(representation, schemaext.Observed, ObjectTypeKind, ValidateObservedObjectType(typed))
	default:
		return fmt.Errorf("%w: %T is not an Oracle user type model", schemaext.ErrInvalidValue, value)
	}
}

func wantRepresentation(want, got schemaext.Representation, kind schemaext.Kind, validation error) error {
	if want != got {
		return modelError(kind, want, fmt.Errorf("%w: expected the %s representation, got the %s one", schemaext.ErrInvalidValue, want, got))
	}
	return validation
}

// Selections is how a command's scope names the owner's types: a domain and an
// object type each by its own name, in the schema that holds it, with the
// selector types the common types use, `domain` and `composite_type`. An
// exclusion subtracts no field from one.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: DomainKind, Type: "domain", Naming: selectext.InSchema},
		{Kind: ObjectTypeKind, Type: "composite_type", Naming: selectext.InSchema},
	}}
}
