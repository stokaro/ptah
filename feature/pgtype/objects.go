package pgtype

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// ownedCoverage builds this owner's codec registry once, on first use.
var ownedCoverage = schemaext.OwnedCoverageSource(Owner, Codecs)

// Coverage enrolls one type model in a source's claims. knowledge is the
// database-wide claim and subjects override it for individual types.
// Enrollment names the model's exact definition, so a source captured before
// a runtime gained another provider never claims it.
//
// Known absence needs complete enumeration: a read that could not list a
// schema's types claims less than complete knowledge rather than reporting
// each one absent.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge,
	subjects []schemaext.SubjectCoverage,
) (schemaext.Coverage, error) {
	return ownedCoverage(kind, representation, knowledge, subjects)
}

// CompleteCoverage enrolls the three models with complete knowledge: the
// source describes every domain, composite type and range type it holds, so
// a type it leaves out is absent.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	var combined schemaext.Coverage
	for index, kind := range Kinds() {
		claim, err := Coverage(kind, representation, schemaext.Knowledge{State: schemaext.Complete}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		if index == 0 {
			combined = claim
			continue
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, err
		}
	}
	return combined, nil
}

// DesiredObject validates a declaration and its identity, which must be of
// the declaration's model, and returns it as a feature object.
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
			err = fmt.Errorf("%w: a %s object is identified as a %s, not %q", schemaext.ErrInvalidValue,
				labels[value.Kind()], labels[value.Kind()], ref.Kind)
		}
		return schemaext.Object{}, modelError(value.Kind(), representation, err)
	}
	return schemaext.Object{Ref: ref, Value: value.Clone()}, nil
}

// validValue validates value as one of the six models in representation.
func validValue(value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *DesiredDomain:
		return wantRepresentation(representation, schemaext.Desired, DomainKind, ValidateDesiredDomain(typed))
	case *ObservedDomain:
		return wantRepresentation(representation, schemaext.Observed, DomainKind, ValidateObservedDomain(typed))
	case *DesiredComposite:
		return wantRepresentation(representation, schemaext.Desired, CompositeKind, ValidateDesiredComposite(typed))
	case *ObservedComposite:
		return wantRepresentation(representation, schemaext.Observed, CompositeKind, ValidateObservedComposite(typed))
	case *DesiredRange:
		return wantRepresentation(representation, schemaext.Desired, RangeKind, ValidateDesiredRange(typed))
	case *ObservedRange:
		return wantRepresentation(representation, schemaext.Observed, RangeKind, ValidateObservedRange(typed))
	default:
		return fmt.Errorf("%w: %T is not a PostgreSQL user type model", schemaext.ErrInvalidValue, value)
	}
}

func wantRepresentation(want, got schemaext.Representation, kind schemaext.Kind, validation error) error {
	if want != got {
		return modelError(kind, want, fmt.Errorf("%w: expected the %s representation, got the %s one", schemaext.ErrInvalidValue, want, got))
	}
	return validation
}
