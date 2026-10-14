package privilegecompare

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func capturePairs(request schemaext.ObjectComparisonRequest) ([]pair, error) {
	pairs := make(map[objectidentity.Key]*pair)
	for _, side := range []struct {
		state          schemaext.ObjectState
		representation schemaext.Representation
	}{
		{request.Current, schemaext.Observed}, {request.Desired, schemaext.Desired},
	} {
		objects, err := side.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			captured, err := capturePair(pairs, object.Ref, request.Identifiers)
			if err != nil {
				return nil, err
			}
			if err := captureValue(captured, object.Value, side.representation); err != nil {
				return nil, err
			}
		}
		for _, claim := range side.state.Coverage.SubjectRecords() {
			if err := pgdefaultprivilege.ValidateCoverageSubject(claim); err != nil {
				return nil, err
			}
			if claim.Subject.Kind == pgdefaultprivilege.ClassSubjectKind {
				if err := validateClassTarget(claim.Subject, request.Identifiers); err != nil {
					return nil, err
				}
				continue
			}
			captured, err := capturePair(pairs, claim.Subject, request.Identifiers)
			if err != nil {
				return nil, err
			}
			if err := captureClaim(captured, claim.Knowledge, side.representation); err != nil {
				return nil, err
			}
		}
	}
	result := make([]pair, 0, len(pairs))
	for _, captured := range pairs {
		result = append(result, *captured)
	}
	slices.SortFunc(result, func(a, b pair) int { return schemaext.CompareRefs(a.ref, b.ref) })
	return result, nil
}

func capturePair(pairs map[objectidentity.Key]*pair, ref objectidentity.ID, semantics identifier.Semantics) (*pair, error) {
	identity, err := pgdefaultprivilege.IdentityOf(ref)
	if err != nil {
		return nil, err
	}
	expected, err := pgdefaultprivilege.Ref(identity, semantics)
	if err != nil {
		return nil, err
	}
	if expected != ref {
		return nil, fmt.Errorf("%w: default privilege identity does not match target semantics", schemaext.ErrInvalidValue)
	}
	captured := pairs[ref.Key()]
	if captured == nil {
		captured = &pair{}
		pairs[ref.Key()] = captured
	}
	captured.ref, captured.identity = ref, identity
	return captured, nil
}

func captureClaim(captured *pair, knowledge schemaext.Knowledge, representation schemaext.Representation) error {
	if representation == schemaext.Desired && (knowledge.State == schemaext.Absent || knowledge.State == schemaext.Defaulted) {
		if captured.desired != nil {
			return fmt.Errorf("%w: a default privilege declaration conflicts with an absence or default claim", schemaext.ErrInvalidValue)
		}
		captured.desired = &pgdefaultprivilege.Desired{}
	}
	return nil
}

func captureValue(captured *pair, value schemaext.Value, representation schemaext.Representation) error {
	switch v := value.(type) {
	case *pgdefaultprivilege.Desired:
		if representation != schemaext.Desired {
			return fmt.Errorf("%w: a default privilege declaration is not an observation", schemaext.ErrInvalidValue)
		}
		if err := pgdefaultprivilege.ValidateDesired(v); err != nil {
			return err
		}
		captured.desired = v.Canonical()
	case *pgdefaultprivilege.Observed:
		if representation != schemaext.Observed {
			return fmt.Errorf("%w: a default privilege observation is not a declaration", schemaext.ErrInvalidValue)
		}
		if err := pgdefaultprivilege.ValidateObserved(v); err != nil {
			return err
		}
		captured.observed = v.Canonical()
	default:
		return fmt.Errorf("%w: unsupported default privilege value %T", schemaext.ErrInvalidValue, value)
	}
	return nil
}

// validateClassTarget checks normalization without creating a grantee pair for
// a catalog class whose contents remain unknown.
func validateClassTarget(ref objectidentity.ID, semantics identifier.Semantics) error {
	cls, err := pgdefaultprivilege.ClassOf(ref)
	if err != nil {
		return err
	}
	expected, err := pgdefaultprivilege.ClassRef(cls, semantics)
	if err != nil {
		return err
	}
	if expected != ref {
		return fmt.Errorf("%w: default privilege class does not match target semantics", schemaext.ErrInvalidValue)
	}
	return nil
}
