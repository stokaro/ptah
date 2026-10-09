package ydbcompare

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService compares YDB secrets by path alone. The server never returns a
// secret's value and a declaration never holds one, so a secret both sides hold
// is equal by its presence; a changed value is planned only when the
// declaration asks for a rotation (see [ydbsecret.RequestRotation]).
type SecretService struct{}

type secret struct {
	ref     objectidentity.ID
	desired *ydbsecret.Desired
	current *ydbsecret.Observed
}

// CompareObjects plans a creation only where the read established absence,
// since CREATE SECRET carries no guard, and a removal only where the desired
// source claims to describe the secret. An incomplete observation never
// establishes absence or a destructive change.
func (SecretService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	secrets, err := captureStandaloneInputs(ctx, request, ydbsecret.Kind, "secret", collectSecrets)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := refuseSecrets(request, secrets); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	values := make([]standalonePresence, 0, len(secrets))
	for _, value := range secrets {
		values = append(values, standalonePresence{ref: value.ref, desired: value.desired != nil, current: value.current != nil})
	}
	coverage, err := standaloneCoverage(request, values, ydbsecret.Kind, "secrets", ydbsecret.Coverage)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return completeStandaloneComparison(ctx, request, secrets, coverage, ydbsecret.Kind, "secret",
		func(value secret) objectidentity.ID { return value.ref }, compareSecret)
}

// refuseSecrets names the first secret, by path, that a target without the
// secrets capability would have to create, rotate or drop.
func refuseSecrets(request schemaext.ObjectComparisonRequest, secrets map[objectidentity.Key]secret) error {
	ordered := slices.Collect(maps.Values(secrets))
	slices.SortFunc(ordered, func(a, b secret) int { return schemaext.CompareRefs(a.ref, b.ref) })
	for _, value := range ordered {
		if value.desired == nil && value.current == nil {
			continue
		}
		return ydbsecret.Refuse(request.Target, request.Capabilities, "secret "+ydbsecret.Display(value.ref.Schema.Source, value.ref.Name.Source))
	}
	return nil
}

func compareSecret(request schemaext.ObjectComparisonRequest, value secret, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbsecret.Kind, value.ref)
	if value.desired != nil && standaloneLimited(request.Desired.Coverage, ydbsecret.Kind, value.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbsecret.Kind, Subject: value.ref,
			Reason: "the desired source cannot describe the secret"})
		return nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(ydbsecret.Kind, value.ref)
	if standaloneLimited(request.Current.Coverage, ydbsecret.Kind, value.ref) || (value.current == nil && unknown(currentKnowledge)) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbsecret.Kind, Subject: value.ref,
			Reason: "the secret or its absence was not established: " + currentKnowledge.Reason})
		return nil
	}
	if value.desired == nil && unknown(desiredKnowledge) {
		if value.current != nil {
			var err error
			result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: value.ref, Value: value.current.Desired()})
			return err
		}
		return nil
	}
	switch {
	case value.desired == nil && value.current == nil:
		return nil
	case value.desired != nil && value.current != nil && !value.desired.Rotate:
		return nil
	case value.current == nil:
		// CREATE SECRET already takes the value, so a declared secret the
		// database does not hold is created and not rotated as well.
		after := *value.desired
		after.Rotate = false
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: &ydbdiff.Secret{After: &after}})
	default:
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: &ydbdiff.Secret{Before: value.current, After: value.desired}})
	}
	return nil
}

func collectSecrets(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, secrets map[objectidentity.Key]secret) error {
	err := captureStandalone(ctx, state, direction, ydbsecret.Kind, ydbsecret.Codecs(), ydbsecret.ValidateIdentity,
		func(ref objectidentity.ID, desired *ydbsecret.Desired, current *ydbsecret.Observed) {
			value := secrets[ref.Key()]
			value.ref = ref
			if desired != nil {
				value.desired = desired
			}
			if current != nil {
				value.current = current
			}
			secrets[ref.Key()] = value
		})
	if err != nil {
		return err
	}
	for _, record := range state.Coverage.SubjectRecords() {
		if record.Kind != ydbsecret.Kind || record.Knowledge.State == schemaext.Defaulted {
			return fmt.Errorf("%w: secret coverage cannot declare another kind or a default object", schemaext.ErrInvalidValue)
		}
		if err := ydbsecret.ValidateIdentity(record.Subject); err != nil {
			return err
		}
		value := secrets[record.Subject.Key()]
		value.ref = record.Subject
		secrets[record.Subject.Key()] = value
	}
	return nil
}
