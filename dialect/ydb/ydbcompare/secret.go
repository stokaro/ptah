package ydbcompare

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService compares YDB secrets by path alone. The server never returns a
// secret's value and a declaration never holds one, so a secret both sides hold
// is equal by its presence; a changed value is planned only when the caller
// requests a rotation (see [ydbsecret.RotationRequests]), by path or as the
// operator wrote the path.
type SecretService struct{}

type secret struct {
	ref     objectidentity.ID
	desired *ydbsecret.Desired
	current *ydbsecret.Observed
	rotate  bool
}

// CompareObjects plans a creation only where the read established absence,
// since CREATE SECRET carries no guard, and a removal only where the desired
// source claims to describe the secret. An incomplete observation never
// establishes absence or a destructive change, and it is reported only where
// the desired source makes a claim it would decide. A rotation request must
// name a declared secret. A target without the secrets capability is refused
// only when the comparison plans a statement.
func (SecretService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	secrets, err := captureStandaloneInputs(ctx, request, ydbsecret.Kind, "secret", collectSecrets)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	resolved, err := requestRotations(request.Requests, secrets)
	if err != nil {
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
	result, err := completeStandaloneComparison(ctx, request, secrets, coverage, ydbsecret.Kind, "secret",
		func(value secret) objectidentity.ID { return value.ref }, compareSecret)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	held := make(map[objectidentity.Key]bool)
	for key, value := range secrets {
		held[key] = value.current != nil
	}
	var drops []objectidentity.ID
	for _, change := range result.Changes {
		if value, ok := change.Value.(*ydbdiff.Secret); ok && value.After == nil {
			drops = append(drops, change.Subject)
		}
	}
	if err := refuseDottedLimits(request.Desired.Coverage, "secret", held, drops, ydbsecret.Display); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	// The changes come out in path order, so the refusal names the first
	// secret a statement would create, rotate or drop.
	if len(result.Changes) > 0 {
		ref := result.Changes[0].Subject
		if err := ydbsecret.Refuse(request.Target, request.Capabilities, "secret "+ydbsecret.Display(ref.Schema.Source, ref.Name.Source)); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
	}
	result.Resolved = resolved
	return result, nil
}

// requestRotations marks each secret a request asks to rotate, and returns the
// written requests it resolved: a path as the operator wrote it, read by
// [ydbsecret.ParsePath]. A request that names a secret the desired source
// does not declare is refused, so a typo cannot read as a rotation done, and
// so is a written path that names no secret.
func requestRotations(requests []schemaext.ChangeRequest, secrets map[objectidentity.Key]secret) ([]schemaext.ResolvedRequest, error) {
	var resolved []schemaext.ResolvedRequest
	for i, requested := range requests {
		if requested.Action != ydbsecret.RotateAction {
			return nil, fmt.Errorf("%w: a secret accepts no %q request", schemaext.ErrInvalidValue, requested.Action)
		}
		subject := requested.Subject
		if requested.Written != "" {
			ref, err := ydbsecret.ParsePath(requested.Written)
			if err != nil {
				return nil, fmt.Errorf("rotate secret: %w", err)
			}
			subject = ref
		}
		if err := ydbsecret.ValidateIdentity(subject); err != nil {
			return nil, err
		}
		value, found := secrets[subject.Key()]
		if !found || value.desired == nil {
			return nil, fmt.Errorf("rotate secret %q: %w",
				ydbsecret.Display(subject.Schema.Source, subject.Name.Source), ydbsecret.ErrRotateUndeclared)
		}
		value.rotate = true
		secrets[subject.Key()] = value
		if requested.Written != "" {
			resolved = append(resolved, schemaext.ResolvedRequest{Request: i, Subject: value.ref})
		}
	}
	return resolved, nil
}

func compareSecret(request schemaext.ObjectComparisonRequest, value secret, result *schemaext.ObjectComparisonResult) error {
	desiredKnowledge := request.Desired.Coverage.Lookup(ydbsecret.Kind, value.ref)
	if value.desired != nil && standaloneLimited(request.Desired.Coverage, ydbsecret.Kind, value.ref) {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbsecret.Kind, Subject: value.ref,
			Reason: "the desired source cannot describe the secret"})
		return nil
	}
	// A desired source that neither declares the secret nor describes its
	// namespace asks for nothing here, so an unread secret has nothing to
	// decide; one read in full is kept.
	claimed := value.desired != nil || !unknown(desiredKnowledge)
	currentKnowledge := request.Current.Coverage.Lookup(ydbsecret.Kind, value.ref)
	if standaloneLimited(request.Current.Coverage, ydbsecret.Kind, value.ref) || (value.current == nil && unknown(currentKnowledge)) {
		if claimed {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbsecret.Kind, Subject: value.ref,
				Reason: "the secret or its absence was not established: " + currentKnowledge.Reason})
		}
		return nil
	}
	if !claimed {
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
	case value.desired != nil && value.current != nil && !value.rotate:
		return nil
	case value.current == nil:
		// CREATE SECRET already takes the value, so a declared secret the
		// database does not hold is created and not rotated as well.
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: &ydbdiff.Secret{After: value.desired}})
	default:
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: value.ref, Value: &ydbdiff.Secret{Before: value.current, After: value.desired}})
	}
	return nil
}

func collectSecrets(ctx context.Context, state schemaext.ObjectState, direction schemaext.Representation, secrets map[objectidentity.Key]secret) error {
	return collectStandalone(ctx, state, direction, ydbsecret.Kind, "secret", ydbsecret.Codecs(), ydbsecret.ValidateIdentity,
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
}
