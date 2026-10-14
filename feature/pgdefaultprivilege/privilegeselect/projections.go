package privilegeselect

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
)

// SelectAdditions retains grants and grant-option additions from each change.
// Pure revocations and whole-ACL restoration are omitted. Before remains intact
// so reversing the retained additions can recover the inspected ACL.
func (Service) SelectAdditions(ctx context.Context, request selectext.AdditiveRequest) (selectext.AdditiveResult, error) {
	if err := projectionContext(ctx); err != nil {
		return selectext.AdditiveResult{}, err
	}
	result := selectext.AdditiveResult{Complete: true}
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return selectext.AdditiveResult{}, err
		}
		if _, err := pgdefaultprivilege.IdentityOf(record.Subject); err != nil {
			return selectext.AdditiveResult{}, err
		}
		value, ok := record.Value.(*pgdefaultprivilege.Change)
		if !ok {
			return selectext.AdditiveResult{}, fmt.Errorf("%w: unexpected default privilege change %T", schemaext.ErrInvalidValue, record.Value)
		}
		if err := value.Validate(); err != nil {
			return selectext.AdditiveResult{}, err
		}
		decision := selectext.AdditiveDecision{Subject: record.Subject, Kind: pgdefaultprivilege.ChangeKind}
		if len(value.Granted)+len(value.GrantOptionsAdded) > 0 {
			kept := value.Copy()
			kept.Revoked, kept.GrantOptionsRevoked = nil, nil
			decision.Value = kept
		}
		result.Decisions = append(result.Decisions, decision)
	}
	if err := ctx.Err(); err != nil {
		return selectext.AdditiveResult{}, err
	}
	return result, nil
}

// SubtractBaseline matches individual rights within an ACL. Grant options and
// the granted/revoked direction do not change a right's identity. A declaration
// of ALL retains every right; an empty declaration retains none of the baseline.
func (Service) SubtractBaseline(ctx context.Context, request selectext.BaselineRequest) (selectext.BaselineResult, error) {
	if err := projectionContext(ctx); err != nil {
		return selectext.BaselineResult{}, err
	}
	current, err := observedACLs(request.Current)
	if err != nil {
		return selectext.BaselineResult{}, err
	}
	starting, err := observedACLs(request.StartingPoint)
	if err != nil {
		return selectext.BaselineResult{}, err
	}
	declared, err := observedACLs(request.Declared)
	if err != nil {
		return selectext.BaselineResult{}, err
	}
	result := selectext.BaselineResult{Complete: true}
	for _, ref := range request.Current.Refs() {
		if err := ctx.Err(); err != nil {
			return selectext.BaselineResult{}, err
		}
		value := current[ref.Key()]
		before, keep := rightNames(starting[ref.Key()]), rightNames(declared[ref.Key()])
		retain := func(name string) bool { return !before[name] && !before["ALL"] || keep[name] || keep["ALL"] }
		filtered := value.Copy()
		filtered.Privileges = slices.DeleteFunc(filtered.Privileges, func(grant pgdefaultprivilege.Grant) bool { return !retain(grant.Privilege) })
		filtered.Revoked = slices.DeleteFunc(filtered.Revoked, func(name string) bool { return !retain(name) })
		decision := selectext.BaselineDecision{Subject: ref}
		if len(filtered.Privileges)+len(filtered.Revoked) > 0 || starting[ref.Key()] == nil {
			decision.Value = filtered
		}
		result.Decisions = append(result.Decisions, decision)
	}
	if err := ctx.Err(); err != nil {
		return selectext.BaselineResult{}, err
	}
	return result, nil
}

func observedACLs(objects schemaext.Objects) (map[objectidentity.Key]*pgdefaultprivilege.Observed, error) {
	values, err := objects.All()
	if err != nil {
		return nil, err
	}
	result := make(map[objectidentity.Key]*pgdefaultprivilege.Observed, len(values))
	for _, object := range values {
		if _, err := pgdefaultprivilege.IdentityOf(object.Ref); err != nil {
			return nil, err
		}
		value, ok := object.Value.(*pgdefaultprivilege.Observed)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected default privilege observation %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := pgdefaultprivilege.ValidateObserved(value); err != nil {
			return nil, err
		}
		result[object.Ref.Key()] = value.Canonical()
	}
	return result, nil
}

func rightNames(value *pgdefaultprivilege.Observed) map[string]bool {
	result := make(map[string]bool)
	if value == nil {
		return result
	}
	for _, grant := range value.Privileges {
		result[grant.Privilege] = true
	}
	for _, name := range value.Revoked {
		result[name] = true
	}
	return result
}

func projectionContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: default privilege selection requires a context", schemaext.ErrInvalidValue)
	}
	return ctx.Err()
}
