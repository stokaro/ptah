package pgpolicy

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// NormalizedPolicy is a connected server's spelling of a declared policy: the
// role list and the clauses as pg_policy would report them for the
// declaration.
//
// PostgreSQL stores a parse tree rather than the expression text, and the cast
// it inserts depends on the type of the column the expression names, so a
// declared clause and the catalog's differ for a policy nobody changed. It
// also resolves CURRENT_ROLE, CURRENT_USER and SESSION_USER to a role name
// when the policy is created. A probe puts the declaration through the same
// server, and a comparison compares its answer with the observation.
type NormalizedPolicy struct {
	// Roles is the TO list as the catalog records it: the PUBLIC keyword alone
	// or role names, a set.
	Roles []RoleSelector `json:"roles"`
	// Using and WithCheck are nil exactly where the declaration has no such
	// clause.
	Using     *string `json:"using,omitempty"`
	WithCheck *string `json:"with_check,omitempty"`
}

// Copy returns an independent copy. A nil receiver returns nil.
func (v *NormalizedPolicy) Copy() *NormalizedPolicy {
	if v == nil {
		return nil
	}
	return &NormalizedPolicy{Roles: slices.Clone(v.Roles), Using: cloneText(v.Using), WithCheck: cloneText(v.WithCheck)}
}

func (v *NormalizedPolicy) equal(right *NormalizedPolicy) bool {
	if v == nil || right == nil {
		return v == right
	}
	return sameRoles(v.Roles, right.Roles) && equalText(v.Using, right.Using) && equalText(v.WithCheck, right.WithCheck)
}

// validate checks a server answer against the declaration it answers for: the
// roles an observation can hold, and a clause exactly where the declaration
// has one.
func (v *NormalizedPolicy) validate(declared *DesiredPolicy) error {
	if v == nil {
		return nil
	}
	if len(v.Roles) == 0 {
		return fmt.Errorf("%w: a normalized policy names at least one role", schemaext.ErrInvalidValue)
	}
	if err := validateRoles(v.Roles, []RoleKeyword{Public}); err != nil {
		return err
	}
	if (v.Using == nil) != (declared.Using == nil) || (v.WithCheck == nil) != (declared.WithCheck == nil) {
		return fmt.Errorf("%w: a normalized policy holds exactly the clauses its declaration holds", schemaext.ErrInvalidValue)
	}
	return validateClauses(declared.Command, v.Using, v.WithCheck)
}

// Observed predicts the observation a server reports for the declaration, with
// PostgreSQL's defaults resolved. A server's answer, where one is attached,
// replaces the declared roles and clauses. Without one, a role keyword the
// server resolves when the policy is created has no prediction, and the
// projection is refused rather than guessed.
func (v *DesiredPolicy) Observed() (*ObservedPolicy, error) {
	if err := ValidateDesiredPolicy(v); err != nil {
		return nil, err
	}
	observed := &ObservedPolicy{Command: v.EffectiveCommand(), Roles: v.EffectiveRoles(), Using: cloneText(v.Using),
		WithCheck: cloneText(v.WithCheck), Composition: v.EffectiveComposition(), Comment: v.Comment}
	if v.Normalized != nil {
		observed.Roles = slices.Clone(v.Normalized.Roles)
		observed.Using, observed.WithCheck = cloneText(v.Normalized.Using), cloneText(v.Normalized.WithCheck)
	}
	if slices.ContainsFunc(observed.Roles, resolvedAtCreation) {
		return nil, fmt.Errorf("%w: a policy TO %s has no observation until a server resolves the keyword",
			schemaext.ErrInvalidValue, observed.Roles[slices.IndexFunc(observed.Roles, resolvedAtCreation)])
	}
	return observed, ValidateObservedPolicy(observed)
}

// Desired returns the declaration that asks for exactly this policy: every
// value the observation holds, written out.
func (v *ObservedPolicy) Desired() (*DesiredPolicy, error) {
	if err := ValidateObservedPolicy(v); err != nil {
		return nil, err
	}
	return &DesiredPolicy{Command: v.Command, Roles: slices.Clone(v.Roles), Using: cloneText(v.Using),
		WithCheck: cloneText(v.WithCheck), Composition: v.Composition, Comment: v.Comment}, nil
}

// Observed predicts the switches a server reports for the declaration.
func (v *DesiredTableState) Observed() (*ObservedTableState, error) {
	if err := ValidateDesiredTableState(v); err != nil {
		return nil, err
	}
	return &ObservedTableState{Enabled: v.Enabled, Forced: v.Forced}, nil
}

// Desired returns the declaration that asks for exactly these switches.
func (v *ObservedTableState) Desired() (*DesiredTableState, error) {
	if err := ValidateObservedTableState(v); err != nil {
		return nil, err
	}
	return &DesiredTableState{Enabled: v.Enabled, Forced: v.Forced}, nil
}
