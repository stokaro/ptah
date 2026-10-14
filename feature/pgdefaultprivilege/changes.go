package pgdefaultprivilege

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// ChangeKind identifies an executable default-privilege delta for one identity.
const ChangeKind schemaext.Kind = "ptah.run/pgdefaultprivilege/default-privilege-change"

// Change records rights to grant or revoke and grant options to change. Revoked
// entries retain their observed grant options for reversal. UnknownBefore marks
// convergence from an unread owner ACL; its original rights cannot be restored
// from this change. It never denotes a known empty observation.
type Change struct {
	// Before is the complete inspected delta used to reverse an ordinary
	// change. Nil means the initial state is unavailable, not empty.
	Before *Observed `json:"before,omitempty"`
	// Restore replaces the identity's entire delta with this captured state.
	// It is exclusive with individual adjustments and supports an exact
	// rollback after GRANT ALL without guessing the server's privilege list.
	Restore             *Observed `json:"restore,omitempty"`
	Granted             []Grant   `json:"granted,omitempty"`
	Revoked             []Grant   `json:"revoked,omitempty"`
	GrantOptionsAdded   []string  `json:"grant_options_added,omitempty"`
	GrantOptionsRevoked []string  `json:"grant_options_revoked,omitempty"`
	UnknownBefore       bool      `json:"unknown_before,omitempty"`
}

// Kind returns the change's owner-qualified identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns an independent change. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy returns a change sharing no privilege lists with v. Nil stays nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Before = v.Before.Copy()
	copied.Restore = v.Restore.Copy()
	copied.Granted = slices.Clone(v.Granted)
	copied.Revoked = slices.Clone(v.Revoked)
	copied.GrantOptionsAdded = slices.Clone(v.GrantOptionsAdded)
	copied.GrantOptionsRevoked = slices.Clone(v.GrantOptionsRevoked)
	return &copied
}

// RemovesState reports whether the change revokes a right or its grant option.
func (v *Change) RemovesState() bool {
	return v != nil && (v.Restore != nil || len(v.Revoked) != 0 || len(v.GrantOptionsRevoked) != 0)
}

// Validate requires a nonempty delta and valid privilege names. UnknownBefore
// permits only a single ALL grant or revoke, with no grant-option transition:
// these operations converge without knowing the owner's initial subset.
func (v *Change) Validate() error {
	if v == nil || (v.Restore == nil && len(v.Granted)+len(v.Revoked)+len(v.GrantOptionsAdded)+len(v.GrantOptionsRevoked) == 0) {
		return fmt.Errorf("%w: a default privilege change requires a delta", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObserved(v.Before); err != nil {
			return err
		}
		if v.UnknownBefore {
			return fmt.Errorf("%w: an unread ACL cannot have a before value", schemaext.ErrInvalidValue)
		}
	}
	if v.Restore != nil {
		if v.UnknownBefore || len(v.Granted)+len(v.Revoked)+len(v.GrantOptionsAdded)+len(v.GrantOptionsRevoked) != 0 {
			return fmt.Errorf("%w: a default privilege restoration cannot carry other adjustments", schemaext.ErrInvalidValue)
		}
		return ValidateObserved(v.Restore)
	}
	if err := validatePrivileges(v.Granted, nil); err != nil {
		return err
	}
	if err := validatePrivileges(v.Revoked, nil); err != nil {
		return err
	}
	if err := validatePrivileges(nil, slices.Concat(v.GrantOptionsAdded, v.GrantOptionsRevoked)); err != nil {
		return err
	}
	if v.UnknownBefore {
		rights := slices.Concat(v.Granted, v.Revoked)
		if len(rights) != 1 || rights[0].Privilege != "ALL" || len(v.GrantOptionsAdded)+len(v.GrantOptionsRevoked) != 0 {
			return fmt.Errorf("%w: an unread owner ACL requires a single ALL grant or revoke", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

// Effect reports the change to privileges on future objects. Existing object
// data remains intact; the change affects who can access newly created objects.
func (v *Change) Effect() schemaext.Effect {
	if err := v.Validate(); err != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes privileges on future objects"}
}

// AccessEffect classifies any grant as widening access, including a mixed
// change. A change containing only revokes narrows access. Invalid deltas leave
// the assessment unknown.
func (v *Change) AccessEffect() schemaext.AccessEffect {
	if err := v.Validate(); err != nil {
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "default privilege delta is invalid"}
	}
	if v.Restore != nil || len(v.Granted)+len(v.GrantOptionsAdded) != 0 {
		return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "grants rights or grant options on future objects"}
	}
	return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "revokes rights or grant options on future objects"}
}
