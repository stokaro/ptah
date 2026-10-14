package pgdefaultprivilege

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// ComposeDeclaration joins independent declarations of this ACL identity. A
// privilege's grantable spelling wins. Contradictory grant/revoke intent or
// different nonempty comments is refused. SQL statement order is handled by its
// decoder before its resulting declaration reaches this operation.
func (v *Desired) ComposeDeclaration(other schemaext.Value) (schemaext.Value, error) {
	right, ok := other.(*Desired)
	if !ok {
		return nil, fmt.Errorf("%w: default privilege declaration cannot combine with %T", schemaext.ErrInvalidValue, other)
	}
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	if err := ValidateDesired(right); err != nil {
		return nil, err
	}
	result := &Desired{Privileges: slices.Concat(v.Privileges, right.Privileges), Revoked: slices.Concat(v.Revoked, right.Revoked), Comment: v.Comment}
	if result.Comment == "" {
		result.Comment = right.Comment
	}
	if right.Comment != "" && result.Comment != right.Comment {
		return nil, fmt.Errorf("%w: conflicting default privilege comments", schemaext.ErrDuplicate)
	}
	if err := ValidateDesired(result); err != nil {
		return nil, err
	}
	return result.Canonical(), nil
}
