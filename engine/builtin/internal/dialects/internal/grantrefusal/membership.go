package grantrefusal

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// Membership refuses a membership of one role in another, which only the YDB
// renderer writes, wrapping [ptaherr.ErrUnsupportedFeature]. statement is the
// clause refused, such as `ADD member TO role`.
//
// A target without [capability.RoleMembership] is refused by the key, the
// answer every withheld kind gets. One with the key and this renderer is
// refused as unwritten, because a renderer that wrote nothing for a membership
// would let an apply exit 0 with the member holding nothing it was declared to.
func Membership(dialect string, caps capability.Capabilities, statement string) error {
	normalized := platform.NormalizeDialect(dialect)
	if !caps.Has(capability.RoleMembership) {
		return &ptaherr.CapabilityError{
			Dialect: normalized,
			Feature: string(capability.RoleMembership),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
				statement, capability.RoleMembership, normalized),
		}
	}
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: string(capability.RoleMembership),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s renderer writes no role membership", statement, normalized),
	}
}
