package ydbrender

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// refuseKey refuses subject on a YDB target without the capability key.
func refuseKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: string(key), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, platform.YDB)}
}

// refuseFact refuses subject for a reason the target's keys do not express.
func refuseFact(subject, reason string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: subject, Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, reason)}
}
