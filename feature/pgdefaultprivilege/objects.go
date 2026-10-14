package pgdefaultprivilege

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DesiredObject captures a declaration with a validated structured identity.
func DesiredObject(ref objectidentity.ID, value Desired) (schemaext.Object, error) {
	if _, err := IdentityOf(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateDesired(&value); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: value.Canonical()}, nil
}

// ObservedObject captures established catalog state under its exact identity.
func ObservedObject(ref objectidentity.ID, value Observed) (schemaext.Object, error) {
	if _, err := IdentityOf(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateObserved(&value); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: value.Canonical()}, nil
}
