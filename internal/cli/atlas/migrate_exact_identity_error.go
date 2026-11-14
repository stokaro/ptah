package atlas

import (
	"ptah.run/internal/atlasreport"
)

type atlasExactIdentityError struct {
	cause   error
	message string
}

func (e *atlasExactIdentityError) Error() string { return e.message }

func (e *atlasExactIdentityError) Unwrap() error { return e.cause }

// remapAtlasExactIdentityError names each migration err mentions by the exact
// revision identity identities gives its execution-order key, keeping err in
// the chain; see [atlasreport.NameRevisionIdentities].
func remapAtlasExactIdentityError(err error, identities map[int64]string) error {
	if err == nil || len(identities) == 0 {
		return err
	}
	message := atlasreport.NameRevisionIdentities(err.Error(), identities)
	if message == err.Error() {
		return err
	}
	return &atlasExactIdentityError{cause: err, message: message}
}

var _ interface{ Unwrap() error } = (*atlasExactIdentityError)(nil)
var _ error = (*atlasExactIdentityError)(nil)
