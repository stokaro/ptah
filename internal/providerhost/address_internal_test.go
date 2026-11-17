package providerhost

// White-box testing required: a provider receives an address only from a
// started provider process, and the refusal has to come before any request
// reaches it. These tests call each method that sends an address with no host
// behind it, so a request that got past the check would reach the nil client
// and fail the test. The session path is covered black-box as well, by
// integration/providercli/provider_mysql_ca_bundle_e2e_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
)

// caBundleAddress is a MySQL URL whose ssl-ca only Ptah's connector applies.
const caBundleAddress = "mysql://app@db.internal:3306/shop?SSL_CA=/etc/mysql/ca.pem"

// TestEveryAddressHandoffRefusesACABundle refuses the URL on each request
// that would hand it to the provider: a session, a database context and a dev
// realm.
func TestEveryAddressHandoffRefusesACABundle(t *testing.T) {
	t.Run("a session", func(t *testing.T) {
		c := qt.New(t)

		session, err := (&target{}).Open(c.Context(), dbschema.NativeRequest{Address: caBundleAddress})

		c.Assert(err, qt.ErrorIs, errProviderCABundle)
		c.Assert(session, qt.IsNil)
	})

	t.Run("a database context", func(t *testing.T) {
		c := qt.New(t)

		got, err := (&target{}).ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Address: caBundleAddress})

		c.Assert(err, qt.ErrorIs, errProviderCABundle)
		c.Assert(got.IsZero(), qt.IsTrue)
	})

	t.Run("a dev realm", func(t *testing.T) {
		c := qt.New(t)
		owner := &DevDatabaseOwner{realms: map[string]string{"mysql": "mysql"}}

		realm, release, err := owner.EnterRealm(c.Context(), caBundleAddress)

		c.Assert(err, qt.ErrorIs, errProviderCABundle)
		c.Assert(realm, qt.Equals, "")
		c.Assert(release, qt.IsNil)
	})
}
