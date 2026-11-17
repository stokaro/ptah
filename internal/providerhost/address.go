package providerhost

import (
	"errors"

	"ptah.run/internal/atlasurl"
)

// errProviderCABundle refuses a MySQL-family URL with ssl-ca that a provider
// would be handed.
var errProviderCABundle = errors.New("the database URL sets ssl-ca, which only Ptah's built-in MySQL connector applies, " +
	"and a provider serves this URL's engine; remove ssl-ca, or remove the provider's claim on the engine from the provider manifest")

// checkProviderAddress refuses an address the provider would be handed with a
// TLS parameter that Ptah owns. The provider receives the URL as text, and the
// protocol has no way to say that the provider verifies the server against the
// bundle. Handing the URL on could connect without the verification it asks
// for, so every request that carries an address checks it first.
func checkProviderAddress(address string) error {
	if atlasurl.NamesMySQLCABundle(address) {
		return errProviderCABundle
	}
	return nil
}
