package shadowdb

import (
	"context"
	"net/url"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// MayAddressSameDatabase reports whether candidate, a shadow or dev database
// URL, may name the database target is connected to, as
// [atlasurl.MayAddressSameDatabase] reads two URLs.
//
// A URL an executable provider serves is not read: the provider owns the
// grammar of its addresses, and the host reads none of them. Such a pair
// answers false here, and the realms the two sessions select decide once both
// are connected, which every caller asks next (devlock.SameRealm).
func MayAddressSameDatabase(ctx context.Context, target *dbschema.DatabaseConnection, candidate string) (bool, error) {
	if !target.Info().Provider.IsZero() || providerServes(ctx, candidate) {
		return false, nil
	}
	return atlasurl.MayAddressSameDatabase(target.Info().URL, candidate)
}

// providerServes reports whether an executable provider serves the scheme of
// rawURL under ctx.
func providerServes(ctx context.Context, rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" {
		return false
	}
	_, served := dbschema.NativeSources(ctx)[strings.ToLower(parsed.Scheme)]
	return served
}
