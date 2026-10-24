package atlasschema

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
)

// providerDevTarget returns the target an executable provider serves for a
// dev URL, and whether one does: the engine a docker URL names when the
// provider provisions it, or the target its URL scheme is registered to. The
// host knows neither from its own dialect table, and the provider owns the
// grammar of its addresses.
func providerDevTarget(ctx context.Context, devURL string) (string, bool) {
	if engine, owned := devdocker.ExternalEngine(ctx, devURL); owned {
		return engine, true
	}
	parsed, err := url.Parse(strings.TrimSpace(devURL))
	if err != nil || parsed.Scheme == "" {
		return "", false
	}
	target, served := dbschema.NativeSources(ctx)[strings.ToLower(parsed.Scheme)]
	return target, served
}

// validateDevDialect is [atlasurl.ValidateDialectMatch] for a run that may
// have selected an executable provider: a dev URL the provider serves names
// the provider's target, and matches when that target is the target
// database's.
func validateDevDialect(ctx context.Context, devURL, targetDialect string) error {
	if target, served := providerDevTarget(ctx, devURL); served {
		if target != targetDialect {
			return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", target, targetDialect)
		}
		return nil
	}
	return atlasurl.ValidateDialectMatch(devURL, targetDialect)
}
