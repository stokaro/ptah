// Package providerurl reads which engine a database or dev-database URL names
// when an executable provider may serve it. The built-in dialect table answers
// for the engines Ptah ships; an engine a provider serves is named only by the
// selection on the command's context, which is where this package reads it.
//
// It starts nothing and opens no connection: a lazy provider session lists the
// URL schemes it would serve without starting a process.
package providerurl

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
)

// Provided returns the target an executable provider serves for rawURL under
// ctx, and whether one does: the engine a docker URL names when a provider
// provisions that engine's dev databases, or the target the URL's scheme is
// registered to. The scheme is compared lower-cased.
func Provided(ctx context.Context, rawURL string) (string, bool) {
	if engine, owned := devdocker.ExternalEngine(ctx, rawURL); owned {
		return engine, true
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" {
		return "", false
	}
	target, served := dbschema.NativeSources(ctx)[strings.ToLower(parsed.Scheme)]
	return target, served
}

// Dialect returns the dialect or engine rawURL names: the target a provider
// serves for it under ctx, or the built-in dialect [atlasurl.DialectFromURL]
// reads, with its error for a URL that names neither.
func Dialect(ctx context.Context, rawURL string) (string, error) {
	if target, served := Provided(ctx, rawURL); served {
		return target, nil
	}
	return atlasurl.DialectFromURL(rawURL)
}

// ValidateDialectMatch is [atlasurl.ValidateDialectMatch] for a run that may
// have selected an executable provider: a URL the provider serves names the
// provider's target, and matches when that target is the target database's.
func ValidateDialectMatch(ctx context.Context, rawURL, targetDialect string) error {
	if target, served := Provided(ctx, rawURL); served {
		if target != targetDialect {
			return fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", target, targetDialect)
		}
		return nil
	}
	return atlasurl.ValidateDialectMatch(rawURL, targetDialect)
}
