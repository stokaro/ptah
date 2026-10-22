package devdocker

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"sync"
)

// External provisions the dev databases of engines a selected provider owns.
// The provider chooses images, ports and cleanup. The host passes the URL the
// operator wrote and starts no container of its own for these engines.
type External interface {
	// DevEngines lists the engine names a docker URL may carry for this owner.
	DevEngines() []string
	// ProvisionDev creates a dev database for rawURL. The release function must
	// be called once the caller is done; it is safe to call more than once.
	ProvisionDev(ctx context.Context, rawURL string) (devURL string, release func(), err error)
}

type externalKey struct{}

// WithExternal returns a context in which [Resolve] hands docker URLs of the
// owner's engines to it. Owners accumulate: a later one is consulted after the
// earlier ones.
func WithExternal(ctx context.Context, owner External) context.Context {
	owners, _ := ctx.Value(externalKey{}).([]External)
	return context.WithValue(ctx, externalKey{}, append(slices.Clone(owners), owner))
}

// externalFor returns the owner of rawURL's engine, or nil.
func externalFor(ctx context.Context, rawURL string) External {
	owners, _ := ctx.Value(externalKey{}).([]External)
	if len(owners) == 0 {
		return nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(parsed.Scheme, "docker") {
		return nil
	}
	for _, owner := range owners {
		if slices.Contains(owner.DevEngines(), parsed.Host) {
			return owner
		}
	}
	return nil
}

// resolveExternal provisions through the owner and records the address as one
// this process started, so replay treats it as a server the run owns.
func resolveExternal(ctx context.Context, owner External, rawURL string) (string, func(), error) {
	devURL, release, err := owner.ProvisionDev(ctx, rawURL)
	if err != nil {
		return "", func() {}, err
	}
	recordProvisioned(devURL)
	return devURL, sync.OnceFunc(func() {
		release()
		forgetProvisioned(devURL)
	}), nil
}
