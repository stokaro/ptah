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

// ExternalRealms is an [External] owner that creates a realm of the run's own
// inside a database the operator names, for an engine whose dev database the
// operator cannot create, as SQL cannot create a YDB database.
type ExternalRealms interface {
	// RealmSchemes lists the URL schemes of the engines it creates realms for.
	RealmSchemes() []string
	// EnterRealm creates a realm in the database rawURL names. It returns the
	// realm's address and a release that removes the realm and is safe to
	// call more than once.
	EnterRealm(ctx context.Context, rawURL string) (realmURL string, release func(), err error)
}

type externalKey struct{}

// WithExternal returns a context in which [Resolve] hands docker URLs of the
// owner's engines to it. Owners accumulate: a later one is consulted after the
// earlier ones.
func WithExternal(ctx context.Context, owner External) context.Context {
	owners, _ := ctx.Value(externalKey{}).([]External)
	return context.WithValue(ctx, externalKey{}, append(slices.Clone(owners), owner))
}

// ExternalEngine returns the engine a docker URL names when an owner in ctx
// provisions that engine's dev databases, as [Resolve] hands the URL to it.
func ExternalEngine(ctx context.Context, rawURL string) (string, bool) {
	owner, engine := externalFor(ctx, rawURL)
	return engine, owner != nil
}

// externalFor returns the owner of rawURL's engine and the engine, or nil.
// Both docker URL forms name the engine: `docker://<engine>/...` in its host,
// and `docker+<engine>://...` in its scheme.
func externalFor(ctx context.Context, rawURL string) (External, string) {
	owners, _ := ctx.Value(externalKey{}).([]External)
	if len(owners) == 0 {
		return nil, ""
	}
	engine, ok := DockerEngine(rawURL)
	if !ok {
		return nil, ""
	}
	for _, owner := range owners {
		if slices.Contains(owner.DevEngines(), engine) {
			return owner, engine
		}
	}
	return nil, ""
}

// DockerEngine returns the engine either form of docker URL names, without
// asking whether Ptah starts it: the host of `docker://<engine>/...`, and the
// scheme suffix of `docker+<engine>://...`.
func DockerEngine(rawURL string) (string, bool) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "docker" {
		return parsed.Host, parsed.Host != ""
	}
	engine, found := strings.CutPrefix(scheme, "docker+")
	if !found || engine == "" {
		return "", false
	}
	return engine, true
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

// externalRealmFor returns the owner that creates realms for the engine
// rawURL's scheme names, or nil.
func externalRealmFor(ctx context.Context, rawURL string) ExternalRealms {
	owners, _ := ctx.Value(externalKey{}).([]External)
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Scheme == "" {
		return nil
	}
	for _, owner := range owners {
		if realms, ok := owner.(ExternalRealms); ok && slices.Contains(realms.RealmSchemes(), strings.ToLower(parsed.Scheme)) {
			return realms
		}
	}
	return nil
}

// enterExternalRealm creates the run's realm through its owner. The realm is
// a database inside the operator's, not a server the run owns, so it is not
// recorded as provisioned.
func enterExternalRealm(ctx context.Context, owner ExternalRealms, rawURL string) (string, func(), error) {
	realmURL, release, err := owner.EnterRealm(ctx, rawURL)
	if err != nil {
		return "", func() {}, err
	}
	return realmURL, sync.OnceFunc(release), nil
}
