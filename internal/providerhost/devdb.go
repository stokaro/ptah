package providerhost

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// releaseTimeout bounds the removal of a dev database after the command ended
// or was canceled.
const releaseTimeout = 2 * time.Minute

// DevDatabaseRefusedError reports an owner that will not provision a dev
// database. Unavailable separates a runtime the owner needs but lacks from a
// request it does not serve.
type DevDatabaseRefusedError struct {
	Engine      string
	Unavailable bool
	Reason      string
}

// Error describes the refusal without the operator's URL.
func (e *DevDatabaseRefusedError) Error() string {
	kind := "does not provision this dev database"
	if e.Unavailable {
		kind = "cannot provision a dev database now"
	}
	return fmt.Sprintf("provider engine %s %s: %s", e.Engine, kind, e.Reason)
}

// DevDatabases returns the owner of the dev databases of the engines this
// provider declares with the dev database service, or nil when none does.
func (h *Host) DevDatabases() *DevDatabaseOwner {
	if !slices.Contains(h.description.GetFeatures(), provider.DevDatabaseFeature) {
		return nil
	}
	owner := &DevDatabaseOwner{host: h, realms: make(map[string]string)}
	for _, engine := range h.description.GetEngines() {
		if !slices.Contains(engine.GetFeatures(), provider.DevDatabaseFeature) {
			continue
		}
		owner.engines = append(owner.engines, engine.GetId())
		// A realm address travels only when both sides list the feature,
		// and the negotiated description keeps it only then.
		if slices.Contains(engine.GetFeatures(), provider.DevRealmsFeature) {
			for _, scheme := range append([]string{engine.GetId()}, engine.GetAliases()...) {
				owner.realms[scheme] = engine.GetId()
			}
		}
	}
	if len(owner.engines) == 0 {
		return nil
	}
	slices.Sort(owner.engines)
	return owner
}

// DevDatabaseOwner provisions dev databases through the provider process.
type DevDatabaseOwner struct {
	host    *Host
	engines []string
	// realms maps each URL scheme of an engine that creates dev realms to
	// the engine.
	realms map[string]string
}

// DevEngines lists the engines the provider provisions.
func (o *DevDatabaseOwner) DevEngines() []string { return slices.Clone(o.engines) }

// RealmSchemes lists the URL schemes of the engines whose provider creates a
// realm of its own for a run inside a database the operator names.
func (o *DevDatabaseOwner) RealmSchemes() []string {
	schemes := make([]string, 0, len(o.realms))
	for scheme := range o.realms {
		schemes = append(schemes, scheme)
	}
	slices.Sort(schemes)
	return schemes
}

// ProvisionDev asks the provider for a dev database. The operator's URL goes
// through as typed. The reply is a lease the release gives back, or a typed
// refusal that becomes [DevDatabaseRefusedError].
func (o *DevDatabaseOwner) ProvisionDev(ctx context.Context, rawURL string) (string, func(), error) {
	engine, err := devEngine(rawURL)
	if err != nil {
		return "", nil, err
	}
	return o.provision(ctx, engine, &protocol.ProvisionDevRequest{Engine: engine, Spec: rawURL})
}

// EnterRealm asks the provider for a realm of the run's own inside the
// database the operator's URL names. The URL can carry credentials, as an
// address passed to Open can; the realm's address keeps its scheme and host
// and either its credentials or none. The release removes the realm.
//
// A realm address names a host, so a URL that names none is not one a realm
// is created in: it comes back as it is, and its database is the dev
// database.
func (o *DevDatabaseOwner) EnterRealm(ctx context.Context, rawURL string) (string, func(), error) {
	address, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", nil, fmt.Errorf("the dev URL does not parse")
	}
	engine, found := o.realms[strings.ToLower(address.Scheme)]
	if !found {
		return "", nil, fmt.Errorf("the provider creates no dev realm for this URL")
	}
	if address.Host == "" {
		return rawURL, func() {}, nil
	}
	return o.provision(ctx, engine, &protocol.ProvisionDevRequest{Engine: engine, Address: rawURL})
}

func (o *DevDatabaseOwner) provision(ctx context.Context, engine string, request *protocol.ProvisionDevRequest) (string, func(), error) {
	reply, err := o.host.client.ProvisionDevDatabase(ctx, request)
	if err != nil {
		return "", nil, err
	}
	if refusal := reply.GetRefusal(); refusal != nil {
		return "", nil, &DevDatabaseRefusedError{
			Engine:      engine,
			Unavailable: refusal.GetKind() == protocol.DevDatabaseRefusal_UNAVAILABLE,
			Reason:      refusal.GetReason(),
		}
	}
	lease := reply.GetLease()
	return reply.GetUrl(), sync.OnceFunc(func() {
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		// A failed removal is reported by the provider, which also removes its
		// leases when its process ends.
		_, _ = o.host.client.ReleaseDevDatabase(release, &protocol.ReleaseDevRequest{Engine: engine, Lease: lease})
	}), nil
}

// devEngine reads the engine out of either form of docker URL the operator
// types: `docker://<engine>/...` and `docker+<engine>://...`.
func devEngine(rawURL string) (string, error) {
	if rest, found := strings.CutPrefix(rawURL, "docker://"); found && rest != "" {
		engine, _, _ := strings.Cut(rest, "/")
		engine, _, _ = strings.Cut(engine, "?")
		return engine, nil
	}
	if rest, found := strings.CutPrefix(rawURL, "docker+"); found {
		if engine, _, ok := strings.Cut(rest, "://"); ok && engine != "" {
			return engine, nil
		}
	}
	return "", errors.New("a provider dev database needs a docker:// or docker+<engine>:// URL")
}
