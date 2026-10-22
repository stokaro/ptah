package providerhost

import (
	"context"
	"errors"
	"fmt"
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
	var engines []string
	for _, engine := range h.description.GetEngines() {
		if slices.Contains(engine.GetFeatures(), provider.DevDatabaseFeature) {
			engines = append(engines, engine.GetId())
		}
	}
	if len(engines) == 0 {
		return nil
	}
	slices.Sort(engines)
	return &DevDatabaseOwner{host: h, engines: engines}
}

// DevDatabaseOwner provisions dev databases through the provider process.
type DevDatabaseOwner struct {
	host    *Host
	engines []string
}

// DevEngines lists the engines the provider provisions.
func (o *DevDatabaseOwner) DevEngines() []string { return slices.Clone(o.engines) }

// ProvisionDev asks the provider for a dev database. The operator's URL goes
// through as typed. The reply is a lease the release gives back, or a typed
// refusal that becomes [DevDatabaseRefusedError].
func (o *DevDatabaseOwner) ProvisionDev(ctx context.Context, rawURL string) (string, func(), error) {
	engine, err := devEngine(rawURL)
	if err != nil {
		return "", nil, err
	}
	reply, err := o.host.client.ProvisionDevDatabase(ctx, &protocol.ProvisionDevRequest{Engine: engine, Spec: rawURL})
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

func devEngine(rawURL string) (string, error) {
	rest, found := strings.CutPrefix(rawURL, "docker://")
	if !found || rest == "" {
		return "", errors.New("a provider dev database needs a docker:// URL")
	}
	engine, _, _ := strings.Cut(rest, "/")
	engine, _, _ = strings.Cut(engine, "?")
	return engine, nil
}
