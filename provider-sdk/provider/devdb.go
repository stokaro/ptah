package provider

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// DevDatabaseFeature advertises throwaway-database provisioning for the engines
// that declare it. The owner chooses images, ports and health checks, and removes
// everything it created. The host passes the operator's request and never names
// an image.
const DevDatabaseFeature = "database.dev"

// DevRealmsFeature is an engine feature beside DevDatabaseFeature. The owner
// takes a database the operator names, creates a realm of its own for the
// run there and removes it on release, for an engine whose dev database the
// operator cannot create, such as a directory in a database only an
// administrator creates. It is a property of the engine, not a service of its
// own.
const DevRealmsFeature = "database.dev-realms"

// maxDevText bounds an operator spec, a lease and a refusal reason.
const maxDevText = 1024

// DevProvisioner creates and removes throwaway databases.
type DevProvisioner interface {
	// ProvisionDevDatabase creates a database for the spec, or refuses with a typed
	// refusal when it will not. A canceled call leaves nothing behind: the owner
	// removes what it started before it returns.
	ProvisionDevDatabase(context.Context, *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error)
	// ReleaseDevDatabase removes what a lease names. Releasing a lease that is
	// already gone succeeds. The owner also removes its leases when its process
	// ends, because a host that crashed never calls this.
	ReleaseDevDatabase(context.Context, *protocol.ReleaseDevRequest) (*protocol.Empty, error)
}

func devDatabaseEngine(description *protocol.Description, name string) (*protocol.Engine, error) {
	if !slices.Contains(description.GetFeatures(), DevDatabaseFeature) {
		return nil, fmt.Errorf("%w: dev database provisioning is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, name, nil)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(engine.GetFeatures(), DevDatabaseFeature) {
		return nil, fmt.Errorf("%w: engine dev database provisioning is unavailable", ErrProtocol)
	}
	return engine, nil
}

// validateDevServices checks the engine's dev database declarations: each
// service the engine names is one the provider serves, and realms are a form
// of provisioning.
func validateDevServices(description *protocol.Description, engine *protocol.Engine) error {
	for _, feature := range []string{DevDatabaseFeature, DevCleanupFeature, ReplayGuardFeature} {
		if slices.Contains(engine.GetFeatures(), feature) && !slices.Contains(description.GetFeatures(), feature) {
			return fmt.Errorf("%w: engine %s requires the service", ErrProtocol, feature)
		}
	}
	if slices.Contains(engine.GetFeatures(), DevRealmsFeature) && !slices.Contains(engine.GetFeatures(), DevDatabaseFeature) {
		return fmt.Errorf("%w: engine dev realms require dev database provisioning", ErrProtocol)
	}
	if slices.Contains(description.GetFeatures(), DevRealmsFeature) {
		return fmt.Errorf("%w: dev realms are an engine feature, not a service", ErrProtocol)
	}
	return nil
}

func validDevText(text string, allowEmpty bool) bool {
	return (allowEmpty || text != "") && len(text) <= maxDevText && utf8.ValidString(text) &&
		!strings.ContainsFunc(text, unicode.IsControl)
}

// ValidateProvisionDevRequest checks the negotiated service and the operator's
// spec or address. A request carries one of them. An address needs an engine
// that declares DevRealmsFeature, and a scheme and a host to keep.
func ValidateProvisionDevRequest(description *protocol.Description, request *protocol.ProvisionDevRequest) error {
	if request == nil {
		return fmt.Errorf("%w: dev database request is missing", ErrProtocol)
	}
	engine, err := devDatabaseEngine(description, request.Engine)
	if err != nil {
		return err
	}
	if !validDevText(request.Spec, true) {
		return fmt.Errorf("%w: invalid dev database spec", ErrProtocol)
	}
	if request.Address == "" {
		return nil
	}
	if request.Spec != "" {
		return fmt.Errorf("%w: a dev database request carries a spec or an address, not both", ErrProtocol)
	}
	if !slices.Contains(engine.GetFeatures(), DevRealmsFeature) {
		return fmt.Errorf("%w: engine dev realms are unavailable", ErrProtocol)
	}
	if _, err := realmAddress(request.Address); err != nil {
		return err
	}
	return nil
}

// realmAddress parses the address a realm is created in. Its text is not
// repeated in an error, since it can hold credentials.
func realmAddress(text string) (*url.URL, error) {
	address, err := url.Parse(text)
	if err != nil || !validDevText(text, false) || address.Scheme == "" || address.Host == "" {
		return nil, fmt.Errorf("%w: a dev realm address needs a scheme and a host", ErrProtocol)
	}
	return address, nil
}

// ValidateDevDatabase refuses a reply that is neither a lease with an address nor
// a typed refusal, and a refusal with an unknown kind or no reason. The address
// must carry a scheme. A provisioned database's address carries no credentials:
// the host opens it, and a secret in a reply would travel into diagnostics. A
// realm's address keeps the scheme and host of the operator's address, and its
// credentials or none.
func ValidateDevDatabase(description *protocol.Description, request *protocol.ProvisionDevRequest, result *protocol.DevDatabase) error {
	if err := ValidateProvisionDevRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: dev database reply is missing", ErrProtocol)
	}
	if refusal := result.Refusal; refusal != nil {
		if result.Lease != "" || result.Url != "" {
			return fmt.Errorf("%w: a refused dev database carries a lease or an address", ErrProtocol)
		}
		switch refusal.Kind {
		case protocol.DevDatabaseRefusal_UNSUPPORTED, protocol.DevDatabaseRefusal_UNAVAILABLE:
		default:
			return fmt.Errorf("%w: dev database refusal has no kind", ErrProtocol)
		}
		if !validDevText(refusal.Reason, false) || strings.TrimSpace(refusal.Reason) == "" {
			return fmt.Errorf("%w: dev database refusal has no reason", ErrProtocol)
		}
		return nil
	}
	if !validDevText(result.Lease, false) || !validDevText(result.Url, false) {
		return fmt.Errorf("%w: dev database reply needs a lease and an address", ErrProtocol)
	}
	address, err := url.Parse(result.Url)
	if err != nil || address.Scheme == "" {
		return fmt.Errorf("%w: dev database address needs a scheme", ErrProtocol)
	}
	if request.Address == "" {
		if address.User != nil {
			return fmt.Errorf("%w: a provisioned dev database address carries credentials", ErrProtocol)
		}
		return nil
	}
	// A realm lives in the database the operator named. Its address reaches
	// that server with the operator's credentials or none, never another
	// server or another identity.
	origin, err := realmAddress(request.Address)
	if err != nil {
		return err
	}
	if address.Scheme != origin.Scheme || address.Host != origin.Host {
		return fmt.Errorf("%w: a dev realm address leaves the database it was created in", ErrProtocol)
	}
	if address.User != nil && address.User.String() != origin.User.String() {
		return fmt.Errorf("%w: a dev realm address changes the operator's credentials", ErrProtocol)
	}
	return nil
}

// ValidateReleaseDevRequest checks the negotiated service and the lease.
func ValidateReleaseDevRequest(description *protocol.Description, request *protocol.ReleaseDevRequest) error {
	if request == nil {
		return fmt.Errorf("%w: dev database release is missing", ErrProtocol)
	}
	if _, err := devDatabaseEngine(description, request.Engine); err != nil {
		return err
	}
	if !validDevText(request.Lease, false) {
		return fmt.Errorf("%w: dev database release needs a lease", ErrProtocol)
	}
	return nil
}

// ValidateReleasedDev checks that the reply answers a valid release.
func ValidateReleasedDev(description *protocol.Description, request *protocol.ReleaseDevRequest, result *protocol.Empty) error {
	if err := ValidateReleaseDevRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: dev database release reply is missing", ErrProtocol)
	}
	return nil
}
