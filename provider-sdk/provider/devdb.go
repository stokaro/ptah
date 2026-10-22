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

func devDatabaseEngine(description *protocol.Description, name string) error {
	if !slices.Contains(description.GetFeatures(), DevDatabaseFeature) {
		return fmt.Errorf("%w: dev database provisioning is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, name, nil)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), DevDatabaseFeature) {
		return fmt.Errorf("%w: engine dev database provisioning is unavailable", ErrProtocol)
	}
	return nil
}

func validDevText(text string, allowEmpty bool) bool {
	return (allowEmpty || text != "") && len(text) <= maxDevText && utf8.ValidString(text) &&
		!strings.ContainsFunc(text, unicode.IsControl)
}

// ValidateProvisionDevRequest checks the negotiated service and the operator spec.
func ValidateProvisionDevRequest(description *protocol.Description, request *protocol.ProvisionDevRequest) error {
	if request == nil {
		return fmt.Errorf("%w: dev database request is missing", ErrProtocol)
	}
	if err := devDatabaseEngine(description, request.Engine); err != nil {
		return err
	}
	if !validDevText(request.Spec, true) {
		return fmt.Errorf("%w: invalid dev database spec", ErrProtocol)
	}
	return nil
}

// ValidateDevDatabase refuses a reply that is neither a lease with an address nor
// a typed refusal, and a refusal with an unknown kind or no reason. The address
// must carry a scheme and no credentials: the host opens it, and a secret in a
// reply would travel into diagnostics.
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
	if err != nil || address.Scheme == "" || address.User != nil {
		return fmt.Errorf("%w: dev database address needs a scheme and no credentials", ErrProtocol)
	}
	return nil
}

// ValidateReleaseDevRequest checks the negotiated service and the lease.
func ValidateReleaseDevRequest(description *protocol.Description, request *protocol.ReleaseDevRequest) error {
	if request == nil {
		return fmt.Errorf("%w: dev database release is missing", ErrProtocol)
	}
	if err := devDatabaseEngine(description, request.Engine); err != nil {
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
