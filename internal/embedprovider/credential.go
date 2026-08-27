package embedprovider

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// CredentialRef names where a credential lives, and never what it is.
//
// The epic's rule is that keys must not appear in project configuration, process
// arguments, logs, state tables, reports, MCP resources, Assist messages,
// session exports or OCI artifacts. A reference satisfies all nine at once
// because the value never enters the configuration in the first place: it is
// read at the moment of use and not kept.
//
// The zero value is a valid reference meaning "no credential", which is what a
// local endpoint usually needs.
type CredentialRef struct {
	// Scheme is where to look: "env" or "file".
	Scheme string
	// Locator is the variable name or path.
	Locator string
}

// Errors a caller distinguishes when resolving one.
var (
	// ErrCredentialUnset is a reference whose source holds nothing.
	ErrCredentialUnset = errors.New("credential reference resolves to nothing")
	// ErrCredentialScheme is a scheme this build does not support.
	ErrCredentialScheme = errors.New("unsupported credential reference scheme")
)

// ParseCredentialRef reads a reference in `scheme:locator` form.
//
// An empty string is the absent credential, which a local endpoint may want and
// a hosted one will fail on later with the endpoint's own answer rather than
// with a guess made here.
func ParseCredentialRef(raw string) (CredentialRef, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return CredentialRef{}, nil
	}
	scheme, locator, found := strings.Cut(trimmed, ":")
	if !found || strings.TrimSpace(locator) == "" {
		return CredentialRef{}, fmt.Errorf(
			"%w: %q is not scheme:locator, such as env:NAME or file:/path", ErrCredentialScheme, raw)
	}
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	switch scheme {
	case "env", "file":
		return CredentialRef{Scheme: scheme, Locator: strings.TrimSpace(locator)}, nil
	default:
		return CredentialRef{}, fmt.Errorf(
			"%w: %q, and this build resolves env and file", ErrCredentialScheme, scheme)
	}
}

// Set reports whether a credential was configured at all.
func (r CredentialRef) Set() bool {
	return r.Scheme != ""
}

// String renders the reference for display and for the run's record.
//
// It is the LOCATOR, which is the point: a report saying `env:PTAH_EMBED_TOKEN`
// tells an operator where to look without telling a reader what the token is.
func (r CredentialRef) String() string {
	if !r.Set() {
		return ""
	}
	return r.Scheme + ":" + r.Locator
}

// Resolve reads the credential.
//
// The value is returned and not retained. Nothing in this package stores it,
// and a caller that keeps it beyond the request it authorizes has undone the
// reason references exist.
func (r CredentialRef) Resolve() (string, error) {
	if !r.Set() {
		return "", nil
	}
	switch r.Scheme {
	case "env":
		value := os.Getenv(r.Locator)
		if strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("%w: environment variable %s is unset or empty", ErrCredentialUnset, r.Locator)
		}
		return value, nil
	case "file":
		return r.resolveFile()
	default:
		return "", fmt.Errorf("%w: %q", ErrCredentialScheme, r.Scheme)
	}
}

// resolveFile reads a credential file, refusing one the filesystem lets others
// read where the filesystem can be asked.
//
// A token in a world-readable file is a token every process on the host has,
// and reading it anyway would make Ptah the reason it leaked. Whether that can
// be established at all is a property of the platform, which is why the refusal
// lives behind FilePermissionsEnforced rather than in this function.
func (r CredentialRef) resolveFile() (string, error) {
	info, err := os.Stat(r.Locator)
	if err != nil {
		return "", fmt.Errorf("read credential file %s: %w", r.Locator, err)
	}
	if err := refuseReadableBeyondOwner(r.Locator, info); err != nil {
		return "", err
	}
	body, err := os.ReadFile(r.Locator) //gosec:disable G304 -- the operator named this file as the credential source
	if err != nil {
		return "", fmt.Errorf("read credential file %s: %w", r.Locator, err)
	}
	value := strings.TrimSpace(string(body))
	if value == "" {
		return "", fmt.Errorf("%w: credential file %s is empty", ErrCredentialUnset, r.Locator)
	}
	return value, nil
}
