// Package agenttarget carries the live databases an agent may be permitted to
// inspect.
//
// A target is operator-configured and immutable for the life of the process.
// The model never supplies one: it may name a target by the identity the
// operator gave it, and nothing else. That is the whole point of the package.
//
// Before this existed, read_database took a connection URL as a tool argument.
// The model chose the resource an authorization decision was exercised on,
// which means it also chose the answer: a policy that says "an unclassified
// database is denied" decides nothing if the caller can hand over any URL and
// have it classified by the same rule. Worse, the URL itself is authority --
// it carries the credential.
//
// So the URL lives here, and leaves only to open a connection. What the model
// and the audit record see is an identity and a class.
package agenttarget

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/agentpolicy"
)

// ErrUnknown reports a target identity nothing configured matches.
var ErrUnknown = errors.New("no such database target")

// ErrNoneConfigured reports that the process has no live database at all.
var ErrNoneConfigured = errors.New("no database target is configured")

// Target is one operator-configured live database.
//
// Every field is set at startup and never afterwards. There is no setter and no
// exported field: a value that a later caller could edit would be a value the
// approval it was bound to no longer describes.
type Target struct {
	id      string
	name    string
	url     string
	class   agentpolicy.DatabaseClass
	display string
}

// Config describes one target as the operator gave it.
type Config struct {
	// Name is the identity a caller may select by. It is not secret and it
	// carries no authority: two targets may not share one.
	Name string
	// URL is the connection string. It never leaves this package except to
	// open a connection.
	URL string
	// Class is the trust classification. It comes from the operator, never
	// from the URL: see [Classify].
	Class agentpolicy.DatabaseClass
}

// New builds a target, refusing a configuration that could not be authorized
// meaningfully.
func New(cfg Config) (*Target, error) {
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return nil, errors.New("a database target needs a name: it is what an approval is bound to")
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("database target %q needs a connection URL", name)
	}
	class := cfg.Class
	if class == "" {
		// Absence is not a permission. An unclassified target is denied by the
		// builtin table, which is the answer a configuration that said nothing
		// should get.
		class = agentpolicy.ClassUnclassified
	}
	if !validClass(class) {
		return nil, fmt.Errorf("database target %q has unknown class %q: want one of %s",
			name, class, strings.Join(classNames(), ", "))
	}
	return &Target{
		id:      identify(name, cfg.URL),
		name:    name,
		url:     cfg.URL,
		class:   class,
		display: display(cfg.URL),
	}, nil
}

// ID is the immutable identity an approval is bound to.
//
// It covers the name and the URL, so that repointing a name at a different
// database is a different target: an approval granted for the old one does not
// carry to the new one, which is the property a session grant needs.
func (t *Target) ID() string { return t.id }

// Name is the identity a caller selects by.
func (t *Target) Name() string { return t.name }

// Class is the trust classification the operator gave it.
func (t *Target) Class() agentpolicy.DatabaseClass { return t.class }

// Display is a sanitized description for a prompt or an audit record: driver,
// host and database name, and never a credential.
func (t *Target) Display() string { return t.display }

// URL is the connection string. It is for opening a connection and nothing
// else: it is not shown, logged, recorded or returned.
func (t *Target) URL() string { return t.url }

// Set is the collection of targets one process was configured with.
type Set struct {
	targets []*Target
	byName  map[string]*Target
}

// NewSet builds the collection, refusing duplicate names.
func NewSet(targets ...*Target) (*Set, error) {
	set := &Set{targets: targets, byName: make(map[string]*Target, len(targets))}
	for _, target := range targets {
		if _, clash := set.byName[target.Name()]; clash {
			return nil, fmt.Errorf("two database targets are named %q: a name is what an approval is bound to",
				target.Name())
		}
		set.byName[target.Name()] = target
	}
	return set, nil
}

// Len is how many targets are configured.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.targets)
}

// All lists the targets, in the order the operator configured them.
func (s *Set) All() []*Target {
	if s == nil {
		return nil
	}
	return append([]*Target(nil), s.targets...)
}

// Select resolves the target a caller named.
//
// An empty name selects the only target when exactly one is configured, which
// is the shape a single-database process has. It is an error when several are,
// because guessing which database an operation meant is the mistake this
// package exists to prevent.
func (s *Set) Select(name string) (*Target, error) {
	if s.Len() == 0 {
		return nil, ErrNoneConfigured
	}
	if name == "" {
		if len(s.targets) == 1 {
			return s.targets[0], nil
		}
		return nil, fmt.Errorf("name a database target: this process has %s",
			strings.Join(s.Names(), ", "))
	}
	target, known := s.byName[name]
	if !known {
		return nil, fmt.Errorf("%w: %q; this process has %s",
			ErrUnknown, name, strings.Join(s.Names(), ", "))
	}
	return target, nil
}

// Names lists the configured identities, sorted so a diagnostic reads the same
// way twice.
func (s *Set) Names() []string {
	names := make([]string, 0, s.Len())
	for _, target := range s.All() {
		names = append(names, target.Name())
	}
	sort.Strings(names)
	return names
}

// identify is the immutable identity of a name pointed at a URL.
func identify(name, connectionURL string) string {
	sum := sha256.Sum256([]byte(name + "\x00" + connectionURL))
	return "target:" + hex.EncodeToString(sum[:])[:16]
}

// display renders a URL without its credential.
//
// Nothing here parses a class out of the result. It is for a person reading an
// approval prompt and for an audit record, and a host name is a label somebody
// chose, not a fact about trust.
func display(connectionURL string) string {
	parsed, err := url.Parse(connectionURL)
	if err != nil || parsed.Scheme == "" {
		// Unparseable, so nothing can be safely shown from it. The name and the
		// class are what the prompt has, which is enough to decide with.
		return "(connection details withheld)"
	}
	host := parsed.Host
	if parsed.User != nil {
		host = parsed.User.Username() + "@" + host
	}
	path := strings.TrimPrefix(parsed.Path, "/")
	if path == "" {
		return fmt.Sprintf("%s://%s", parsed.Scheme, host)
	}
	return fmt.Sprintf("%s://%s/%s", parsed.Scheme, host, path)
}

// validClass reports whether a class is one Ptah knows.
func validClass(class agentpolicy.DatabaseClass) bool {
	return slices.Contains(agentpolicy.DatabaseClasses(), class)
}

// classNames spells the classes for a diagnostic.
func classNames() []string {
	names := make([]string, 0, len(agentpolicy.DatabaseClasses()))
	for _, class := range agentpolicy.DatabaseClasses() {
		names = append(names, string(class))
	}
	return names
}
