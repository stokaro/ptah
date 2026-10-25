package providerselect

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
)

// ErrClosed is returned when a [Lazy] is asked to start a provider after Close.
var ErrClosed = errors.New("the provider session is closed")

// Lazy starts a provider the first time a connection needs one. A command that
// decides its engine late, from a URL it resolved after reading project
// configuration, installs one Lazy on its context and closes it when it
// returns.
//
// The default selection is additive. A URL scheme a provider serves is routed to
// that provider; every other scheme keeps its built-in driver. A provider is
// started at most once per Lazy, and a start failure is remembered, so a second
// connection to the same scheme fails the same way without another attempt.
//
// Lazy implements [dbschema.NativeLookup] for connections and
// [devdocker.External] for docker dev URLs. The methods are safe for concurrent
// use.
type Lazy struct {
	config     providerconfig.Configuration
	configured bool
	resolver   *providerconfig.Resolver

	mu      sync.Mutex
	closed  bool
	answers map[string]answer
	running map[string]*Started
	order   []string
}

// answer is what selection decided for one scheme, kept so the decision and
// any failure are not repeated.
type answer struct {
	registration dbschema.NativeRegistration
	found        bool
	err          error
}

// NewLazy returns a Lazy over config. configured reports that config came from
// a manifest the operator named. A nil resolver selects
// [providerconfig.DefaultResolver].
func NewLazy(config providerconfig.Configuration, configured bool, resolver *providerconfig.Resolver) *Lazy {
	return &Lazy{config: config, configured: configured, resolver: resolver, answers: make(map[string]answer), running: make(map[string]*Started)}
}

type lazyKey struct{}

// With returns a context under which connections and docker dev URLs consult
// the Lazy, and from which [FromContext] returns it.
func (l *Lazy) With(ctx context.Context) context.Context {
	ctx = dbschema.WithNativeLookup(ctx, l)
	ctx = devdocker.WithExternal(ctx, l)
	return context.WithValue(ctx, lazyKey{}, l)
}

// FromContext returns the Lazy installed by [Lazy.With], or nil.
func FromContext(ctx context.Context) *Lazy {
	if ctx == nil {
		return nil
	}
	lazy, _ := ctx.Value(lazyKey{}).(*Lazy)
	return lazy
}

// Serves reports whether a provider, and not a built-in engine, would serve the
// URL scheme. It reads the manifest and the catalog only: it starts nothing and
// opens no executable, so a command can use it to decide whether a scheme is
// known before it connects.
func (l *Lazy) Serves(scheme string) bool {
	if l == nil {
		return false
	}
	return l.selects([]Target{{Flag: FlagDatabaseURL, Value: strings.TrimSpace(scheme) + ":"}})
}

// selects reports whether a provider serves the engine the targets name. A
// target that names no engine, a built-in engine, or a name nothing claims is
// left to the built-in code and to the caller's own error for an unsupported
// engine.
func (l *Lazy) selects(targets []Target) bool {
	selector := Selector(targets)
	if selector == "" || servedBuiltIn(l.config, selector, false) {
		return false
	}
	_, official := providercatalog.Lookup(selector)
	return official || claimed(l.config, selector)
}

// NativeSchemes implements [dbschema.NativeSchemeLister]. It lists the URL
// schemes a provider serves under this configuration with their canonical
// targets, without starting any provider.
func (l *Lazy) NativeSchemes() map[string]string {
	schemes := make(map[string]string)
	for _, installation := range l.config.Providers {
		maps.Copy(schemes, installation.Claims())
	}
	for _, entry := range providercatalog.Entries() {
		if servedBuiltIn(l.config, entry.EngineID, false) {
			continue
		}
		for _, scheme := range entry.URLSchemes {
			schemes[scheme] = entry.EngineID
		}
	}
	return schemes
}

// NativeRegistration implements [dbschema.NativeLookup]. It starts the provider
// that serves scheme on first use and returns its binding, or reports that no
// provider serves the scheme.
func (l *Lazy) NativeRegistration(ctx context.Context, scheme string) (dbschema.NativeRegistration, bool, error) {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	if !l.Serves(scheme) {
		return dbschema.NativeRegistration{}, false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if known, cached := l.answers[scheme]; cached {
		return known.registration, known.found, known.err
	}
	if l.closed {
		return dbschema.NativeRegistration{}, false, ErrClosed
	}
	result := l.resolve(ctx, scheme)
	l.answers[scheme] = result
	return result.registration, result.found, result.err
}

// resolve decides and, when a provider serves scheme, starts it. The caller
// holds the lock.
func (l *Lazy) resolve(ctx context.Context, scheme string) answer {
	target := Target{Flag: FlagDatabaseURL, Value: scheme + ":"}
	choice, err := Choose(Request{Config: l.config, Configured: l.configured, Resolver: l.resolver, Targets: []Target{target}})
	if err != nil || choice == nil {
		return answer{err: err}
	}
	started, err := l.startLocked(ctx, choice)
	if err != nil {
		return answer{err: err}
	}
	registration, found := started.Connections[scheme]
	return answer{registration: registration, found: found}
}

// startLocked starts the provider of choice once. The caller holds the lock.
func (l *Lazy) startLocked(ctx context.Context, choice *Choice) (*Started, error) {
	if started, running := l.running[choice.Target]; running {
		return started, nil
	}
	// The provider outlives the connection that asked for it: it is closed with
	// the command, not with the connect timeout the first caller carried.
	started, err := choice.Start(context.WithoutCancel(ctx), Additive)
	if err != nil {
		return nil, err
	}
	l.running[choice.Target] = started
	l.order = append(l.order, choice.Target)
	return started, nil
}

// DevEngines implements [devdocker.External]. It lists the engine names a
// provider serves by this Lazy's configuration, without starting any.
func (l *Lazy) DevEngines() []string {
	var names []string
	for _, installation := range l.config.Providers {
		for name := range installation.Claims() {
			names = append(names, name)
		}
	}
	for _, entry := range providercatalog.Entries() {
		if !servedBuiltIn(l.config, entry.EngineID, false) {
			names = append(names, entry.EngineID)
			names = append(names, entry.Aliases...)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// ProvisionDev implements [devdocker.External]. It starts the provider of the
// engine rawURL names and asks it for a dev database.
func (l *Lazy) ProvisionDev(ctx context.Context, rawURL string) (string, func(), error) {
	engineName, found := dockerEngine(FlagDevURL, strings.TrimSpace(rawURL))
	if !found {
		return "", func() {}, fmt.Errorf("the dev URL names no engine")
	}
	l.mu.Lock()
	started, err := l.devOwnerLocked(ctx, rawURL)
	l.mu.Unlock()
	if err != nil {
		return "", func() {}, err
	}
	if started.Dev == nil {
		return "", func() {}, fmt.Errorf("the provider of %s does not provision dev databases", engineName)
	}
	return started.Dev.ProvisionDev(ctx, rawURL)
}

// RealmSchemes implements [devdocker.ExternalRealms]. It lists the URL
// schemes of the engines a provider serves by this Lazy's configuration,
// without starting any: whether an engine creates dev realms is known only
// once its provider runs, and [Lazy.EnterRealm] asks it then.
func (l *Lazy) RealmSchemes() []string {
	var schemes []string
	for _, installation := range l.config.Providers {
		for name := range installation.Claims() {
			schemes = append(schemes, strings.ToLower(name))
		}
	}
	slices.Sort(schemes)
	return slices.Compact(schemes)
}

// EnterRealm implements [devdocker.ExternalRealms]. It starts the provider
// that serves rawURL and asks it for a dev realm in the database the URL
// names. A provider whose engine creates no realm leaves the URL as it is,
// and its database is the dev database.
func (l *Lazy) EnterRealm(ctx context.Context, rawURL string) (string, func(), error) {
	l.mu.Lock()
	started, err := l.devOwnerLocked(ctx, rawURL)
	l.mu.Unlock()
	if err != nil {
		return "", func() {}, err
	}
	realms, owns := started.Dev.(devdocker.ExternalRealms)
	scheme, _, _ := strings.Cut(strings.TrimSpace(rawURL), ":")
	if !owns || !slices.Contains(realms.RealmSchemes(), strings.ToLower(scheme)) {
		return rawURL, func() {}, nil
	}
	return realms.EnterRealm(ctx, rawURL)
}

func (l *Lazy) devOwnerLocked(ctx context.Context, rawURL string) (*Started, error) {
	if l.closed {
		return nil, ErrClosed
	}
	choice, err := Choose(Request{
		Config: l.config, Configured: l.configured, Resolver: l.resolver,
		Targets: []Target{{Flag: FlagDevURL, Value: strings.TrimSpace(rawURL)}},
	})
	if err != nil {
		return nil, err
	}
	if choice == nil {
		return nil, fmt.Errorf("no provider serves the dev URL")
	}
	return l.startLocked(ctx, choice)
}

// Runtime returns the built-in runtime extended with every provider this Lazy
// has started, after starting the providers the targets name. A command that
// builds its planning runtime before it connects names its URLs here, so the
// provider is already running when the runtime is built. While no provider has
// started it is the shared [builtin.Bundled] runtime.
func (l *Lazy) Runtime(ctx context.Context, targets ...Target) (*engine.Runtime, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, target := range targets {
		if strings.TrimSpace(target.Value) == "" {
			continue
		}
		if !l.selects([]Target{target}) {
			continue
		}
		if l.closed {
			return nil, ErrClosed
		}
		choice, err := Choose(Request{Config: l.config, Configured: l.configured, Resolver: l.resolver, Targets: []Target{target}})
		if err != nil {
			return nil, err
		}
		if choice == nil {
			continue
		}
		if _, err := l.startLocked(ctx, choice); err != nil {
			return nil, err
		}
	}
	if len(l.order) == 0 {
		return builtin.Bundled()
	}
	providers := make([]engine.Provider, 0, len(l.order))
	for _, name := range l.order {
		providers = append(providers, l.running[name].provider)
	}
	return builtin.New(providers...)
}

// Close stops every provider this Lazy started. It is safe to call more than
// once, and a Lazy starts nothing after it.
func (l *Lazy) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	for _, name := range l.order {
		l.running[name].Close()
	}
	l.order = nil
	clear(l.running)
}
