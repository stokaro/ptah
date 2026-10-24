// Package providerselect decides which executable provider, if any, serves an
// invocation, starts it, and builds the runtime, connection bindings and
// dev-database owner the invocation runs under.
//
// It sits below the command layer: the native commands in
// internal/cli/internal/providercommand call it with the flags they read, and
// the compatibility surface calls it with the URLs it resolves late. Both get
// the same selection order (see [providerconfig.Resolver.Select]), the same
// verification before every start, and the same process lifetime rules.
//
// Selection is data until [Choice.Start] runs: nothing here executes a binary
// because a name, a URL scheme or a catalog entry matched. A manifest entry or
// a pinned official artifact authorizes bytes, and those bytes are hashed before
// each start.
package providerselect

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
)

// Target is one explicit connection value of an invocation: the flag that
// carried it and its text. The flags that select an engine are db-url, dialect
// and dev-url.
type Target struct {
	Flag  string
	Value string
}

// Request is everything selection reads. The zero value selects nothing.
type Request struct {
	// Config is the manifest the operator named. It is the zero value when none
	// was named.
	Config providerconfig.Configuration
	// Configured reports that Config came from an explicitly named manifest.
	Configured bool
	// Engine is the engine the operator asked for, lower-cased by the caller or
	// not; Explicit reports whether one was asked for at all.
	Engine   string
	Explicit bool
	// Targets are the connection values the invocation names.
	Targets []Target
	// Resolver locates bundled and stored official providers. Nil selects
	// [providerconfig.DefaultResolver].
	Resolver *providerconfig.Resolver
}

// Choice is the installation that serves an invocation and the engine it
// serves. Choosing starts nothing.
type Choice struct {
	Installation providerconfig.Installation
	Target       string
}

// Choose decides whether a provider serves the request. It returns nil, nil
// when no provider does and the built-in engines keep the invocation.
//
// An engine with a built-in implementation stays built in unless the operator
// asked for a provider: an explicit engine, or a manifest entry that claims the
// engine. A name no catalog entry knows and no manifest claims is left to the
// caller's own error for an unsupported dialect.
func Choose(request Request) (*Choice, error) {
	selector := strings.ToLower(strings.TrimSpace(request.Engine))
	if !request.Explicit {
		selector = Selector(request.Targets)
	}
	if selector == "" && request.Configured {
		if len(request.Config.Providers) != 1 || len(request.Config.Providers[0].Engines) != 1 {
			return nil, fmt.Errorf("--provider-engine is required when the command does not identify one configured provider engine")
		}
		selector = request.Config.Providers[0].Engines[0].ID
	}
	if selector == "" || servedBuiltIn(request.Config, selector, request.Explicit) {
		return nil, nil
	}
	resolver := providerconfig.DefaultResolver()
	if request.Resolver != nil {
		resolver = *request.Resolver
	}
	installation, target, err := resolver.Select(request.Config, selector)
	if err != nil {
		if !request.Configured && !request.Explicit {
			if _, known := providercatalog.Lookup(selector); !known {
				return nil, nil
			}
		}
		return nil, err
	}
	if err := validateTargets(request.Targets, installation, target); err != nil {
		return nil, err
	}
	return &Choice{Installation: installation, Target: target}, nil
}

// servedBuiltIn reports whether an engine with a built-in implementation keeps
// that implementation for this invocation. Only an explicit engine, or a
// manifest entry that claims the engine, asks for a provider instead; the
// bundled and stored candidates of the selection order are not consulted for an
// engine that is still built in.
//
// This is the one place the default for an official engine is decided. Serving
// an engine from its provider by default means returning false here for it.
func servedBuiltIn(config providerconfig.Configuration, selector string, explicit bool) bool {
	return !explicit && platform.NormalizeDialect(selector) != "" && !claimed(config, selector)
}

func claimed(config providerconfig.Configuration, selector string) bool {
	if entry, found := providercatalog.Lookup(selector); found {
		selector = entry.EngineID
	}
	for _, installation := range config.Providers {
		if _, found := installation.Claims()[selector]; found {
			return true
		}
	}
	return false
}

// Mode says how a started provider combines with the built-in engines.
type Mode int

const (
	// Exclusive gives the invocation the provider's targets and nothing else.
	// A URL scheme the provider does not register is refused, including the
	// schemes of bundled drivers. An operator who names a provider explicitly
	// asked for exactly that provider.
	Exclusive Mode = iota
	// Additive adds the provider's targets to the built-in runtime and leaves
	// the built-in drivers reachable. A registration that collides with a
	// built-in target is an error, never a precedence rule.
	Additive
)

// Started is a running provider and what an invocation needs from it. Close
// ends the provider process; it is safe to call more than once.
type Started struct {
	// Runtime is the engine runtime the invocation plans and renders with.
	Runtime *engine.Runtime
	// Connections binds the provider's URL schemes to their connectors.
	Connections map[string]dbschema.NativeRegistration
	// Dev provisions dev databases for the provider's engines. Nil when the
	// provider declares none.
	Dev devdocker.External

	host     *providerhost.Host
	provider engine.Provider
}

// Close stops the provider process.
func (s *Started) Close() { s.host.Close() }

// Start verifies the chosen installation, starts it, and builds the runtime for
// mode. Every failure closes any process it started.
func (c *Choice) Start(ctx context.Context, mode Mode) (*Started, error) {
	host, err := providerconfig.Start(ctx, c.Installation, c.Target)
	if err != nil {
		return nil, err
	}
	var runtime *engine.Runtime
	if mode == Additive {
		runtime, err = builtin.New(host.Provider())
	} else {
		runtime, err = engine.New(host.Provider())
	}
	if err != nil {
		host.Close()
		return nil, err
	}
	started := &Started{Runtime: runtime, Connections: host.Connections(), host: host, provider: host.Provider()}
	if owner := host.DevDatabases(); owner != nil {
		started.Dev = owner
	}
	return started, nil
}
