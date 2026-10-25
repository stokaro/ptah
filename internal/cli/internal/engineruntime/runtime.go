// Package engineruntime selects the schema runtime for one command invocation.
// The caller owns provider lifetime and supplies a frozen runtime before running
// the command tree. An explicit selection never falls back to bundled services.
package engineruntime

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/providerselect"
)

type selectionKey struct{}

// With selects a runtime for this command context and its derived contexts.
// The runtime is immutable; this function starts no provider and owns no handle.
func With(ctx context.Context, runtime *engine.Runtime) (context.Context, error) {
	if runtime == nil {
		return nil, fmt.Errorf("command runtime selection is nil")
	}
	return context.WithValue(ctx, selectionKey{}, runtime), nil
}

// Resolve returns the runtime the invocation plans, renders and loads schemas
// with. An explicit selection is returned as it is, and an explicitly empty
// runtime remains empty. Otherwise it is the bundled runtime, extended with the
// provider of every engine the targets name when the command runs under a
// provider session (see [providerselect.Lazy]); the session starts each such
// provider once, and a target that names a built-in engine, a file or nothing
// starts none.
//
// The runtime is frozen when it is built, so a command names here every URL and
// dialect it will plan or render for: a provider started later, by a
// connection, is not part of a runtime resolved before it.
func Resolve(ctx context.Context, targets ...providerselect.Target) (*engine.Runtime, error) {
	if runtime, selected := Selected(ctx); selected {
		return runtime, nil
	}
	if session := providerselect.FromContext(ctx); session != nil {
		return session.Runtime(ctx, targets...)
	}
	return builtin.Bundled()
}

// Selected returns the explicit command selection, and false when none was
// made and [Resolve] returns the bundled default. A command whose own reading
// of a dialect is built in asks here, because a selected provider owns its
// engines and the built-in reading must not stand in for it.
func Selected(ctx context.Context) (*engine.Runtime, bool) {
	runtime, selected := ctx.Value(selectionKey{}).(*engine.Runtime)
	return runtime, selected
}

// DatabaseURL names a URL the command connects to as a target of [Resolve].
func DatabaseURL(value string) providerselect.Target {
	return providerselect.Target{Flag: providerselect.FlagDatabaseURL, Value: value}
}

// DevURL names a dev-database URL as a target of [Resolve]. A docker dev URL
// names its engine, which a database URL does not.
func DevURL(value string) providerselect.Target {
	return providerselect.Target{Flag: providerselect.FlagDevURL, Value: value}
}

// Dialect names a dialect as a target of [Resolve].
func Dialect(value string) providerselect.Target {
	return providerselect.Target{Flag: providerselect.FlagDialect, Value: value}
}

// TargetName returns the target a dialect name selects: the dialect a built-in
// name normalizes to, or the engine runtime resolves the name to, which is how
// an engine an executable provider serves is named. It is "" for a name
// neither knows.
func TargetName(runtime *engine.Runtime, name string) string {
	name = strings.TrimSpace(name)
	if normalized := platform.NormalizeDialect(name); normalized != "" || name == "" {
		return normalized
	}
	selected, err := runtime.ResolveTarget(name)
	if err != nil {
		return ""
	}
	return selected.Name()
}
