// Package engineruntime selects the schema runtime for one command invocation.
// The caller owns provider lifetime and supplies a frozen runtime before running
// the command tree. An explicit selection never falls back to bundled services.
package engineruntime

import (
	"context"
	"fmt"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
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

// Resolve returns the explicit command selection or the bundled default when
// no selection was made. An explicitly empty runtime remains empty.
func Resolve(ctx context.Context) (*engine.Runtime, error) {
	if runtime, selected := Selected(ctx); selected {
		return runtime, nil
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
