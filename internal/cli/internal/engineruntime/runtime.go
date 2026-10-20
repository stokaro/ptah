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
	if runtime, selected := ctx.Value(selectionKey{}).(*engine.Runtime); selected {
		return runtime, nil
	}
	return builtin.Bundled()
}
