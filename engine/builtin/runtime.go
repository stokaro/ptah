package builtin

import (
	"context"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/engine"
	"ptah.run/internal/ydbextensions"
)

// New assembles a runtime with the bundled providers and their codecs. It returns a
// fresh registry on every call and installs no process-global handlers. The
// provider services create their own visitor for each batch, so the runtime
// can render concurrently without sharing output buffers.
func New() (*engine.Runtime, error) {
	aliases := make(map[string][]string)
	var names []string
	for _, spelling := range SupportedDialects() {
		name := platform.NormalizeDialect(spelling)
		if _, exists := aliases[name]; !exists {
			names = append(names, name)
			aliases[name] = nil
		}
		if spelling != name {
			aliases[name] = append(aliases[name], spelling)
		}
	}
	providers := make([]engine.Provider, 0, len(names))
	for _, name := range names {
		provider := engine.Provider{
			ID: "ptah.run/" + name,
			Targets: []engine.Target{{
				Name:      name,
				Aliases:   aliases[name],
				Rendering: renderingService{},
			}},
		}
		if name == platform.YDB {
			provider.Codecs = ydbextensions.Codecs()
		}
		providers = append(providers, provider)
	}
	return engine.New(providers...)
}

type renderingService struct{}

func (renderingService) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	if err := ctx.Err(); err != nil {
		return renderer.Result{}, err
	}
	visitor, err := NewRendererWithCapabilities(request.Target, request.Capabilities)
	if err != nil {
		return renderer.Result{}, err
	}
	sql, err := visitorRenderSQL(visitor, request.Nodes...)
	if err != nil {
		return renderer.Result{}, err
	}
	return renderer.Result{SQL: sql}, nil
}

// renderTarget preserves an unknown spelling in the diagnostic while resolving
// all built-in transport aliases at the composition boundary.
func renderTarget(dialect string) string {
	if normalized := platform.NormalizeDialect(dialect); normalized != "" {
		return normalized
	}
	return dialect
}
