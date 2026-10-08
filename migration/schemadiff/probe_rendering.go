package schemadiff

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/renderer"
)

// renderProbe renders the complete probe, including its cleanup or fallback
// statements, before database work begins. A completed declaration refusal or
// omitted output leaves normalization unresolved. An unavailable service,
// cancellation, or malformed reply must abort comparison instead.
func renderProbe(ctx context.Context, service renderer.Service, info catalog.ServerInfo, nodes ...ast.Node) (renderer.Result, bool, error) {
	result, err := renderer.Render(ctx, service, renderer.Request{
		Target: info.Dialect, Capabilities: info.Capabilities, Nodes: nodes,
	})
	if err != nil {
		if _, refused := errors.AsType[*renderer.BatchRefusalError](err); refused {
			return renderer.Result{}, false, nil
		}
		return renderer.Result{}, false, fmt.Errorf("render comparison probe: %w", err)
	}
	if len(result.Omissions) != 0 {
		return renderer.Result{}, false, nil
	}
	for _, fragment := range result.Fragments {
		if strings.TrimSpace(fragment) == "" {
			return renderer.Result{}, false, nil
		}
	}
	return result, true, nil
}
