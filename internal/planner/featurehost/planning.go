// Package featurehost lowers selected feature plans into contributions to a
// host's common operation graph. Target-specific phases remain with the host.
package featurehost

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/featureops"
)

// Result retains lowered contributions and explicit common-step rewrites.
// The caller must apply the rewrites while scheduling the complete graph.
// Phases holds the phase of every contributed step whose owner asked for one
// other than [featureplan.PhaseDefault].
type Result struct {
	Contributions []plangraph.Contribution[[]ast.Node]
	Rewrites      []plangraph.Rewrite
	Phases        map[plangraph.StepID]featureplan.Phase
}

// Plan dispatches one contextual batch and converts complete operation replies
// into AST contributions without scheduling them independently. Names binds
// captured table identities, and the materialized views whose attached
// settings the request changes, to the host's source-spelled emission names.
// A parent without a binding cannot receive an ALTER operation. The caller must
// join these contributions to its common graph before returning any nodes.
//
// Accepted names the phases other than [featureplan.PhaseDefault] the caller
// has a window for. A step whose operation asks for any other phase is
// refused: placing it in the default window would reorder it silently.
func Plan(ctx context.Context, runtime featureplan.Runtime, request featureplan.Request, names map[objectidentity.Key]string, accepted ...featureplan.Phase) (Result, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return Result{}, err
	}
	names = maps.Clone(names)
	if err := validateNames(request, names); err != nil {
		return Result{}, err
	}
	result, err := runtime.PlanFeatures(ctx, request)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := result.Err(request); err != nil {
		return Result{}, err
	}
	contributions := make([]plangraph.Contribution[[]ast.Node], len(result.Contributions))
	phases := make(map[plangraph.StepID]featureplan.Phase)
	for i, feature := range result.Contributions {
		contribution := plangraph.Contribution[[]ast.Node]{Owner: feature.Owner, Dependencies: slices.Clone(feature.Dependencies)}
		for _, step := range feature.Steps {
			nodes, err := featureops.Nodes(step.Payload, names)
			if err != nil {
				return Result{}, err
			}
			contribution.Steps = append(contribution.Steps, plangraph.Step[[]ast.Node]{
				ID: step.ID, Payload: nodes, Effects: slices.Clone(step.Effects), Transaction: step.Transaction, Impact: step.Impact,
				Placement: step.Placement,
			})
			if step.Payload.Phase != featureplan.PhaseDefault {
				if !slices.Contains(accepted, step.Payload.Phase) {
					return Result{}, fmt.Errorf("%w: feature step %s/%s asks for the %q phase, which this host has no window for",
						schemaext.ErrInvalidValue, step.ID.Owner, step.ID.Name, step.Payload.Phase)
				}
				phases[step.ID] = step.Payload.Phase
			}
		}
		contributions[i] = contribution
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	rewrites := make([]plangraph.Rewrite, len(result.Rewrites))
	for i, rewrite := range result.Rewrites {
		rewrites[i] = rewrite.Clone()
	}
	return Result{Contributions: contributions, Rewrites: rewrites, Phases: phases}, nil
}

func validateNames(request featureplan.Request, names map[objectidentity.Key]string) error {
	captured := make(map[objectidentity.Key]bool, len(request.Tables))
	for _, table := range request.Tables {
		captured[table.Subject.Key()] = true
	}
	views := make(map[objectidentity.Key]bool)
	for _, change := range request.Changes {
		if change.Subject.Kind == objectidentity.KindMatView {
			views[change.Subject.Key()] = true
		}
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for key, name := range names {
		table := captured[key] && builder.Table(name).Key() == key
		view := views[key] && builder.SchemaScoped(objectidentity.KindMatView, name).Key() == key
		if name == "" || !table && !view {
			return fmt.Errorf("%w: feature table name disagrees with its captured identity", schemaext.ErrInvalidValue)
		}
	}
	return nil
}
