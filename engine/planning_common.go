package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func (r *Runtime) snapshotCommonSteps(ctx context.Context, request featureplan.Request) ([]featureplan.CommonStep, error) {
	result := slices.Clone(request.CommonSteps)
	seen := make(map[plangraph.StepID]bool)
	for i, input := range request.CommonSteps {
		if seen[input.ID] {
			return nil, fmt.Errorf("%w: duplicate common planning step", schemaext.ErrInvalidValue)
		}
		seen[input.ID] = true
		// A single-step graph validates metadata only. Cross-step ordering and
		// ownership remain the complete host graph's responsibility.
		_, err := plangraph.Schedule(ctx, plangraph.Contribution[struct{}]{Owner: input.ID.Owner,
			Steps: []plangraph.Step[struct{}]{{ID: input.ID, Effects: input.Effects, Transaction: input.Transaction, Impact: input.Impact}},
		})
		if err != nil {
			return nil, fmt.Errorf("%w: invalid common planning step: %w", schemaext.ErrInvalidValue, err)
		}
		if input.Parent != (objectidentity.ID{}) && !slices.ContainsFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject == input.Parent }) {
			return nil, fmt.Errorf("%w: common planning step has no captured parent", schemaext.ErrInvalidValue)
		}
		step := input.Clone()
		if step.AddedColumn != nil {
			if err := validateAddedColumn(request, step); err != nil {
				return nil, err
			}
			step.AddedColumn.Facets, err = r.codecs.SnapshotFacets(ctx, schemaext.Desired, step.AddedColumn.Facets)
			if err != nil {
				return nil, err
			}
		}
		result[i] = step
	}
	return result, ctx.Err()
}

func validateAddedColumn(request featureplan.Request, step featureplan.CommonStep) error {
	column := step.AddedColumn
	if step.Parent.Kind != objectidentity.KindTable || column.Name == "" || column.Type == "" {
		return fmt.Errorf("%w: common column addition has an incomplete definition", schemaext.ErrInvalidValue)
	}
	subject := objectidentity.NewBuilder(request.Identifiers).ColumnParts(step.Parent.Schema.Source, step.Parent.Name.Source, column.Name)
	if !slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool {
		return effect.Action == plangraph.Create && effect.Subject.Key() == subject.Key()
	}) {
		return fmt.Errorf("%w: common column addition disagrees with its effects", schemaext.ErrInvalidValue)
	}
	return nil
}

func snapshotPlanningRewrites(request featureplan.Request, rewrites []plangraph.Rewrite, steps map[plangraph.StepID]bool) ([]plangraph.Rewrite, error) {
	common := make(map[plangraph.StepID]bool, len(request.CommonSteps))
	for _, step := range request.CommonSteps {
		common[step.ID] = true
		if steps[step.ID] {
			return nil, fmt.Errorf("%w: planning reused a common step identity", schemaext.ErrInvalidValue)
		}
	}
	result := slices.Clone(rewrites)
	sources, replacements := make(map[plangraph.StepID]bool), make(map[plangraph.StepID]bool)
	for i, rewrite := range rewrites {
		if len(rewrite.Sources) == 0 || !steps[rewrite.Replacement] || replacements[rewrite.Replacement] {
			return nil, fmt.Errorf("%w: missing or duplicate planning replacement", schemaext.ErrInvalidValue)
		}
		replacements[rewrite.Replacement] = true
		for _, source := range rewrite.Sources {
			if !common[source] || sources[source] || source.Owner == rewrite.Replacement.Owner {
				return nil, fmt.Errorf("%w: missing, duplicate, or misowned common rewrite source", schemaext.ErrInvalidValue)
			}
			sources[source] = true
		}
		result[i] = rewrite.Clone()
	}
	return result, nil
}
