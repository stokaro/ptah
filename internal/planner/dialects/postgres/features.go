package postgres

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/pgeffects"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

// commonOwner is the contribution owner of the PostgreSQL family's own steps.
const commonOwner = "ptah.run/postgres"

// featurePositions records where the common sequence leaves room for feature
// operations: creation is after the tables and columns a feature object may
// read and before the views that may read it, and removal is after those views
// are dropped and before the tables. Both are indexes into the common node
// list.
type featurePositions struct {
	creation, removal int
}

// scheduleFeatures dispatches a diff's feature changes to their owners and
// joins the contributed operations to the common sequence. A diff with no
// feature change is returned as it was planned, without asking the runtime.
//
// Owners place their steps through dependencies on common steps they can
// identify from effects. Every other step keeps the common order, and each
// feature step joins one of two windows: one that drops an object goes after
// the planner drops the views that may read it, and every other one goes
// before it creates them. Scheduling validates the complete graph before
// any node is returned, so a late conflict leaves no successful prefix.
func (p *Planner) scheduleFeatures(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, positions featurePositions) ([]ast.Node, error) {
	target := p.targetDialect()
	if diff == nil || !hasFeatureChanges(target, diff) {
		return nodes, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(target)
	request := featureplan.Request{Target: target, Identifiers: semantics, Capabilities: p.capabilities(), Changes: slices.Clone(diff.FeatureChanges)}
	names := make(map[objectidentity.Key]string)
	builder := objectidentity.NewBuilder(semantics)
	for _, table := range diff.TablesModified {
		// On CockroachDB the settings of surviving tables are planned where
		// row-level TTL needs them; see [Planner.planTableFeatures].
		if len(table.FeatureChanges) == 0 || target == platform.CockroachDB {
			continue
		}
		subject := builder.Table(table.TableName)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, table.FeatureChanges...)
		names[subject.Key()] = table.TableName
	}
	graph := newCommonGraph(semantics, nodes, positions)
	request.CommonSteps = graph.steps
	features, err := featurehost.Plan(ctx, runtime, request, names)
	if err != nil {
		return nil, err
	}
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			window := graph.creation
			if slices.ContainsFunc(step.Effects, func(effect plangraph.Effect) bool { return effect.Action == plangraph.Drop }) {
				window = graph.removal
			}
			graph.contribution.Dependencies = append(graph.contribution.Dependencies,
				plangraph.Dependency{Before: window[0], After: step.ID}, plangraph.Dependency{Before: step.ID, After: window[1]})
		}
	}
	plan, err := plangraph.ScheduleRewritten(ctx, graph.contribution, features.Rewrites, features.Contributions...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	var scheduled []ast.Node
	for _, step := range plan.Steps {
		scheduled = append(scheduled, step.Payload...)
	}
	return scheduled, ctx.Err()
}

func hasFeatureChanges(target string, diff *difftypes.SchemaDiff) bool {
	return len(diff.FeatureChanges) > 0 || (target != platform.CockroachDB && slices.ContainsFunc(diff.TablesModified, func(table difftypes.TableDiff) bool {
		return len(table.FeatureChanges) > 0
	}))
}

// commonGraph is the common sequence as graph units, with an empty marker pair
// around each feature window.
type commonGraph struct {
	contribution      plangraph.Contribution[[]ast.Node]
	steps             []featureplan.CommonStep
	creation, removal [2]plangraph.StepID
}

func newCommonGraph(semantics identifier.Semantics, nodes []ast.Node, positions featurePositions) commonGraph {
	graph := commonGraph{contribution: plangraph.Contribution[[]ast.Node]{Owner: commonOwner}}
	marker := func(name string) plangraph.StepID {
		id := plangraph.StepID{Owner: commonOwner, Name: name}
		graph.append(plangraph.Step[[]ast.Node]{ID: id})
		return id
	}
	effects := pgeffects.Sequence(objectidentity.NewBuilder(semantics), nodes)
	for i := 0; i <= len(nodes); i++ {
		if i == positions.creation {
			graph.creation = [2]plangraph.StepID{marker("common/feature-creations"), marker("common/after-feature-creations")}
		}
		if i == positions.removal {
			graph.removal = [2]plangraph.StepID{marker("common/feature-removals"), marker("common/after-feature-removals")}
		}
		if i == len(nodes) {
			break
		}
		graph.append(plangraph.Step[[]ast.Node]{ID: plangraph.StepID{Owner: commonOwner, Name: fmt.Sprintf("common/%06d", i)},
			Payload: []ast.Node{nodes[i]}, Effects: effects[i]})
	}
	return graph
}

func (g *commonGraph) append(step plangraph.Step[[]ast.Node]) {
	if count := len(g.contribution.Steps); count > 0 {
		g.contribution.Dependencies = append(g.contribution.Dependencies, plangraph.Dependency{Before: g.contribution.Steps[count-1].ID, After: step.ID})
	}
	g.contribution.Steps = append(g.contribution.Steps, step)
	g.steps = append(g.steps, featureplan.CommonStep{ID: step.ID, Effects: slices.Clone(step.Effects)})
}

// planTableFeatures lowers the owner-planned changes of surviving CockroachDB
// tables, and accounts for the owned state of every removed table that carries
// some, on every target of the family. Other targets join the settings of
// surviving tables to the plan graph in [Planner.scheduleFeatures]. The
// operations are emitted at the caller's position in the plan, which is after
// the columns an owned setting may refer to exist and before any column is
// dropped. CockroachDB row-level TTL is the setting that needs it: its
// expression may name a column the same plan adds, and the column it currently
// names cannot be dropped while the policy refers to it (stokaro/ptah#1027).
//
// The common phases are not a graph here, so an owner's ordering against them
// is the position itself; an owner that asks to rewrite a common step is
// refused rather than scheduled as if it had not asked.
func (p *Planner) planTableFeatures(ctx context.Context, runtime featureplan.Runtime, result []ast.Node, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	target := p.targetDialect()
	request := featureplan.Request{
		Target: target, Identifiers: diff.EffectiveIdentifierSemantics(target), Capabilities: p.capabilities(),
	}
	names := make(map[objectidentity.Key]string)
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, table := range diff.TablesModified {
		if len(table.FeatureChanges) == 0 || target != platform.CockroachDB {
			continue
		}
		subject := builder.Table(table.TableName)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, table.FeatureChanges...)
		names[subject.Key()] = table.TableName
	}
	// A removed table is the runtime's business only when it carries owned
	// state: a table without any has nothing for an owner to account for, and
	// asking would make every table drop depend on a parent planning service
	// the target may not register.
	for _, removal := range diff.TablesRemoved {
		table := featureplan.Table{Subject: builder.Table(removal.Name), Action: featureplan.DropTable, Current: removal.Current}
		kinds, err := table.CapturedKinds()
		if err != nil {
			return nil, err
		}
		if len(kinds) == 0 {
			continue
		}
		request.Tables = append(request.Tables, table)
		names[table.Subject.Key()] = removal.Name
	}
	if len(request.Tables) == 0 {
		return result, nil
	}
	features, err := featurehost.Plan(ctx, runtime, request, names)
	if err != nil {
		return nil, err
	}
	if len(features.Rewrites) != 0 {
		return nil, fmt.Errorf("%w: %s planning has no common steps for a feature to rewrite", schemaext.ErrInvalidValue, target)
	}
	plan, err := plangraph.Schedule(ctx, features.Contributions...)
	if err != nil {
		return nil, err
	}
	for _, step := range plan.Steps {
		result = append(result, step.Payload...)
	}
	return result, nil
}
