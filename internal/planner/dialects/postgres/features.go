package postgres

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

// unownedFeatureChanges is the part of a diff no owner on this target plans.
// CockroachDB's table settings are planned by their owner through the runtime;
// every other feature change, and every change on another target, is refused
// before the first statement.
func (p *Planner) unownedFeatureChanges(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	if diff == nil || p.targetDialect() != platform.CockroachDB {
		return diff
	}
	return &difftypes.SchemaDiff{FeatureChanges: diff.FeatureChanges}
}

// planTableFeatures lowers the owner-planned changes of surviving tables, and
// on CockroachDB accounts for the owned state of every removed table. The
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
		if len(table.FeatureChanges) == 0 {
			continue
		}
		subject := builder.Table(table.TableName)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, table.FeatureChanges...)
		names[subject.Key()] = table.TableName
	}
	if target == platform.CockroachDB {
		for _, removal := range diff.TablesRemoved {
			subject := builder.Table(removal.Name)
			request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Action: featureplan.DropTable, Current: removal.Current})
			names[subject.Key()] = removal.Name
		}
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
