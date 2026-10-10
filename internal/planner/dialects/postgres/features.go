package postgres

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

// unownedFeatureChanges is the part of a diff this planner has no step for:
// standalone feature objects, which it refuses before the first statement.
// A table's feature changes go to the runtime in planTableFeatures, which
// plans them through the target's owner or refuses a kind no owner on the
// target plans; this planner does not decide which targets have one.
func (p *Planner) unownedFeatureChanges(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	if diff == nil {
		return nil
	}
	return &difftypes.SchemaDiff{FeatureChanges: diff.FeatureChanges}
}

// planTableFeatures lowers the owner-planned changes of surviving tables, and
// accounts for the owned state of every removed table that carries some. The
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
