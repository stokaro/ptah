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
	"ptah.run/core/schemaext"
	"ptah.run/internal/pgeffects"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

// commonOwner is the contribution owner of the PostgreSQL family's own steps.
const commonOwner = "ptah.run/postgres"

// scheduleFeatures dispatches a diff's feature changes to their owners and
// joins the contributed operations to the common sequence through
// [featurehost.Graph], which places each step in its window, orders the
// effects of different owners by their lifecycle and refuses what no window
// can order. A diff with no feature change is returned as it was planned,
// without asking the runtime.
//
// The default creation window is after the tables, columns and sequences a
// feature object may read and before the views that may read it, which is
// where a TimescaleDB aggregate goes; the default removal window is after the
// views are dropped and before the tables. The dependent window is after the
// views, the role changes, the row-security switches and the common policies,
// and before every removal step. The column changes, routine replacements and
// type rebuilds the plan makes run before it: PostgreSQL refuses ALTER COLUMN
// TYPE on a column a policy uses, and a refused routine replacement or a
// rebuilt domain or composite type drops an object a policy still uses, so a
// plan that combines one of them with removing the policy fails at that
// statement.
func (p *Planner) scheduleFeatures(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, windows featurehost.Windows) ([]ast.Node, error) {
	target := p.targetDialect()
	if diff == nil || !hasFeatureChanges(target, diff) {
		return nodes, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(target)
	request := featureplan.Request{Target: target, Identifiers: semantics, Capabilities: p.capabilities(), Changes: slices.Clone(diff.FeatureChanges),
		DatabasePath: diff.CurrentDatabasePath}
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
	graph, err := featurehost.NewGraph(commonOwner, nodes, pgeffects.Sequence(builder, nodes), windows)
	if err != nil {
		return nil, err
	}
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(ctx, runtime, request, names, featureplan.PhaseDependent)
	if err != nil {
		return nil, err
	}
	return graph.Schedule(ctx, features)
}

func hasFeatureChanges(target string, diff *difftypes.SchemaDiff) bool {
	return len(diff.FeatureChanges) > 0 || (target != platform.CockroachDB && slices.ContainsFunc(diff.TablesModified, func(table difftypes.TableDiff) bool {
		return len(table.FeatureChanges) > 0
	}))
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
// refused rather than scheduled as if it had not asked. An operation of the
// dependent phase takes the same position, which is after every creation and
// change it may name and before every drop.
func (p *Planner) planTableFeatures(ctx context.Context, runtime featureplan.Runtime, result []ast.Node, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	target := p.targetDialect()
	request := featureplan.Request{
		Target: target, Identifiers: diff.EffectiveIdentifierSemantics(target), Capabilities: p.capabilities(),
		DatabasePath: diff.CurrentDatabasePath,
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
	features, err := featurehost.Plan(ctx, runtime, request, names, featureplan.PhaseDependent)
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
