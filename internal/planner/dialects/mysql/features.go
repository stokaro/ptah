package mysql

import (
	"context"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/internal/pgeffects"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// featureOwner is the contribution owner of the common steps a SQL Server
// plan joins feature operations to.
const featureOwner = "ptah.run/sqlserver"

// hostsFeatures reports whether this planner dispatches feature changes to
// their owners. SQL Server does; MySQL, MariaDB and Oracle have no feature
// owner and refuse every feature change.
func (p *Planner) hostsFeatures() bool { return p.targetDialect() == platform.SQLServer }

// refuseUnhostedFeatureChanges refuses the feature changes this planner has
// no window for: every one on a target without owners, and the settings of a
// materialized view on SQL Server, which has none.
func (p *Planner) refuseUnhostedFeatureChanges(diff *difftypes.SchemaDiff) error {
	if p.hostsFeatures() {
		return schemaprecondition.RefuseMaterializedViewFeatureChanges(p.targetDialect(), diff)
	}
	return schemaprecondition.RefuseFeatureChanges(p.targetDialect(), diff)
}

// scheduleFeatures dispatches a SQL Server diff's feature changes, standalone
// and attached to surviving tables, to their owners and joins the operations
// to the common sequence in the windows recorded while it was planned. A diff
// with no feature change is returned as it was planned, without asking the
// runtime.
//
// Every common statement carries the effects [pgeffects] reads from the
// common nodes, which name the tables, columns, views, routines and roles a
// SQL Server plan writes the same way a PostgreSQL one does. An owner orders
// itself against them, and [featurehost.Graph.Schedule] orders different
// owners' effects against each other.
func (p *Planner) scheduleFeatures(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, windows featurehost.Windows) ([]ast.Node, error) {
	if !p.hostsFeatures() || diff == nil || !hasFeatureChanges(diff) {
		return nodes, nil
	}
	target := p.targetDialect()
	semantics := diff.EffectiveIdentifierSemantics(target)
	builder := objectidentity.NewBuilder(semantics)
	request := featureplan.Request{Target: target, Identifiers: semantics, Capabilities: p.capabilities(), Changes: slices.Clone(diff.FeatureChanges)}
	names := make(map[objectidentity.Key]string)
	for _, table := range diff.TablesModified {
		if len(table.FeatureChanges) == 0 {
			continue
		}
		subject := builder.Table(table.TableName)
		request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, table.FeatureChanges...)
		names[subject.Key()] = table.TableName
	}
	graph, err := featurehost.NewGraph(featureOwner, nodes, pgeffects.Sequence(builder, nodes), windows)
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

func hasFeatureChanges(diff *difftypes.SchemaDiff) bool {
	return len(diff.FeatureChanges) > 0 || slices.ContainsFunc(diff.TablesModified, func(table difftypes.TableDiff) bool {
		return len(table.FeatureChanges) > 0
	})
}
