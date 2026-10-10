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

// featureOwner is the contribution owner of the common steps a plan of this
// planner joins feature operations to, on every dialect it serves.
const featureOwner = "ptah.run/sqlserver"

// hostsFeatures reports whether this planner dispatches feature changes to
// their owners. Every dialect it serves does: SQL Server, Oracle, MySQL and
// MariaDB, whose owner plans the changes of its index options.
func (p *Planner) hostsFeatures() bool {
	switch p.targetDialect() {
	case platform.SQLServer, platform.Oracle, platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// dropsRoutinesLate reports whether routines are dropped after the tables and
// the feature objects that may call them rather than with the other routine
// changes. SQL Server does, because its security policies call predicate
// functions; on the other dialects of this planner no owner's object calls a
// routine.
func (p *Planner) dropsRoutinesLate() bool { return p.targetDialect() == platform.SQLServer }

// refuseUnhostedFeatureChanges refuses the feature changes this planner has
// no window for: every one on a target without owners, and the settings of a
// materialized view, which no dialect of this planner has a window for.
func (p *Planner) refuseUnhostedFeatureChanges(diff *difftypes.SchemaDiff) error {
	if p.hostsFeatures() {
		return schemaprecondition.RefuseMaterializedViewFeatureChanges(p.targetDialect(), diff)
	}
	return schemaprecondition.RefuseFeatureChanges(p.targetDialect(), diff)
}

// scheduleFeatures dispatches a diff's feature changes, standalone
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
func (p *Planner) scheduleFeatures(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, nodes []ast.Node, windows featurehost.Windows) ([]ast.Node, []string, error) {
	if !p.hostsFeatures() || diff == nil || !hasFeatureChanges(diff) {
		return nodes, nil, nil
	}
	target := p.targetDialect()
	semantics := diff.EffectiveIdentifierSemantics(target)
	builder := objectidentity.NewBuilder(semantics)
	request := featureplan.Request{Target: target, Identifiers: semantics, Capabilities: p.capabilities(), Changes: slices.Clone(diff.FeatureChanges),
		DatabasePath: diff.CurrentDatabasePath}
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
		return nil, nil, err
	}
	request.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(ctx, runtime, request, names, featureplan.PhaseDependent)
	if err != nil {
		return nil, nil, err
	}
	scheduled, err := graph.Schedule(ctx, features)
	if err != nil {
		return nil, nil, err
	}
	return scheduled, createdOwnedSchemas(features), nil
}

func hasFeatureChanges(diff *difftypes.SchemaDiff) bool {
	return len(diff.FeatureChanges) > 0 || slices.ContainsFunc(diff.TablesModified, func(table difftypes.TableDiff) bool {
		return len(table.FeatureChanges) > 0
	})
}
