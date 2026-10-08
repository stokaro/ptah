package ydb

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) planFeatureChanges(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, rebuilds map[string]*tableRebuild, semantics identifier.Semantics) (featureplan.Result, error) {
	request := featureplan.Request{Target: platform.YDB, Identifiers: semantics, Capabilities: p.caps, Changes: slices.Clone(diff.FeatureChanges)}
	for _, table := range diff.TablesModified {
		if len(table.FeatureChanges) == 0 {
			continue
		}
		_, rebuilt := rebuilds[semantics.TableIdentityKey(table.TableName)]
		ref := table.FeatureChanges[0].Subject
		subject := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
		if objectidentity.NewBuilder(semantics).Table(table.TableName).Key() != subject.Key() {
			return featureplan.Result{}, fmt.Errorf("%w: feature parent disagrees with the changed table", schemaext.ErrInvalidValue)
		}
		request.Tables = append(request.Tables, featureplan.Table{Rebuild: rebuilt, Subject: subject, Desired: table.Desired, Current: table.Current})
		request.Changes = append(request.Changes, table.FeatureChanges...)
	}
	return runtime.PlanFeatures(ctx, request)
}
