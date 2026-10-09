package ydb

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/migration/schemadiff/difftypes"
)

func (p *Planner) planFeatureChanges(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, rebuilds map[string]*tableRebuild, semantics identifier.Semantics) (featurehost.Result, error) {
	names := make(map[objectidentity.Key]string)
	request := featureplan.Request{Target: platform.YDB, Identifiers: semantics, Capabilities: p.caps, Changes: slices.Clone(diff.FeatureChanges)}
	for _, table := range diff.TablesModified {
		if len(table.FeatureChanges) == 0 {
			continue
		}
		_, rebuilt := rebuilds[semantics.TableIdentityKey(table.TableName)]
		ref := table.FeatureChanges[0].Subject
		subject := featureParent(ref)
		if objectidentity.NewBuilder(semantics).Table(table.TableName).Key() != subject.Key() {
			return featurehost.Result{}, fmt.Errorf("%w: feature parent disagrees with the changed table", schemaext.ErrInvalidValue)
		}
		if !rebuilt {
			request.Tables = append(request.Tables, featureplan.Table{Subject: subject, Desired: table.Desired, Current: table.Current})
		}
		names[subject.Key()] = table.TableName
		request.Changes = append(request.Changes, table.FeatureChanges...)
	}
	builder := objectidentity.NewBuilder(semantics)
	for _, key := range slices.Sorted(maps.Keys(rebuilds)) {
		rebuild := rebuilds[key]
		declared := rebuild.declaration.Table
		subject := builder.TableParts(declared.Schema, declared.Name)
		request.Tables = append(request.Tables, featureplan.Table{Action: featureplan.RebuildTable, Subject: subject, Desired: rebuild.declaration, Current: rebuild.observation})
	}
	for _, removal := range diff.TablesRemoved {
		current := removal.Current.Table
		subject := builder.TableParts(current.Schema, current.Name)
		if removal.Current.HasTable() && builder.Table(removal.Name).Key() != subject.Key() {
			return featurehost.Result{}, fmt.Errorf("%w: removal name disagrees with captured table", schemaext.ErrInvalidValue)
		}
		request.Tables = append(request.Tables, featureplan.Table{Action: featureplan.DropTable, Subject: subject, Current: removal.Current})
	}
	result, err := featurehost.Plan(ctx, runtime, request, names)
	if err != nil {
		return featurehost.Result{}, err
	}
	if err := capturePlannedRebuilds(rebuilds); err != nil {
		return featurehost.Result{}, err
	}
	return result, nil
}

// Capture and emission bindings share one subject: a facet names the table
// itself; a named child carries its parent.
func featureParent(ref objectidentity.ID) objectidentity.ID {
	if ref.Kind == objectidentity.KindTable {
		return ref
	}
	return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
}
