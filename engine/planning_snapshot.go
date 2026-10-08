package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

func (r *Runtime) snapshotPlanning(ctx context.Context, request featureplan.Request) (featureplan.Request, error) {
	var err error
	request.Identifiers = request.Identifiers.Clone()
	request.Capabilities = request.Capabilities.Clone()
	request.ParentKinds = slices.Clone(request.ParentKinds)
	request.Changes, err = r.codecs.SnapshotChanges(ctx, request.Changes)
	if err != nil {
		return featureplan.Request{}, err
	}
	request.Tables = slices.Clone(request.Tables)
	seen := make(map[objectidentity.Key]bool)
	builder := objectidentity.NewBuilder(request.Identifiers)
	for i, table := range request.Tables {
		switch table.Action {
		case "", featureplan.DropTable, featureplan.RebuildTable:
		default:
			return featureplan.Request{}, fmt.Errorf("%w: unknown parent action %q", schemaext.ErrInvalidValue, table.Action)
		}
		if table.Action != "" && !table.Current.HasTable() {
			return featureplan.Request{}, fmt.Errorf("%w: parent operation has no observed table state", schemaext.ErrInvalidValue)
		}
		if table.Action == featureplan.RebuildTable && !table.Desired.HasTable() {
			return featureplan.Request{}, fmt.Errorf("%w: rebuild has no declared table state", schemaext.ErrInvalidValue)
		}
		if table.Action == featureplan.DropTable && table.Desired.HasTable() {
			return featureplan.Request{}, fmt.Errorf("%w: removed table carries a declaration", schemaext.ErrInvalidValue)
		}
		if table.Subject.Kind != objectidentity.KindTable || seen[table.Subject.Key()] {
			return featureplan.Request{}, fmt.Errorf("%w: duplicate or invalid planning table", schemaext.ErrInvalidValue)
		}
		if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: table.Subject.Kind, ID: table.Subject}, []objectidentity.ID{table.Subject}); err != nil {
			return featureplan.Request{}, err
		}
		seen[table.Subject.Key()] = true
		if table.Desired.HasTable() && builder.TableParts(table.Desired.Table.Schema, table.Desired.Table.Name).Key() != table.Subject.Key() {
			return featureplan.Request{}, fmt.Errorf("%w: declared planning table disagrees with its subject", schemaext.ErrInvalidValue)
		}
		if table.Current.HasTable() && builder.TableParts(table.Current.Table.Schema, table.Current.Table.Name).Key() != table.Subject.Key() {
			return featureplan.Request{}, fmt.Errorf("%w: observed planning table disagrees with its subject", schemaext.ErrInvalidValue)
		}
		table = table.Clone()
		if err := r.validatePlanningModels(ctx, table); err != nil {
			return featureplan.Request{}, err
		}
		request.Tables[i] = table
	}
	return request, ctx.Err()
}

func (r *Runtime) validatePlanningModels(ctx context.Context, table featureplan.Table) error {
	for _, group := range planningModelGroups(table) {
		if _, err := r.codecs.SnapshotObjectState(ctx, group.representation, group.state); err != nil {
			return err
		}
		for _, facets := range group.facets {
			values, err := facets.Values()
			if err != nil {
				return err
			}
			if _, err := r.codecs.SnapshotValues(ctx, group.representation, values); err != nil {
				return err
			}
		}
	}
	return nil
}

type planningModelGroup struct {
	representation schemaext.Representation
	facets         []*schemaext.Facets
	state          schemaext.ObjectState
}

func planningModelGroups(table featureplan.Table) []planningModelGroup {
	// These temporary views reuse the models' one inventory of facet locations.
	// They neither build a desired schema nor derive missing parent definitions.
	d := table.Desired
	declared := schemamodel.Database{Tables: []schemamodel.Table{d.Table}, Fields: d.Fields, Enums: d.Enums, Constraints: d.Constraints, Indexes: d.Indexes, Triggers: d.Triggers}
	o := table.Current
	observed := catalog.Database{Tables: []catalog.Table{o.Table}, Indexes: o.Indexes, Constraints: o.Constraints, Triggers: o.Triggers}
	return []planningModelGroup{
		{schemaext.Desired, declared.FacetSlots(), schemaext.ObjectState{Objects: d.OwnedObjects, Coverage: d.FeatureCoverage}},
		{schemaext.Observed, observed.FacetSlots(), schemaext.ObjectState{Objects: o.OwnedObjects, Coverage: o.FeatureCoverage}},
	}
}
