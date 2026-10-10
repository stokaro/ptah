package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemacapture"
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
		if err := validateParentAction(table); err != nil {
			return featureplan.Request{}, err
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
		if err := r.validateCapturedTableModels(ctx, table.Desired, table.Current); err != nil {
			return featureplan.Request{}, err
		}
		request.Tables[i] = table
	}
	request.CommonSteps, err = r.snapshotCommonSteps(ctx, request)
	if err != nil {
		return featureplan.Request{}, err
	}
	return request, ctx.Err()
}

// validateParentAction checks that a table carries the captures its action
// needs: an observation for an operation on a table that exists, a declaration
// for one that survives, none for one the plan drops, and a declaration and no
// observation for one the plan creates.
func validateParentAction(table featureplan.Table) error {
	switch table.Action {
	case "", featureplan.DropTable, featureplan.RebuildTable, featureplan.AlterTable, featureplan.CreateTable:
	default:
		return fmt.Errorf("%w: unknown parent action %q", schemaext.ErrInvalidValue, table.Action)
	}
	if table.Action == featureplan.CreateTable && (!table.Desired.HasTable() || table.Current.HasTable()) {
		return fmt.Errorf("%w: a created table carries its declaration and no observation", schemaext.ErrInvalidValue)
	}
	if table.Action != "" && table.Action != featureplan.CreateTable && !table.Current.HasTable() {
		return fmt.Errorf("%w: parent operation has no observed table state", schemaext.ErrInvalidValue)
	}
	if (table.Action == featureplan.RebuildTable || table.Action == featureplan.AlterTable) && !table.Desired.HasTable() {
		return fmt.Errorf("%w: surviving parent has no declared table state", schemaext.ErrInvalidValue)
	}
	if table.Action == featureplan.DropTable && table.Desired.HasTable() {
		return fmt.Errorf("%w: removed table carries a declaration", schemaext.ErrInvalidValue)
	}
	return nil
}

func (r *Runtime) validateCapturedTableModels(ctx context.Context, declared schemacapture.TableDeclaration, observed schemacapture.TableObservation) error {
	for _, group := range capturedTableModelGroups(declared, observed) {
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

func capturedTableModelGroups(d schemacapture.TableDeclaration, o schemacapture.TableObservation) []planningModelGroup {
	// These temporary views reuse the models' one inventory of facet locations.
	// They neither build a desired schema nor derive missing parent definitions.
	declared := schemamodel.Database{Tables: []schemamodel.Table{d.Table}, Fields: d.Fields, Enums: d.Enums, Constraints: d.Constraints, Indexes: d.Indexes, Triggers: d.Triggers}
	observed := catalog.Database{Tables: []catalog.Table{o.Table}, Indexes: o.Indexes, Constraints: o.Constraints, Triggers: o.Triggers}
	return []planningModelGroup{
		{schemaext.Desired, declared.FacetSlots(), schemaext.ObjectState{Objects: d.OwnedObjects, Coverage: d.FeatureCoverage}},
		{schemaext.Observed, observed.FacetSlots(), schemaext.ObjectState{Objects: o.OwnedObjects, Coverage: o.FeatureCoverage}},
	}
}
