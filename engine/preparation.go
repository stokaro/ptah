package engine

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

// PrepareTables normalizes captured common representations through the selected
// target. Inputs must already carry target scope and its table identities. The
// runtime validates model registrations, clones both directions, and refuses
// missing services, incomplete replies, identity changes, and stronger knowledge.
// Provider errors and cancellation expose no partial prepared result.
func (r *Runtime) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemapreparation.Result{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return schemapreparation.Result{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.preparation == nil {
		return schemapreparation.Result{}, fmt.Errorf("%w: target %q has no table preparation service", ptaherr.ErrUnsupportedFeature, selected.name)
	}
	request.Target = selected.name
	request = request.Clone()
	for _, table := range request.Tables {
		if len(table.ResolvedFacets) != 0 {
			return schemapreparation.Result{}, fmt.Errorf("%w: preparation input already carries resolved facets", schemapreparation.ErrInvalid)
		}
	}
	if err := r.validatePreparation(ctx, request); err != nil {
		return schemapreparation.Result{}, err
	}
	result, err := selected.preparation.PrepareTables(ctx, request.Clone())
	if err != nil {
		return schemapreparation.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	capture, err := schemapreparation.Accept(request, result)
	if err != nil {
		return schemapreparation.Result{}, err
	}
	for i := range capture.Prepared {
		for j, record := range capture.Prepared[i].ResolvedFacets {
			facets, err := r.snapshotResolvedFacets(ctx, selected.owner, record.Values)
			if err != nil {
				return schemapreparation.Result{}, err
			}
			capture.Prepared[i].ResolvedFacets[j].Values = facets
		}
	}
	prepared := request
	prepared.Tables = capture.Prepared
	if err := r.validatePreparation(ctx, prepared); err != nil {
		return schemapreparation.Result{}, err
	}
	return schemapreparation.Result{Complete: true, Tables: prepared.Tables}, nil
}

func (r *Runtime) validatePreparation(ctx context.Context, request schemapreparation.Request) error {
	seen := make(map[objectidentity.Key]bool)
	for _, table := range request.Tables {
		if table.Subject.Kind != objectidentity.KindTable || seen[table.Subject.Key()] || !table.Desired.HasTable() {
			return fmt.Errorf("%w: invalid or duplicate declared table", schemapreparation.ErrInvalid)
		}
		if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: table.Subject.Kind, ID: table.Subject}, []objectidentity.ID{table.Subject}); err != nil {
			return err
		}
		seen[table.Subject.Key()] = true
		switch table.CurrentKnowledge.State {
		case schemaext.Complete:
			if !table.Current.HasTable() {
				return fmt.Errorf("%w: present table has no current capture", schemapreparation.ErrInvalid)
			}
		case schemaext.Absent, schemaext.Uninspected:
			if table.Current.HasTable() {
				return fmt.Errorf("%w: table existence contradicts its capture", schemapreparation.ErrInvalid)
			}
		default:
			return fmt.Errorf("%w: unsupported table existence knowledge", schemapreparation.ErrInvalid)
		}
		if err := r.validateCapturedTableModels(ctx, table.Desired, table.Current); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r *Runtime) snapshotResolvedFacets(ctx context.Context, owner string, facets schemaext.Facets) (schemaext.Facets, error) {
	for _, kind := range facets.Kinds() {
		if !r.ownsCodec(owner, kind, schemaext.Desired) {
			return schemaext.Facets{}, fmt.Errorf("%w: preparation resolved a model outside its owner: %q", schemapreparation.ErrInvalid, kind)
		}
	}
	return r.codecs.SnapshotFacets(ctx, schemaext.Desired, facets)
}

// LowerDesired rewrites a whole desired schema through the selected target's
// lowering service, before comparison. A target without one, or a nil desired
// schema, returns request.Desired unchanged. The request is not cloned, so a
// service must not modify it. An unregistered target fails with
// [ptaherr.ErrUnsupportedDialect], and a nil result from a service with
// [schemapreparation.ErrInvalid].
func (r *Runtime) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return nil, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.lowering == nil || request.Desired == nil {
		return request.Desired, nil
	}
	request.Target = selected.name
	lowered, err := selected.lowering.LowerDesired(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lowered == nil {
		return nil, fmt.Errorf("%w: target %q lowered the desired schema to nothing", schemapreparation.ErrInvalid, selected.name)
	}
	return lowered, nil
}
