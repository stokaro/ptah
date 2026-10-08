package engine

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
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
