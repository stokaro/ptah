package engine

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
)

// ProjectTableCreations predicts CREATE effects through the selected target.
// The runtime validates declarations and returned models, preserves ownership,
// and isolates both directions. Missing services wrap ErrUnsupportedFeature;
// malformed batches wrap schemaprojection.ErrInvalid. Provider errors and
// cancellation publish no partial prediction. No database is consulted.
func (r *Runtime) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.creations == nil {
		return schemaprojection.TableCreationResult{}, fmt.Errorf("%w: target %q has no table creation projection service", ptaherr.ErrUnsupportedFeature, selected.name)
	}
	request = request.Clone()
	request.Target = selected.name
	if err := r.validateCreationInputs(ctx, request); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	result, err := selected.creations.ProjectTableCreations(ctx, request.Clone())
	if err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	result, err = schemaprojection.AcceptTableCreations(request, result)
	if err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	for i := range result.Tables {
		desired := func(kind schemaext.Kind) bool { return r.ownsCodec(selected.owner, kind, schemaext.Desired) }
		if err := r.snapshotCreationFacets(ctx, desired, "a model outside its owner", schemaext.Desired, result.Tables[i].Facets); err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		if err := r.snapshotCreationFacets(ctx, r.convertsOn(selected.name), "an observation of a model the target does not convert", schemaext.Observed, result.Tables[i].Observed); err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	return result, nil
}

// snapshotCreationFacets replaces each record's values with an independent
// snapshot in representation, refusing, as refusal says, a model allowed does
// not accept.
// Declared values must be the target owner's own models. An observation may
// be of any model the target converts, since it stands in for a conversion,
// and an owner serving several targets registers its conversions on each.
func (r *Runtime) snapshotCreationFacets(ctx context.Context, allowed func(schemaext.Kind) bool, refusal string,
	representation schemaext.Representation, records []schemaext.FacetRecord,
) error {
	for j, record := range records {
		for _, kind := range record.Values.Kinds() {
			if !allowed(kind) {
				return fmt.Errorf("%w: creation projection returned %s: %q", schemaprojection.ErrInvalid, refusal, kind)
			}
		}
		values, err := r.codecs.SnapshotFacets(ctx, representation, record.Values)
		if err != nil {
			return err
		}
		records[j].Values = values
	}
	return nil
}

// convertsOn reports the models a conversion is registered for on target.
func (r *Runtime) convertsOn(target string) func(schemaext.Kind) bool {
	return func(kind schemaext.Kind) bool {
		_, found := r.conversions[conversionKey{target: target, kind: kind}]
		return found
	}
}

func (r *Runtime) validateCreationInputs(ctx context.Context, request schemaprojection.TableCreationRequest) error {
	seen := make(map[objectidentity.Key]bool)
	identities := objectidentity.NewBuilder(request.Identifiers)
	for _, table := range request.Tables {
		declaration := table.Declaration
		if !declaration.HasTable() || table.Subject.Kind != objectidentity.KindTable || seen[table.Subject.Key()] ||
			identities.TableParts(declaration.Table.Schema, declaration.Table.Name).Key() != table.Subject.Key() {
			return fmt.Errorf("%w: invalid, mismatched, or duplicate declared table", schemaprojection.ErrInvalid)
		}
		if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: table.Subject.Kind, ID: table.Subject}, []objectidentity.ID{table.Subject}); err != nil {
			return err
		}
		seen[table.Subject.Key()] = true
		if err := r.validateCapturedTableModels(ctx, declaration, schemacapture.TableObservation{}); err != nil {
			return err
		}
	}
	return ctx.Err()
}
