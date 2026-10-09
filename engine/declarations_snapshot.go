package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (r *Runtime) snapshotDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationRequest, error) {
	// Creation and migration use the same isolation and metadata validation.
	// Only declared tables are supplied here; no catalog state is synthesized.
	planning := featureplan.Request{Identifiers: request.Identifiers, Capabilities: request.Capabilities,
		CommonSteps: request.CommonSteps, Tables: declarationTables(request)}
	for _, table := range planning.Tables {
		if !table.Desired.HasTable() {
			return featureplan.DeclarationRequest{}, fmt.Errorf("%w: declaration dependency has no table", schemaext.ErrInvalidValue)
		}
	}
	planning, err := r.snapshotPlanning(ctx, planning)
	if err != nil {
		return featureplan.DeclarationRequest{}, err
	}
	request.Identifiers, request.Capabilities = planning.Identifiers, planning.Capabilities
	request.CommonSteps = planning.CommonSteps
	request.Tables = slices.Clone(request.Tables)
	for i, table := range planning.Tables {
		request.Tables[i] = table.Desired
	}
	values := make([]schemaext.Value, len(request.Objects))
	seen := make(map[objectidentity.Key]bool)
	for i, object := range request.Objects {
		if err := schemaext.ValidatePayload(object.Value); err != nil {
			return featureplan.DeclarationRequest{}, err
		}
		if seen[object.Ref.Key()] || object.Ref.Kind != objectidentity.Kind(object.Value.Kind()) || object.Ref.Parent != (objectidentity.Part{}) {
			return featureplan.DeclarationRequest{}, fmt.Errorf("%w: duplicate or non-standalone declaration", schemaext.ErrInvalidValue)
		}
		if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: object.Ref.Kind, ID: object.Ref}, []objectidentity.ID{object.Ref}); err != nil {
			return featureplan.DeclarationRequest{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		seen[object.Ref.Key()] = true
		values[i] = object.Value
	}
	values, err = r.codecs.SnapshotValues(ctx, schemaext.Desired, values)
	if err != nil {
		return featureplan.DeclarationRequest{}, err
	}
	request.Objects = slices.Clone(request.Objects)
	for i, value := range values {
		request.Objects[i].Value = value
	}
	return request, ctx.Err()
}

func declarationTables(request featureplan.DeclarationRequest) []featureplan.Table {
	tables := make([]featureplan.Table, len(request.Tables))
	builder := objectidentity.NewBuilder(request.Identifiers)
	for i, table := range request.Tables {
		tables[i] = featureplan.Table{Subject: builder.TableParts(table.Table.Schema, table.Table.Name), Desired: table}
	}
	return tables
}
