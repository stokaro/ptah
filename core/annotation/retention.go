package annotation

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Retainer checks whether an export preserves the declarations of one owner.
// The request is a batch of one file's declarations and captured schema data.
// Implementations must not modify the request or retain mutable input.
type Retainer interface {
	// RetainAnnotations returns one verdict per declaration, in request order.
	// False means the export does not prove retention; errors discard all verdicts.
	RetainAnnotations(context.Context, RetentionRequest) ([]bool, error)
}

// RetainedTable describes an exported table and the owner's facets on it.
// Struct is source placement metadata, not part of the table's identity.
type RetainedTable struct {
	Table  Table
	Facets schemaext.Facets
}

// RetentionRequest supplies declarations whose removal needs proof that the
// export retains their meaning. Tables are declared in the source file;
// ExportedTables and Objects describe the exported schema. The owner compares
// semantic data and accounts for source metadata its export does not carry.
type RetentionRequest struct {
	Declarations   []Declaration
	Tables         Tables
	ExportedTables []RetainedTable
	Objects        schemaext.Objects
}

// Retained asks each selected owner once about its declarations in a file.
// The result has one entry per declaration, in input order. False means removal
// is unsafe, including when the owner provides no Retainer. Unselected sets,
// unowned directives, invalid target scopes, malformed replies and canceled
// requests return an error and no partial result. Inputs remain unchanged.
func (s Set) Retained(ctx context.Context, request RetentionRequest) ([]bool, error) {
	if !s.selected {
		return nil, ErrUnselected
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: annotation retention requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groups := make(map[int][]int)
	for position, declaration := range request.Declarations {
		index, found := s.owners[declaration.Directive]
		if !found {
			var err error
			index, found, err = s.scopeIndex(declaration.Directive, declaration.Targets)
			if err != nil {
				return nil, err
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: no selected owner checks directive %q", schemaext.ErrInvalidValue, declaration.Directive)
		}
		groups[index] = append(groups[index], position)
	}
	result := make([]bool, len(request.Declarations))
	for index, extension := range s.extensions {
		positions := groups[index]
		if len(positions) == 0 || extension.Retainer == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := retentionBatch(request, extension.Kinds, positions)
		retained, err := extension.Retainer.RetainAnnotations(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("annotation retention of %s: %w", extension.Owner, err)
		}
		if len(retained) != len(positions) {
			return nil, fmt.Errorf("%w: %s returns %d retention results for %d declarations", schemaext.ErrInvalidValue, extension.Owner, len(retained), len(positions))
		}
		for i, position := range positions {
			result[position] = retained[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func retentionBatch(request RetentionRequest, kinds []schemaext.Kind, positions []int) RetentionRequest {
	batch := RetentionRequest{Tables: slices.Clone(request.Tables), Objects: request.Objects.Select(func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) })}
	for _, position := range positions {
		declaration := request.Declarations[position]
		declaration.Attributes = cloneAttributes(declaration.Attributes)
		declaration.Targets = slices.Clone(declaration.Targets)
		batch.Declarations = append(batch.Declarations, declaration)
	}
	for _, table := range request.ExportedTables {
		for _, kind := range table.Facets.DeclaredKinds() {
			if !slices.Contains(kinds, kind) {
				table.Facets = table.Facets.Without(kind)
			}
		}
		batch.ExportedTables = append(batch.ExportedTables, table)
	}
	return batch
}
