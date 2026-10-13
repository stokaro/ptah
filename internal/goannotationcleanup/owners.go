package goannotationcleanup

import (
	"context"
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectscope"
)

func (p *Plan) retainedOwners(ctx context.Context, owners annotation.Set, source, exported *schemamodel.Database) (map[string]map[int]bool, error) {
	if !owners.Selected() {
		return nil, annotation.ErrUnselected
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: cleanup validation requires a context", ErrUnexportedAnnotation)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[string]map[int]bool)
	if source == nil || exported == nil {
		return result, nil
	}
	for _, file := range p.changes {
		request, err := retentionRequest(owners, file, source, exported)
		if err != nil {
			return nil, err
		}
		retained, err := owners.Retained(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnexportedAnnotation, err)
		}
		for i, declaration := range request.Declarations {
			if result[declaration.File] == nil {
				result[declaration.File] = make(map[int]bool)
			}
			result[declaration.File][declaration.Line] = retained[i]
		}
	}
	return result, nil
}

func retentionRequest(owners annotation.Set, file filePlan, source, exported *schemamodel.Database) (annotation.RetentionRequest, error) {
	request := annotation.RetentionRequest{Objects: exported.FeatureObjects}
	for _, table := range exported.Tables {
		request.ExportedTables = append(request.ExportedTables, annotation.RetainedTable{Table: annotation.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName}, Facets: table.Facets})
	}
	for _, removal := range file.removed {
		if removal.annotation.Directive == "ptah:schema:table" {
			for _, table := range source.Tables {
				if table.StructName == removal.structName {
					request.Tables = append(request.Tables, annotation.Table{Schema: table.Schema, Name: table.Name, Struct: table.StructName})
				}
			}
		}
		var targets []string
		var err error
		if raw, present := removal.values["dialects"]; present {
			targets, err = dialectscope.Parse(raw)
			if err != nil {
				return annotation.RetentionRequest{}, err
			}
		}
		_, owned := owners.Owner(removal.annotation.Directive)
		if !owned {
			_, owned, err = owners.TargetOwner(removal.annotation.Directive, targets)
			if err != nil {
				return annotation.RetentionRequest{}, err
			}
		}
		if owned {
			request.Declarations = append(request.Declarations, annotation.Declaration{
				Directive: removal.annotation.Directive, Attributes: removal.values, Targets: targets,
				Struct: removal.structName, File: removal.annotation.Path, Line: removal.annotation.Line,
			})
		}
	}
	return request, nil
}
