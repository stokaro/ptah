package pgpolicysource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

type annotationRetainer struct{}

func (annotationRetainer) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	result := make([]bool, len(request.Declarations))
	decoder := &fileDecoder{}
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		contribution, err := retentionContribution(decoder, request.Tables, declaration)
		if err != nil {
			return nil, err
		}
		result[i], err = retainContribution(contribution, request)
		if err != nil {
			return nil, err
		}
	}
	return result, ctx.Err()
}

func retentionContribution(decoder *fileDecoder, tables annotation.Tables, declaration annotation.Declaration) (annotation.Contribution, error) {
	switch declaration.Directive {
	case PolicyDirective:
		return decoder.policy(tables, declaration)
	case EnableDirective:
		return decoder.enable(tables, declaration)
	default:
		return annotation.Contribution{}, fmt.Errorf("%w: unexpected row-security directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
}

func retainContribution(contribution annotation.Contribution, request annotation.RetentionRequest) (bool, error) {
	if contribution.Object != nil {
		return retainPolicy(contribution.Object, request.Objects)
	}
	position, err := request.Tables.Owning(contribution.Source.Struct, contribution.Table, contribution.Label)
	if err != nil {
		return false, err
	}
	source := request.Tables[position]
	expected, err := schemaext.NewFacets(contribution.Facet)
	if err != nil {
		return false, err
	}
	expected, err = expected.WithTargetScope(contribution.Facet.Kind(), contribution.Targets...)
	if err != nil {
		return false, err
	}
	for _, table := range request.ExportedTables {
		if table.Table.Schema != source.Schema || table.Table.Name != source.Name {
			continue
		}
		value, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](table.Facets, pgpolicy.TableStateKind)
		if err != nil || !found {
			return false, err
		}
		declared, ok := contribution.Facet.(*pgpolicy.DesiredTableState)
		if !ok {
			return false, fmt.Errorf("%w: retention requires table security switches", schemaext.ErrInvalidValue)
		}
		value.StructName = declared.StructName
		actual, err := schemaext.NewFacets(value)
		if err != nil {
			return false, err
		}
		actual, err = actual.WithTargetScope(pgpolicy.TableStateKind, table.Facets.TargetScope(pgpolicy.TableStateKind)...)
		if err != nil {
			return false, err
		}
		return expected.Equal(actual) && slices.Equal(expected.TargetScope(pgpolicy.TableStateKind), actual.TargetScope(pgpolicy.TableStateKind)), nil
	}
	return false, nil
}

func retainPolicy(object *schemaext.Object, exported schemaext.Objects) (bool, error) {
	expected, err := schemaext.NewObjects(*object)
	if err != nil {
		return false, err
	}
	actual, found, err := exported.Get(object.Ref)
	if err != nil || !found {
		return false, err
	}
	policy, ok := actual.Value.(*pgpolicy.DesiredPolicy)
	if !ok {
		return false, nil
	}
	declared, ok := object.Value.(*pgpolicy.DesiredPolicy)
	if !ok {
		return false, fmt.Errorf("%w: retention requires a policy declaration", schemaext.ErrInvalidValue)
	}
	policy.StructName = declared.StructName
	objects, err := schemaext.NewObjects(actual)
	if err != nil {
		return false, err
	}
	declaredObject, _, err := expected.Get(object.Ref)
	if err != nil {
		return false, err
	}
	return expected.Equal(objects) && slices.Equal(declaredObject.Targets, actual.Targets), nil
}
