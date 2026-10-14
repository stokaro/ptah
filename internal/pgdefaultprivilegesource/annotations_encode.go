package pgdefaultprivilegesource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

type annotations struct{}

func (annotations) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: default privilege export requires a context", schemaext.ErrInvalidValue)
	}
	limits, err := encodedLimits(request.Coverage)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	objects := slices.Clone(request.Objects)
	slices.SortFunc(objects, func(a, b schemaext.Object) int { return schemaext.CompareRefs(a.Ref, b.Ref) })
	result := annotation.EncodeResult{Limits: limits}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := encodedAnnotation(object)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}

func encodedAnnotation(object schemaext.Object) (annotation.EncodedDirective, error) {
	identity, value, err := desiredObject(object)
	if err != nil {
		return annotation.EncodedDirective{}, err
	}
	privileges, grantable := grantLists(value)
	attributes := []annotation.EncodedAttribute{{Name: "for_role", Value: identity.Grantor}, {Name: "object_type", Value: identity.ObjectType}, {Name: "grantee", Value: identity.Grantee}}
	for _, attribute := range []annotation.EncodedAttribute{
		{Name: "schema", Value: identity.Schema}, {Name: "privileges", Value: strings.Join(privileges, ",")},
		{Name: "grantable", Value: strings.Join(grantable, ",")}, {Name: "revoked", Value: strings.Join(value.Revoked, ",")}, {Name: "comment", Value: value.Comment},
	} {
		if attribute.Value != "" {
			attributes = append(attributes, attribute)
		}
	}
	return annotation.EncodedDirective{Directive: Directive, Placement: annotation.AfterPrivileges, Attributes: attributes, Targets: slices.Clone(object.Targets)}, nil
}

func desiredObject(object schemaext.Object) (pgdefaultprivilege.Identity, *pgdefaultprivilege.Desired, error) {
	identity, err := pgdefaultprivilege.IdentityOf(object.Ref)
	if err != nil {
		return pgdefaultprivilege.Identity{}, nil, err
	}
	value, ok := object.Value.(*pgdefaultprivilege.Desired)
	if !ok {
		return pgdefaultprivilege.Identity{}, nil, fmt.Errorf("%w: default privilege export requires desired values", schemaext.ErrInvalidValue)
	}
	if err := pgdefaultprivilege.ValidateDesired(value); err != nil {
		return pgdefaultprivilege.Identity{}, nil, err
	}
	value = value.Canonical()
	if len(value.Privileges)+len(value.Revoked) == 0 {
		return pgdefaultprivilege.Identity{}, nil, fmt.Errorf("%w: an empty default privilege intent has no declaration spelling", ptaherr.ErrUnsupportedFeature)
	}
	return identity, value, nil
}

func grantLists(value *pgdefaultprivilege.Desired) (privileges, grantable []string) {
	for _, grant := range value.Privileges {
		privileges = append(privileges, grant.Privilege)
		if grant.WithOption {
			grantable = append(grantable, grant.Privilege)
		}
	}
	return privileges, grantable
}

func (annotations) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: default privilege retention requires a context", schemaext.ErrInvalidValue)
	}
	var declared schemaext.Objects
	objects := make([]schemaext.Object, 0, len(request.Declarations))
	for _, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		object, err := annotationObject(declaration)
		if err != nil {
			return nil, err
		}
		declared, err = declared.Declare(object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	result := make([]bool, len(objects))
	for i, object := range objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, _, err := declared.Get(object.Ref)
		if err != nil {
			return nil, err
		}
		actual, found, err := request.Objects.Get(object.Ref)
		if err != nil {
			return nil, err
		}
		result[i] = found && expected.Value.Equal(actual.Value) && slices.Equal(expected.Targets, actual.Targets)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
