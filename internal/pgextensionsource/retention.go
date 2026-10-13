package pgextensionsource

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type annotationRetainer struct{}

func (annotationRetainer) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	result := make([]bool, len(request.Declarations))
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if declaration.Directive != Directive {
			return nil, fmt.Errorf("%w: unexpected extension directive %q", schemaext.ErrInvalidValue, declaration.Directive)
		}
		contributions, err := decodeDirective(declaration)
		if err != nil {
			return nil, err
		}
		expected, err := schemaext.NewObjects(*contributions[0].Object)
		if err != nil {
			return nil, err
		}
		actual := request.Objects.Select(func(ref objectidentity.ID) bool { return ref.Key() == contributions[0].Object.Ref.Key() })
		declared, _, err := expected.Get(contributions[0].Object.Ref)
		if err != nil {
			return nil, err
		}
		exported, _, err := actual.Get(contributions[0].Object.Ref)
		if err != nil {
			return nil, err
		}
		result[i] = expected.Equal(actual) && slices.Equal(declared.Targets, exported.Targets)
	}
	return result, ctx.Err()
}
