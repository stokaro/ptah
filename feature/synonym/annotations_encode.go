package synonym

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// annotationEncoder writes declarations through the shared Go writer.
type annotationEncoder struct{}

// EncodeAnnotations preserves the declaration's attributes. The directive
// binds the object to the owner's targets; another explicit scope is refused.
// Coverage the directive cannot express is refused before writing any object.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if err := requireAnnotationCoverage(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	objects, err := schemaext.NewObjects(request.Objects...)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	ordered, err := objects.All()
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	var result annotation.EncodeResult
	for _, object := range ordered {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := encodedDeclaration(object)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	return result, nil
}

func encodedDeclaration(object schemaext.Object) (annotation.EncodedDirective, error) {
	value, ok := object.Value.(*DesiredSynonym)
	if !ok {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: Go annotations require a desired synonym for %s, got %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
	if err := ValidateDesired(value); err != nil {
		return annotation.EncodedDirective{}, err
	}
	if object.Ref != value.Ref() {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: synonym identity %s differs from its declaration", schemaext.ErrInvalidValue, object.Ref)
	}
	if len(object.Targets) != 0 && (len(object.Targets) != 2 || !slices.Contains(object.Targets, platform.SQLServer) || !slices.Contains(object.Targets, platform.Oracle)) {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: Go annotations cannot preserve synonym %s target scope %v", ptaherr.ErrUnsupportedFeature, object.Ref, object.Targets)
	}
	var attributes []annotation.EncodedAttribute
	attributes = append(attributes, annotation.EncodedAttribute{Name: "name", Value: value.Name})
	if value.Schema != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "schema", Value: value.Schema})
	}
	attributes = append(attributes, annotation.EncodedAttribute{Name: "target", Value: value.Target})
	if value.Comment != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "comment", Value: value.Comment})
	}
	return annotation.EncodedDirective{Directive: Directive, Placement: annotation.AfterViews, Attributes: attributes}, nil
}

// A declaration claims complete namespace knowledge. An incomplete read must
// not gain authority to remove objects through a successful export.
func requireAnnotationCoverage(known schemaext.Coverage) error {
	for _, record := range known.KindRecords() {
		if record.Knowledge.State != schemaext.Complete {
			return fmt.Errorf("%w: Go annotations cannot preserve synonym namespace coverage: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Reason)
		}
	}
	for _, record := range known.SubjectRecords() {
		if record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return fmt.Errorf("%w: Go annotations cannot preserve synonym %s coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}
