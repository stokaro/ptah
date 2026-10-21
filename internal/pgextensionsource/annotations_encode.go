package pgextensionsource

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// annotationEncoder writes extension declarations through the shared Go writer.
type annotationEncoder struct{}

// EncodeAnnotations orders extensions by name and preserves their target scopes.
// Provides records catalog dependencies and has no declaration attribute.
// Observations and coverage the annotation cannot preserve are refused.
func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if err := requireAnnotationCoverage(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	objects := slices.Clone(request.Objects)
	slices.SortStableFunc(objects, func(a, b schemaext.Object) int {
		return cmp.Compare(pgextension.Name(a.Ref), pgextension.Name(b.Ref))
	})
	var result annotation.EncodeResult
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := extensionDirective(object)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	return result, nil
}

func extensionDirective(object schemaext.Object) (annotation.EncodedDirective, error) {
	extension, ok := object.Value.(*pgextension.DesiredExtension)
	if !ok {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: Go annotations require a desired extension for %s, got %T", ptaherr.ErrUnsupportedFeature, object.Ref, object.Value)
	}
	if err := pgextension.ValidateRef(object.Ref); err != nil {
		return annotation.EncodedDirective{}, err
	}
	if err := pgextension.ValidateDesired(extension); err != nil {
		return annotation.EncodedDirective{}, err
	}
	attributes := []annotation.EncodedAttribute{{Name: "name", Value: pgextension.Name(object.Ref)}}
	if extension.Schema != "" {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "schema", Value: extension.Schema})
	}
	if extension.IfNotExists {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "if_not_exists", Value: "true"})
	}
	for _, setting := range [][2]string{{"version", extension.Version}, {"comment", extension.Comment}} {
		if setting[1] != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: setting[0], Value: setting[1]})
		}
	}
	return annotation.EncodedDirective{Directive: Directive, Placement: annotation.AfterSchemas,
		Attributes: attributes, Targets: slices.Clone(object.Targets)}, nil
}

// A declaration describes its namespace completely. A read that cannot do so
// must not gain authority to remove extensions through a successful export.
func requireAnnotationCoverage(known schemaext.Coverage) error {
	for _, record := range known.KindRecords() {
		if record.Knowledge.Unscoped().State != schemaext.Complete {
			return fmt.Errorf("%w: Go annotations cannot preserve extension namespace coverage: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Unscoped().Reason)
		}
	}
	for _, record := range known.SubjectRecords() {
		if record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return fmt.Errorf("%w: Go annotations cannot preserve extension %s coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	return nil
}
