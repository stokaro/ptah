// Package spannersource encodes the Spanner row deletion policy in source
// property groups and records what a source format can declare. Source syntax
// is decoded by a frontend; the policy's meaning stays in spannerschema.
package spannersource

import (
	"context"
	"fmt"
	"maps"

	"ptah.run/core/annotation"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/rowdeletion"
)

// managed are the properties a Spanner policy takes, in the order a refusal
// lists them. The clause reads a timestamp column only, so a unit is refused
// by name.
var managed = []string{rowdeletion.ColumnProperty, rowdeletion.IntervalProperty}

// Service carries the row deletion policy through string-valued platform
// properties: row_deletion_column names the timestamp column and
// row_deletion_interval the interval in Spanner's spelling, such as `30 days`.
// It performs no inspection.
type Service struct{}

// Definitions returns independent property ownership declarations. The owner
// claims every key that begins with row_deletion in any case, so a misspelled
// or miscased property, and a unit the clause cannot read, is refused by name
// rather than left as a table option nothing reads. Go annotations prefix these
// keys with platform.spanner.; YAML places them in the spanner platform group.
func Definitions() []schemaext.PropertyDefinition {
	return []schemaext.PropertyDefinition{{Kind: spannerschema.RowDeletionKind, Keys: append([]string(nil), managed...), Prefixes: []string{rowdeletion.PropertyPrefix}}}
}

func validateRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: Spanner source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != platform.Spanner {
		return fmt.Errorf("%w: Spanner source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: Spanner source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties decodes an ordered batch into DesiredRowDeletion values. An
// unknown property, a blank value, a policy without its column or its
// interval, and an interval the server refuses wrap schemaext.ErrInvalidValue.
// Any failure or cancellation returns no partial batch.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != spannerschema.RowDeletionKind {
			return nil, fmt.Errorf("%w: Spanner source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		declaration, err := rowdeletion.Decode(maps.Clone(fragment.Properties), managed)
		if err != nil {
			return nil, err
		}
		value := &spannerschema.DesiredRowDeletion{Policy: spannerschema.Policy{Column: declaration.Column, Interval: declaration.Interval}}
		if err := spannerschema.ValidateDesired(value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties writes the column and the interval of a valid declaration.
// Unknown model types and invalid declarations wrap schemaext.ErrInvalidValue.
// Returned maps do not alias inputs or each other. Any failure or cancellation
// returns no partial batch.
func (Service) EncodeProperties(ctx context.Context, request schemaext.PropertyEncodeRequest) ([]schemaext.PropertyFragment, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.PropertyFragment, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declared, ok := value.(*spannerschema.DesiredRowDeletion)
		if !ok {
			return nil, fmt.Errorf("%w: Spanner source expected a desired row deletion policy, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := spannerschema.ValidateDesired(declared); err != nil {
			return nil, err
		}
		result = append(result, schemaext.PropertyFragment{
			Kind:       spannerschema.RowDeletionKind,
			Properties: rowdeletion.Encode(rowdeletion.Declaration{Column: declared.Policy.Column, Interval: declared.Policy.Interval}),
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Coverage is the knowledge a source format that carries platform properties
// holds: it could have declared a row deletion policy on any of its tables, so
// a table without one requests none. A format without platform properties must
// not enroll it, and its tables leave an existing policy unmanaged.
func Coverage() (schemaext.Coverage, error) {
	return spannerschema.RowDeletionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// Annotations is the owner's contribution to the Go annotation frontend. A Go
// table annotation carries platform.spanner properties, so the source could
// have declared a row deletion policy on any table, and the claim is
// [Coverage]. The owner declares no directive of its own; the properties are
// decoded by [Service] once a target is selected.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:    spannerschema.Owner,
		Kinds:    []schemaext.Kind{spannerschema.RowDeletionKind},
		Coverage: Coverage,
	}
}
