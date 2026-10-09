// Package crdbsource encodes CockroachDB row-level TTL in source property
// groups and records what a source format can declare. Source syntax is decoded
// by a frontend; row-level TTL semantics stay in crdbschema.
package crdbsource

import (
	"context"
	"fmt"
	"maps"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// Service carries row-level TTL through string-valued platform properties. A
// property is a storage parameter under its own name, so what an author writes
// is what the statement carries and what the catalog reports. It performs no
// inspection.
type Service struct{}

// Definitions returns independent property ownership declarations: every
// managed parameter, and the derived ttl marker so that declaring it is refused
// with the reason instead of passing through as an unrelated table option. Go
// annotations prefix these keys with platform.cockroachdb.; YAML places them
// in the cockroachdb platform group.
func Definitions() []schemaext.PropertyDefinition {
	keys := append([]string{crdbschema.MarkerParameter}, crdbschema.ManagedParameters()...)
	return []schemaext.PropertyDefinition{{Kind: crdbschema.RowTTLKind, Keys: keys}}
}

func validateRequest(ctx context.Context, target string, format schemaext.PropertyFormat) error {
	if ctx == nil {
		return fmt.Errorf("%w: CockroachDB source requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != platform.CockroachDB {
		return fmt.Errorf("%w: CockroachDB source target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	if format != schemaext.TablePlatformProperties {
		return fmt.Errorf("%w: CockroachDB source format %q", ptaherr.ErrUnsupportedFeature, format)
	}
	return nil
}

// DecodeProperties decodes an ordered batch into DesiredRowTTL values with
// crdbschema.DecodeDeclared: unknown and refused parameters, malformed values,
// and a policy without an expiry wrap schemaext.ErrInvalidValue. Any failure or
// cancellation returns no partial batch.
func (Service) DecodeProperties(ctx context.Context, request schemaext.PropertyDecodeRequest) ([]schemaext.Value, error) {
	if err := validateRequest(ctx, request.Target, request.Format); err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, 0, len(request.Fragments))
	for _, fragment := range request.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fragment.Kind != crdbschema.RowTTLKind {
			return nil, fmt.Errorf("%w: CockroachDB source kind %q", schemaext.ErrInvalidValue, fragment.Kind)
		}
		value, err := crdbschema.DecodeDeclared(maps.Clone(fragment.Properties))
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// EncodeProperties writes every set parameter of a valid declaration. A flag
// is written as true; a false flag is the engine's default and is omitted.
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
		declared, ok := value.(*crdbschema.DesiredRowTTL)
		if !ok {
			return nil, fmt.Errorf("%w: CockroachDB source expected a desired row-level TTL, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := crdbschema.ValidateDesired(declared); err != nil {
			return nil, err
		}
		fragment := schemaext.PropertyFragment{Kind: crdbschema.RowTTLKind, Properties: make(map[string]string)}
		for _, parameter := range declared.Policy.Parameters() {
			fragment.Properties[parameter.Name] = parameter.Value
		}
		result = append(result, fragment)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Coverage is the knowledge a source format that carries platform properties
// holds: it could have declared a row-level TTL on any of its tables, so a
// table without one requests no TTL. A format without platform properties
// must not enroll it, and its tables leave an existing policy unmanaged.
func Coverage() (schemaext.Coverage, error) {
	return crdbschema.RowTTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}
