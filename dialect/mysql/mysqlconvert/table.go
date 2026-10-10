package mysqlconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TableService converts table options. An observation becomes a declaration
// of the character set the table holds; a declaration becomes the options a
// read of a table created from it reports.
type TableService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the mysql and mariadb targets and opposite desired and observed
// representations. Invalid values or directions wrap
// schemaext.ErrInvalidValue; other targets wrap ptaherr.ErrUnsupportedDialect.
// Any error, including cancellation, returns no partial result.
func (TableService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return optionConversion[*mysqlschema.DesiredTable, *mysqlschema.ObservedTable]{
		name: "MySQL table options", observe: anyTarget((*mysqlschema.DesiredTable).Observed),
		validate: mysqlschema.ValidateObservedTable, declare: (*mysqlschema.ObservedTable).Desired,
	}.convert(ctx, request)
}

// IndexService converts index options, as [TableService] converts table
// options.
type IndexService struct{}

// ConvertFeatures converts an ordered batch, as [TableService.ConvertFeatures]
// does.
func (IndexService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return optionConversion[*mysqlschema.DesiredIndex, *mysqlschema.ObservedIndex]{
		name: "MySQL index options", observe: anyTarget((*mysqlschema.DesiredIndex).Observed),
		validate: mysqlschema.ValidateObservedIndex, declare: (*mysqlschema.ObservedIndex).Desired,
	}.convert(ctx, request)
}

// IndexBlockSizeService converts index block-size hints. An observation
// becomes a declaration of the hint it holds; a declaration becomes the
// observation a read of an index created from it on the target reports,
// which on MySQL does not retain the hint (see
// [mysqlschema.DesiredIndexBlockSize.Observed]).
type IndexBlockSizeService struct{}

// ConvertFeatures converts an ordered batch, as [TableService.ConvertFeatures]
// does.
func (IndexBlockSizeService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return optionConversion[*mysqlschema.DesiredIndexBlockSize, *mysqlschema.ObservedIndexBlockSize]{
		name: "MySQL index block size", observe: (*mysqlschema.DesiredIndexBlockSize).Observed,
		validate: mysqlschema.ValidateObservedIndexBlockSize, declare: (*mysqlschema.ObservedIndexBlockSize).Desired,
	}.convert(ctx, request)
}

// optionConversion converts one kind of options between their declaration D
// and their observation O. observe takes the target, since what a read
// reports for a declaration can depend on the engine.
type optionConversion[D, O schemaext.Value] struct {
	name     string
	observe  func(D, string) (O, error)
	validate func(O) error
	declare  func(O) D
}

func (c optionConversion[D, O]) convert(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return nil, fmt.Errorf("%w: %s conversion on %q", ptaherr.ErrUnsupportedDialect, c.name, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid %s conversion direction", schemaext.ErrInvalidValue, c.name)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := c.one(request.From, request.Target, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

func (c optionConversion[D, O]) one(from schemaext.Representation, target string, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		desired, ok := value.(D)
		if !ok {
			return nil, fmt.Errorf("%w: expected desired %s, got %T", schemaext.ErrInvalidValue, c.name, value)
		}
		return c.observe(desired, target)
	}
	observed, ok := value.(O)
	if !ok {
		return nil, fmt.Errorf("%w: expected observed %s, got %T", schemaext.ErrInvalidValue, c.name, value)
	}
	if err := c.validate(observed); err != nil {
		return nil, err
	}
	return c.declare(observed), nil
}

// anyTarget adapts a projection that reads the same on every target.
func anyTarget[D, O any](observe func(D) (O, error)) func(D, string) (O, error) {
	return func(declared D, _ string) (O, error) { return observe(declared) }
}
