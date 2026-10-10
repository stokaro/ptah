// Package mssqlrelation describes the objects a captured SQL Server security
// policy depends on. It reads captured values only: no database, no source
// file.
package mssqlrelation

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Service reports the tables and functions a policy's predicates bind. Its
// zero value is ready for concurrent use.
//
// A policy belongs to its schema and has no table parent, so every table it
// binds is a dependency, and so is every predicate function. The record is
// exhaustive only when every argument is a single column or literal: an
// argument expression may call functions of its own, which are not parsed
// out of it.
type Service struct{}

// DescribeRelations returns one record per value, in input order, with each
// dependency once, tables before functions.
func (Service) DescribeRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	if ctx == nil {
		return schemaext.RelationResult{}, fmt.Errorf("%w: relation discovery requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.RelationResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return schemaext.RelationResult{}, fmt.Errorf("%w: SQL Server security policy relation discovery on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true, Values: make([]schemaext.ValueRelations, 0, len(request.Values))}
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return schemaext.RelationResult{}, err
		}
		var predicates []mssqlschema.Predicate
		switch typed := value.Value.(type) {
		case *mssqlschema.DesiredSecurityPolicy:
			predicates = typed.Predicates
		case *mssqlschema.ObservedSecurityPolicy:
			predicates = typed.Predicates
		default:
			return schemaext.RelationResult{}, fmt.Errorf("%w: unexpected security policy relation value %T", schemaext.ErrInvalidValue, value.Value)
		}
		result.Values = append(result.Values, describe(builder, value.Subject, predicates))
	}
	return result, ctx.Err()
}

func describe(builder objectidentity.Builder, subject schemaext.RelationSubject, predicates []mssqlschema.Predicate) schemaext.ValueRelations {
	record := schemaext.ValueRelations{Subject: subject, Complete: true}
	var tables, functions []objectidentity.ID
	seen := make(map[objectidentity.Key]bool)
	add := func(list *[]objectidentity.ID, ref objectidentity.ID) {
		if !seen[ref.Key()] {
			seen[ref.Key()] = true
			*list = append(*list, ref)
		}
	}
	for _, predicate := range mssqlschema.SortedPredicates(predicates) {
		add(&tables, builder.TableParts(predicate.Table.Schema, predicate.Table.Name))
		function := builder.TableParts(predicate.Function.Schema, predicate.Function.Name)
		function.Kind = objectidentity.KindFunction
		add(&functions, function)
		for _, argument := range predicate.Arguments {
			if record.Complete && !mssqlschema.SimpleArgument(argument) {
				record.Complete = false
				record.Reason = fmt.Sprintf("the argument %s of the predicate on %s is an expression, "+
					"and the functions an expression calls are not parsed out of it", argument, predicate.Table)
			}
		}
	}
	record.Dependencies = slices.Concat(tables, functions)
	return record
}
