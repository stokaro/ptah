// Package chprepare resolves ClickHouse storage settings and column key membership
// before shared comparison. Key clauses belong to the dialect, not to the common
// comparator.
package chprepare

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chkey"
)

// Service resolves typed table/index settings and desired column primary-key flags.
// It keeps declared facets and all observed state unchanged. The zero value is
// usable and safe for concurrent calls; no live server is consulted.
type Service struct{}

// PrepareTables returns independent captures whose column key flags match the
// declared or retained ClickHouse key. Unknown state required by an omitted
// typed setting returns chresolve.ErrUnknownCurrent. Invalid input, provider
// errors, and cancellation return no partial result. Only ClickHouse is accepted.
func (Service) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if ctx == nil {
		return schemapreparation.Result{}, fmt.Errorf("%w: preparation requires a context", schemapreparation.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.ClickHouse {
		return schemapreparation.Result{}, fmt.Errorf("%w: ClickHouse preparation target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request = request.Clone()
	for i := range request.Tables {
		if len(request.Tables[i].ResolvedFacets) != 0 {
			return schemapreparation.Result{}, fmt.Errorf("%w: preparation input already carries resolved facets", schemapreparation.ErrInvalid)
		}
		if err := prepareTable(&request.Tables[i]); err != nil {
			return schemapreparation.Result{}, err
		}
		if err := prepareIndexes(&request.Tables[i], request.Identifiers); err != nil {
			return schemapreparation.Result{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
}

func prepareTable(table *schemapreparation.Table) error {
	names := make([]string, 0, len(table.Desired.Fields))
	for _, field := range table.Desired.Fields {
		names = append(names, field.Name)
	}
	keys, declared := chkey.PrimaryKeyColumns(table.Desired.Table, names)
	value, typed, err := schemaext.FacetAs[*chschema.DesiredTable](table.Desired.Table.Facets, chschema.TableKind)
	if err != nil {
		return err
	}
	if typed {
		if err := refuseStorageOverrides(table.Desired.Table.Overrides); err != nil {
			return err
		}
		observed, _, err := schemaext.FacetAs[*chschema.ObservedTable](table.Current.Table.Facets, chschema.TableKind)
		if err != nil {
			return err
		}
		if table.CurrentKnowledge.State != schemaext.Complete ||
			table.Current.FeatureCoverage.Lookup(chschema.TableKind, table.Subject).State != schemaext.Complete {
			observed = nil
		}
		resolved, err := chresolve.Table(chresolve.Request{
			Desired: value, Current: observed, BaseEngine: table.Desired.Table.Engine, Creating: table.CurrentKnowledge.State == schemaext.Absent, CommonKey: chkey.CommonColumns(table.Desired),
		})
		if err != nil {
			return err
		}
		facets, err := schemaext.NewFacets(&resolved.Prepared)
		if err != nil {
			return err
		}
		table.ResolvedFacets = append(table.ResolvedFacets, schemaext.FacetRecord{Subject: table.Subject, Values: facets})
		keys, declared = chkey.ReferencedColumns(resolved.Prepared.PrimaryKey.Value, names), true
	}
	if !declared {
		return nil
	}
	for i := range table.Desired.Fields {
		table.Desired.Fields[i].Primary = keys[table.Desired.Fields[i].Name]
	}
	table.ColumnPrimaryKeysPrepared = true
	return nil
}

// Both preparation paths consume decoded settings. Sharing this check prevents
// direct provider calls from silently dropping an unconsumed source property.
func refuseStorageOverrides(overrides map[string]map[string]string) error {
	for target, properties := range overrides {
		if platform.NormalizeDialect(target) != platform.ClickHouse {
			continue
		}
		for _, key := range chresolve.StorageOptionKeys() {
			if _, found := properties[strings.ToLower(key)]; found {
				return fmt.Errorf("%w: ClickHouse table setting %s must be decoded before preparation", schemaext.ErrInvalidValue, key)
			}
		}
	}
	return nil
}
