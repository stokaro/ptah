// Package pgtableprobe captures PostgreSQL partition-key normalization inside
// rolled-back probe transactions. It attaches facts to owner declarations and
// leaves catalog inspection, comparison and migration execution to their callers.
package pgtableprobe

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtablerender"
	"ptah.run/internal/pgtablesource"
	"ptah.run/internal/sqlident"
)

// Service normalizes declared partition keys of existing tables. It creates a
// temporary table with the current table's column types and collations, reads
// pg_get_partkeydef, then rolls back. Its zero value is concurrent-safe.
type Service struct{}

// NormalizeFacets returns independent declarations with fresh normalization
// facts. Missing sessions, unavailable transactions and refused declarations
// leave no fact. Provider errors and cancellation discard the whole result.
func (Service) NormalizeFacets(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	if ctx == nil {
		return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	if !pgtable.SupportsPartition(request.Target) {
		return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(request.Desired.Records)
	for i, record := range request.Desired.Records {
		value, found, err := schemaext.FacetAs[*pgtable.DesiredPartition](record.Values, pgtable.PartitionKind)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		if !found {
			continue
		}
		if record.Subject.Kind != objectidentity.KindTable {
			return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition normalization requires a table", schemaext.ErrInvalidValue)
		}
		if err := pgtable.ValidateDesiredPartition(value); err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		cloned := &pgtable.DesiredPartition{Type: value.Type, Parts: slices.Clone(value.Parts)}
		held := slices.ContainsFunc(request.Owners, func(parent schemaext.ParentState) bool {
			return parent.Subject.Key() == record.Subject.Key() && parent.Current && parent.Desired
		})
		if held && request.Session != nil {
			normalized, err := normalizePartition(ctx, request.Session, record.Subject, cloned)
			if err != nil {
				return schemaext.FacetNormalizationResult{}, err
			}
			cloned.Normalized = normalized
		}
		result.Desired.Records[i].Values, err = record.Values.Replace(cloned)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	return result, nil
}

func normalizePartition(ctx context.Context, session schemaext.ProbeSession, subject objectidentity.ID, value *pgtable.DesiredPartition) (*pgtable.NormalizedPartition, error) {
	key, err := pgtablerender.PartitionSQL(value)
	if err != nil {
		return nil, err
	}
	// The owner parser rejects statement separators and incomplete expressions
	// before any text enters a transaction. Rollback must not depend on accepting
	// arbitrary statement text as a key expression.
	if _, err := pgtablesource.ParsePartitionSQL(key); err != nil {
		return nil, err
	}
	table := sqlident.Qualified("postgres", subject.Schema.Source, subject.Name.Source)
	var definition string
	ran, err := session.WithRolledBackTransaction(ctx, "normalize table partition key", func(ctx context.Context, tx *sql.Tx) error {
		// #nosec G202 -- Identifiers are quoted; the owner parses the key as one clause before the rolled-back probe.
		statement := "CREATE TABLE pg_temp.ptah_partition_probe (LIKE " + table + ") PARTITION BY " + key
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return nil
		}
		if err := tx.QueryRowContext(ctx, "SELECT pg_get_partkeydef('pg_temp.ptah_partition_probe'::regclass)").Scan(&definition); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !ran || definition == "" {
		return nil, ctx.Err()
	}
	normalized, err := pgtablesource.ParsePartitionSQL(definition)
	if err != nil {
		return nil, err
	}
	return &pgtable.NormalizedPartition{
		SourceKey: pgtable.PartitionComparisonKey(&pgtable.Partition{Type: value.Type, Parts: value.Parts}),
		Key:       pgtable.PartitionComparisonKey(&pgtable.Partition{Type: normalized.Type, Parts: normalized.Parts}),
	}, ctx.Err()
}
