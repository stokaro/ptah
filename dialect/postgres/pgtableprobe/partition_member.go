package pgtableprobe

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/sqlident"
)

// PartitionMemberService normalizes the declared bound of a partition the database
// holds. PostgreSQL stores a bound as values of the parent's key types and prints
// them back in its own spelling, so `'2026-01-01'` on a timestamptz key reads as
// `'2026-01-01 00:00:00+00'`. The service creates temporary copies of the parent
// and of the partition, reads pg_get_expr, then rolls back. A bound on a timestamptz
// key reads in the session's time zone, as the inspection that reads the live bound
// does. Its zero value is concurrent-safe.
type PartitionMemberService struct{}

// NormalizeFacets returns independent declarations with fresh normalization facts.
// Missing sessions, unavailable transactions and refused declarations leave no fact.
// Provider errors and cancellation discard the whole result.
func (PartitionMemberService) NormalizeFacets(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	if ctx == nil {
		return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition bound normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	if !pgtable.SupportsPartition(request.Target) {
		return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition bound normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := schemaext.FacetNormalizationResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(request.Desired.Records)
	for i, record := range request.Desired.Records {
		value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](record.Values, pgtable.PartitionMemberKind)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		if !found {
			continue
		}
		if record.Subject.Kind != objectidentity.KindTable {
			return schemaext.FacetNormalizationResult{}, fmt.Errorf("%w: partition bound normalization requires a table", schemaext.ErrInvalidValue)
		}
		if err := pgtable.ValidateDesiredPartitionMember(value); err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		cloned := &pgtable.DesiredPartitionMember{PartitionMember: value.PartitionMember}
		held := slices.ContainsFunc(request.Owners, func(parent schemaext.ParentState) bool {
			return parent.Subject.Key() == record.Subject.Key() && parent.Current && parent.Desired
		})
		if held && request.Session != nil {
			normalized, err := normalizePartitionBound(ctx, request.Session, value.PartitionMember)
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

func normalizePartitionBound(ctx context.Context, session schemaext.ProbeSession, member pgtable.PartitionMember) (*pgtable.NormalizedPartitionBound, error) {
	// The owner parser rejects statement separators and incomplete values before
	// any text enters a transaction. Rollback must not depend on accepting
	// arbitrary statement text as a bound.
	bound, err := pgtable.ParsePartitionBound(member.Bound)
	if err != nil {
		return nil, err
	}
	parent := sqlident.Qualified(platform.Postgres, member.ParentSchema, member.Parent)
	var definition sql.NullString
	ran, err := session.WithRolledBackTransaction(ctx, "normalize table partition bound", func(ctx context.Context, tx *sql.Tx) error {
		var key sql.NullString
		if err := tx.QueryRowContext(ctx, "SELECT pg_get_partkeydef($1::regclass)", parent).Scan(&key); err != nil || !key.Valid {
			// An unreadable parent leaves no fact, and the comparison answers for
			// the bound as written.
			return nil
		}
		for _, statement := range []string{
			// #nosec G202 -- Identifiers are quoted; the key is the server's own text, and the owner parsed the bound.
			"CREATE TABLE pg_temp.ptah_partition_bound_parent (LIKE " + parent + ") PARTITION BY " + key.String,
			// #nosec G202 -- The owner parsed the bound as one clause before the rolled-back probe.
			"CREATE TABLE pg_temp.ptah_partition_bound_probe PARTITION OF pg_temp.ptah_partition_bound_parent " + bound,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				// A bound the server refuses leaves no fact, and the apply reports
				// the server's answer.
				return nil
			}
		}
		return tx.QueryRowContext(ctx, "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = 'pg_temp.ptah_partition_bound_probe'::regclass").Scan(&definition)
	})
	if err != nil {
		return nil, err
	}
	if !ran || !definition.Valid || definition.String == "" {
		return nil, ctx.Err()
	}
	return &pgtable.NormalizedPartitionBound{SourceKey: pgtable.PartitionBoundKey(member.Bound), Key: pgtable.PartitionBoundKey(definition.String)}, ctx.Err()
}
