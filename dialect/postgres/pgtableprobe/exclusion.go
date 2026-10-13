package pgtableprobe

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtablerender"
	"ptah.run/internal/pgexclude"
	"ptah.run/internal/sqlident"
)

// ExclusionService attaches PostgreSQL's expression spelling to exclusion
// declarations whose constraints the current read observes. Its zero value is
// concurrent-safe. It performs probes only through a rolled-back session.
type ExclusionService struct{}

// NormalizeObjects clears input normalization and probes matching observations.
// Without an isolated session, or when PostgreSQL refuses the declaration, the
// returned declaration has no normalization fact. Other errors and cancellation
// discard the whole batch. Source expressions and coverage remain unchanged.
func (ExclusionService) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: EXCLUDE normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.Postgres {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: EXCLUDE normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	objects, err := request.Desired.Objects.All()
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := schemaext.NormalizationResult{Complete: true, Desired: request.Desired}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return schemaext.NormalizationResult{}, err
		}
		normalized, err := normalizeExclusionObject(ctx, request, object)
		if err != nil {
			return schemaext.NormalizationResult{}, fmt.Errorf("normalize EXCLUDE %s: %w", object.Ref, err)
		}

		result.Desired.Objects, err = result.Desired.Objects.Replace(normalized)
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	return result, nil
}

func normalizeExclusionObject(ctx context.Context, request schemaext.NormalizationRequest, object schemaext.Object) (schemaext.Object, error) {
	value, ok := object.Value.(*pgtable.DesiredExclusion)
	if !ok {
		return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE normalization requires exclusion declarations", schemaext.ErrInvalidValue)
	}
	if err := pgtable.ValidateExclusionRef(object.Ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := pgtable.ValidateDesiredExclusion(value); err != nil {
		return schemaext.Object{}, err
	}
	value.Normalized = nil
	current, present, err := request.Current.Objects.Get(object.Ref)
	if err != nil {
		return schemaext.Object{}, err
	}
	if present {
		observation, ok := current.Value.(*pgtable.ObservedExclusion)
		if !ok {
			return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE normalization requires exclusion observations", schemaext.ErrInvalidValue)
		}
		if err := pgtable.ValidateObservedExclusion(observation); err != nil {
			return schemaext.Object{}, err
		}
	}
	if present && request.Session != nil {
		value.Normalized, err = normalizeExclusion(ctx, request.Session, object.Ref, value)
		if err != nil {
			return schemaext.Object{}, err
		}
	}
	return object, nil
}

func normalizeExclusion(ctx context.Context, session schemaext.ProbeSession, subject objectidentity.ID, value *pgtable.DesiredExclusion) (*pgtable.NormalizedExclusion, error) {
	clause, err := pgtablerender.ExclusionSQL("", value)
	if err != nil {
		return nil, err
	}
	// Refuse statement separators and incomplete clauses before the probe starts.
	if _, err := pgexclude.Parse(clause); err != nil {
		return nil, err
	}
	table := sqlident.Qualified(platform.Postgres, sqlident.UnquoteDoubleQuoted(subject.Schema.Source), sqlident.UnquoteDoubleQuoted(subject.Parent.Source))
	temporary := sqlident.Qualified(platform.Postgres, "pg_temp", sqlident.UnquoteDoubleQuoted(subject.Parent.Source))
	var definition string
	ran, err := session.WithRolledBackTransaction(ctx, "normalize exclusion constraint", func(ctx context.Context, tx *sql.Tx) error {
		// #nosec G202 -- Identifiers are quoted and the owner validates one complete EXCLUDE clause before the rolled-back probe.
		statement := "CREATE TEMPORARY TABLE " + temporary + " (LIKE " + table + ", " + clause + ")"
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return nil
		}
		return tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::regclass AND contype='x'`, temporary).Scan(&definition)
	})
	if err != nil {
		return nil, err
	}
	if !ran || definition == "" {
		return nil, ctx.Err()
	}
	normalized, err := pgexclude.Parse(definition)
	if err != nil {
		return nil, err
	}
	return &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(value.Exclusion), Elements: normalized.Elements, WhereCondition: normalized.WhereCondition}, ctx.Err()
}
