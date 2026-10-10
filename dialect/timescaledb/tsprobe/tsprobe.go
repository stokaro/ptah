// Package tsprobe asks a connected TimescaleDB server to normalize declared
// continuous aggregates, so a comparison holds the same spelling on both
// sides. Every probe runs inside a rolled-back transaction of the session it
// is given; nothing it creates survives the call.
package tsprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/sqlident"
)

// aggregateCatalog is the view a probe's definition is read back from. It
// exists only where the extension is installed.
const aggregateCatalog = "timescaledb_information.continuous_aggregates"

// Service attaches the server's own spelling of each declared aggregate body
// that the database also holds. Its zero value is ready for concurrent use.
//
// TimescaleDB rewrites a definition before storing it, and a read-back
// differs from the declaration that produced it: an interval literal becomes an
// interval cast, a column reference gains quotes, and a GROUP BY key written by
// its output name comes back as the whole expression that name stood for.
// Comparing a declaration against that is comparing two languages, and acting
// on the difference would drop and recreate an aggregate that had not changed.
//
// The declaration is put through the same rewrite: an aggregate with the same
// body is created under a probe name inside the session's rolled-back
// transaction and its stored definition is read back. Measured on 2.29.2 /
// PostgreSQL 17.11, the probe's stored definition is identical to the real
// aggregate's, and after the rollback the catalog holds neither the probe nor
// a materialization hypertable for it.
//
// Only aggregates the database holds are probed: one being created carries its
// declaration into the CREATE statement unchanged, and one being dropped has no
// declaration left to normalize.
type Service struct{}

type probe struct {
	object schemaext.Object
	value  *tsschema.DesiredContinuousAggregate
}

// NormalizeObjects returns every declared aggregate, with Normalized attached
// to those the server rewrote. A server without the extension, a session that
// could not open an isolated transaction, and a body the server refused leave
// the declaration without it, which a comparison treats as unanswered.
func (Service) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: TimescaleDB normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.Session == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a probe session", schemaext.ErrInvalidValue)
	}
	probes, err := heldProbes(request)
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := schemaext.NormalizationResult{Complete: true, Desired: request.Desired}
	if len(probes) == 0 {
		return result, nil
	}
	normalized := make([]string, len(probes))
	resolved := make([]bool, len(probes))
	installed := false
	ran, err := request.Session.WithRolledBackTransaction(ctx, "resolve continuous aggregate bodies",
		func(ctx context.Context, tx *sql.Tx) error {
			const query = `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`
			if err := tx.QueryRowContext(ctx, query).Scan(&installed); err != nil {
				return fmt.Errorf("resolve continuous aggregate bodies: read extension list: %w", err)
			}
			if !installed {
				return nil
			}
			for i, probe := range probes {
				body, ok, err := normalizeOne(ctx, tx, i, probe)
				if err != nil {
					return err
				}
				normalized[i], resolved[i] = body, ok
			}
			return nil
		})
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !ran || !installed {
		return result, nil
	}
	for i, probe := range probes {
		if !resolved[i] {
			continue
		}
		value := probe.value.Copy()
		value.Normalized = &tsschema.NormalizedBody{Body: normalized[i]}
		result.Desired.Objects, err = result.Desired.Objects.Replace(schemaext.Object{Ref: probe.object.Ref, Value: value})
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	return result, ctx.Err()
}

// heldProbes selects the declarations whose aggregate the database holds,
// compared under the request's identifier rules.
func heldProbes(request schemaext.NormalizationRequest) ([]probe, error) {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range request.Current.Objects.Refs() {
		held[rekey(request, ref).Key()] = true
	}
	objects, err := request.Desired.Objects.All()
	if err != nil {
		return nil, err
	}
	var probes []probe
	for _, object := range objects {
		value, ok := object.Value.(*tsschema.DesiredContinuousAggregate)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected continuous aggregate declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
		if held[rekey(request, object.Ref).Key()] && tsschema.FoldBody(value.Body) != "" {
			probes = append(probes, probe{object: object, value: value})
		}
	}
	return probes, nil
}

func rekey(request schemaext.NormalizationRequest, ref objectidentity.ID) objectidentity.ID {
	return tsschema.ContinuousAggregateRefWith(request.Identifiers, tsschema.AuthoredSchema(ref), ref.Name.Source)
}

// normalizeOne creates one probe aggregate and reads its stored definition back.
//
// Each probe runs inside its own savepoint. A body the server refuses aborts
// the transaction, and without the savepoint the first refused declaration would
// take every later probe with it. Measured: after ROLLBACK TO SAVEPOINT the
// session answers the next query normally.
//
// WITH NO DATA is a requirement here rather than an optimization: without it
// the server answers `CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside
// a transaction block`. The probe sends the declared option too, because the
// catalog reports it beside the definition and a probe naming a value the
// declaration did not would answer for a different declaration.
func normalizeOne(ctx context.Context, tx *sql.Tx, index int, probe probe) (string, bool, error) {
	name := fmt.Sprintf("ptah_cagg_probe_%d", index)
	qualified := name
	if schema := tsschema.AuthoredSchema(probe.object.Ref); strings.TrimSpace(schema) != "" {
		qualified = sqlident.Quote(platform.Postgres, sqlident.UnquoteDoubleQuoted(schema)) + "." + name
	}
	options := "timescaledb.continuous"
	if probe.value.MaterializedOnly != nil {
		options += fmt.Sprintf(", timescaledb.materialized_only = %t", *probe.value.MaterializedOnly)
	}
	statement := fmt.Sprintf("CREATE MATERIALIZED VIEW %s WITH (%s) AS\n%s\nWITH NO DATA", qualified, options, tsschema.FoldBody(probe.value.Body))
	const savepoint = "ptah_cagg_probe"
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return "", false, fmt.Errorf("resolve continuous aggregate bodies: savepoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return "", false, fmt.Errorf("resolve continuous aggregate bodies: roll back to savepoint after %q: %w", probe.object.Ref, rollbackErr)
		}
		// The declaration is the author's, and refusing it here would fail a
		// comparison over an aggregate the server refuses later anyway, with a
		// worse message. Unresolved is the honest answer.
		return "", false, nil
	}
	query := `SELECT c.view_definition FROM ` + aggregateCatalog + ` c WHERE c.view_name = $1`
	var stored string
	if err := tx.QueryRowContext(ctx, query, name).Scan(&stored); err != nil {
		return "", false, fmt.Errorf("resolve continuous aggregate bodies: read back %q: %w", probe.object.Ref, err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return "", false, fmt.Errorf("resolve continuous aggregate bodies: release probe: %w", err)
	}
	return strings.TrimSpace(stored), true, nil
}
