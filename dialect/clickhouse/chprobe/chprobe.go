// Package chprobe asks a connected ClickHouse server to spell the filters of
// declared row policies the way it stores them, so a comparison holds the same
// spelling on both sides. A probe is a read-only query run inside the session
// the host gives it; it creates nothing.
package chprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// filterQuery formats a filter as the server formats the one it stores. The
// renderer writes `USING (<filter>)`, and the probe puts the same text in
// parentheses, because 26.9 keeps parentheses the whole condition is written
// in and 24.10 drops them.
const filterQuery = `SELECT formatQuerySingleLine(concat('SELECT (', ?, ')'))`

// Service attaches the server's own spelling of each declared row policy
// filter whose policy the database also holds. Its zero value is ready for
// concurrent use.
//
// ClickHouse stores a filter reformatted: `tenant=1` reads back as
// `tenant = 1`, `id>1 AND tenant<3` as `(id > 1) AND (tenant < 3)`, and
// `id BETWEEN 1 AND 3` as `(id >= 1) AND (id <= 3)`, and the release lines
// format parentheses differently. Measured on 24.10.4.191 and 26.9.14.10,
// formatQuerySingleLine over `SELECT (<filter>)` returns exactly what
// system.row_policies stores for a policy created with `USING (<filter>)`.
//
// Only policies the database holds are probed: one being created carries its
// declaration into the statement unchanged, and one being dropped has no
// declaration left to spell.
type Service struct{}

type probe struct {
	object schemaext.Object
	value  *chschema.DesiredRowPolicy
}

// NormalizeObjects returns every declared policy, with NormalizedFilter
// attached to those the server spelled. A session that could not run the
// probe, and a filter the server could not parse, leave the declaration
// without it, which a comparison treats as unanswered and compares by tokens.
func (Service) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: ClickHouse row policy normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
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
	ran, err := request.Session.WithRolledBackTransaction(ctx, "resolve row policy filters", func(ctx context.Context, tx *sql.Tx) error {
		for i, probe := range probes {
			if err := ctx.Err(); err != nil {
				return err
			}
			var formatted string
			if err := tx.QueryRowContext(ctx, filterQuery, *probe.value.Filter).Scan(&formatted); err != nil {
				// The declaration is the author's, and a filter the server
				// cannot parse fails later with a better message. Unresolved
				// is the honest answer here.
				continue
			}
			spelled, found := strings.CutPrefix(formatted, "SELECT ")
			if found && strings.TrimSpace(spelled) != "" {
				normalized[i], resolved[i] = spelled, true
			}
		}
		return nil
	})
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !ran {
		return result, nil
	}
	for i, probe := range probes {
		if !resolved[i] {
			continue
		}
		value := probe.value.Copy()
		value.NormalizedFilter = new(normalized[i])
		result.Desired.Objects, err = result.Desired.Objects.Replace(schemaext.Object{Ref: probe.object.Ref, Value: value})
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	return result, ctx.Err()
}

// heldProbes selects the declarations with a filter whose policy the database
// holds, compared under the request's identifier rules.
func heldProbes(request schemaext.NormalizationRequest) ([]probe, error) {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range request.Current.Objects.Refs() {
		if ref.Kind == objectidentity.Kind(chschema.RowPolicyKind) {
			held[chschema.ResolveRowPolicyRef(request.Identifiers, ref).Key()] = true
		}
	}
	objects, err := request.Desired.Objects.All()
	if err != nil {
		return nil, err
	}
	var probes []probe
	for _, object := range objects {
		value, ok := object.Value.(*chschema.DesiredRowPolicy)
		if !ok {
			return nil, fmt.Errorf("%w: unexpected ClickHouse row policy declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
		if value.Filter != nil && held[chschema.ResolveRowPolicyRef(request.Identifiers, object.Ref).Key()] {
			probes = append(probes, probe{object: object, value: value})
		}
	}
	return probes, nil
}
