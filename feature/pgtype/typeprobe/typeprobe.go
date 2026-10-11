// Package typeprobe asks a connected PostgreSQL server to spell the CHECK and
// default of declared domains for the owner of package pgtype, so a
// comparison holds the server's spelling on both sides. Every probe runs
// inside a rolled-back transaction of the session it is given, on a domain in
// the session's temporary schema; nothing it creates survives the call.
package typeprobe

import (
	"context"
	"fmt"
	"strconv"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/sqlident"
)

// Service attaches the server's spelling of each declared domain the database
// also holds. Its zero value is ready for concurrent use.
//
// PostgreSQL stores a domain's CHECK and default as parse trees and prints
// them back from the tree: parentheses appear, literals gain casts, and `IN`
// becomes `= ANY (ARRAY[...])`. A declaration compared as written would never
// agree with the catalog, so a changed constraint used to go unnoticed and a
// plan reported a synced schema over a database still enforcing the old rule
// (stokaro/ptah#1717). The declaration is put through the same server: a
// domain over the declared base type, with the declared CHECK and default, is
// created in pg_temp and its stored expressions are read back.
//
// A probe the server refuses leaves its declaration without an answer and is
// not an error: an expression that does not parse, or a base type the session
// cannot see, leaves the CHECK and the default uncompared. Only domains the
// database holds and that state a CHECK or a default are probed; one being
// created carries its declaration into CREATE DOMAIN unchanged.
type Service struct{}

// probe is one declared domain to ask about.
type probe struct {
	object schemaext.Object
	value  *pgtype.DesiredDomain
}

// NormalizeObjects returns every declared domain, with Normalized attached to
// those the server answered for. A session that could not open an isolated
// transaction answers nothing, and the declarations come back unchanged, as
// does a declaration whose answer the model cannot hold.
func (Service) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if ctx == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: PostgreSQL domain normalization on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if request.Session == nil {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: normalization requires a probe session", schemaext.ErrInvalidValue)
	}
	probes, asked, err := heldProbes(request)
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := schemaext.NormalizationResult{Complete: true, Desired: request.Desired}
	if len(probes) == 0 {
		return result, nil
	}
	answers, err := dbexprprobe.ProbeDomainExpressions(ctx, request.Session, asked)
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	for i, probe := range probes {
		answer, found := answers[asked[i].Key]
		if !found || !answer.Resolved {
			continue
		}
		value := probe.value.Copy()
		value.Normalized = &pgtype.NormalizedDomain{Check: answer.Check, Default: answer.Default}
		// An answer the model cannot hold is a server this probe does not
		// understand, which is unanswered rather than a failed comparison.
		if pgtype.ValidateDesiredDomain(value) != nil {
			continue
		}
		result.Desired.Objects, err = result.Desired.Objects.Replace(schemaext.Object{Ref: probe.object.Ref, Value: value})
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
	}
	return result, ctx.Err()
}

// heldProbes selects the declared domains the database holds, by identity,
// that state a CHECK or a default, beside the question each asks the server.
// The default is asked as SQL: an expression as written, and a literal as a
// quoted string, which the server casts to the base type.
func heldProbes(request schemaext.NormalizationRequest) ([]probe, []dbexprprobe.DomainExpressionProbe, error) {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range request.Current.Objects.Refs() {
		held[ref.Key()] = true
	}
	objects, err := request.Desired.Objects.All()
	if err != nil {
		return nil, nil, err
	}
	var (
		probes []probe
		asked  []dbexprprobe.DomainExpressionProbe
	)
	for _, object := range objects {
		value, ok := object.Value.(*pgtype.DesiredDomain)
		if !ok {
			return nil, nil, fmt.Errorf("%w: unexpected domain declaration %T", schemaext.ErrInvalidValue, object.Value)
		}
		if err := pgtype.ValidateRef(object.Ref); err != nil {
			return nil, nil, err
		}
		defaultSQL := value.DefaultExpr
		if defaultSQL == "" && value.Default != "" {
			defaultSQL = sqlident.StringLiteral(value.Default)
		}
		if !held[object.Ref.Key()] || value.Check == "" && defaultSQL == "" {
			continue
		}
		probes = append(probes, probe{object: object, value: value})
		asked = append(asked, dbexprprobe.DomainExpressionProbe{Key: strconv.Itoa(len(asked)), BaseType: value.BaseType,
			Check: value.Check, Default: defaultSQL})
	}
	return probes, asked, nil
}
