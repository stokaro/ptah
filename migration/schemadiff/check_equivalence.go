package schemadiff

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/internal/compare"
)

// withTargetCheckDecisions returns opts carrying the answers of a target that
// decides CHECK equivalence itself (see [decideCheckEquivalence]), and opts
// itself for a target that does not. opts is not modified.
//
// The comparison asks it with the desired state it compares the constraints
// of, after the target's lowering. A CHECK may exist only from the lowering
// on: an engine that builds an enum column as a TEXT column under a CHECK
// receives that CHECK from the lowering, and asked about the declaration
// before it, the target never sees the CHECK. The comparison then falls back
// to the texts, reads Spanner's `kind = ANY (ARRAY['a'::text, 'b'::text])`
// against `kind IN ('a', 'b', 'c')` as a rewrite it cannot judge, and plans
// nothing for a changed list of values.
func withTargetCheckDecisions(
	ctx context.Context,
	opts *config.CompareOptions,
	service schemacomparison.Checks,
	desired *schemamodel.Database,
	database *catalog.Database,
	caps capability.Capabilities,
	semantics identifier.Semantics,
) (*config.CompareOptions, error) {
	if service == nil {
		return opts, nil
	}
	checks, err := decideCheckEquivalence(ctx, service, desired, database, opts.Dialect, caps, semantics)
	if err != nil || len(checks) == 0 {
		return opts, err
	}
	decided := *opts
	decided.CheckExpressions = checks
	return &decided, nil
}

// decideCheckEquivalence asks a target that decides CHECK equivalence itself
// about every CHECK the comparison compares that the database also holds, and
// records each answer: the held CHECK where the target calls the two one
// constraint, and the declaration marked as differing where it does not. The
// comparison then compares no CHECK of the target by its text.
func decideCheckEquivalence(
	ctx context.Context,
	service schemacomparison.Checks,
	desired *schemamodel.Database,
	database *catalog.Database,
	target string,
	caps capability.Capabilities,
	semantics identifier.Semantics,
) (map[string]config.CheckExpression, error) {
	if desired == nil || database == nil {
		return nil, nil
	}
	held := make(map[string]catalog.Constraint, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "CHECK") && constraint.CheckClause != nil && strings.TrimSpace(*constraint.CheckClause) != "" {
			held[exprkey.CheckParts(semantics, constraint.Schema, constraint.TableName, constraint.Name)] = constraint
		}
	}
	request := schemacomparison.ChecksRequest{Target: target, Identifiers: semantics, Capabilities: caps, Current: database}
	var keys []string
	for _, constraint := range compare.ComparedCheckConstraints(desired, database, target, semantics) {
		key := exprkey.Check(semantics, constraint.Table, constraint.Name)
		current, exists := held[key]
		if !exists || strings.TrimSpace(constraint.CheckExpression) == "" || slices.Contains(keys, key) {
			continue
		}
		keys = append(keys, key)
		request.Pairs = append(request.Pairs, schemacomparison.CheckPair{Held: current, Declared: strings.TrimSpace(constraint.CheckExpression)})
	}
	if len(request.Pairs) == 0 {
		return nil, nil
	}
	answers, err := service.EquivalentChecks(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if len(answers) != len(request.Pairs) {
		return nil, fmt.Errorf("compare schemas: the target answered %d of %d CHECK comparisons", len(answers), len(request.Pairs))
	}
	result := make(map[string]config.CheckExpression, len(keys))
	for i, key := range keys {
		pair := request.Pairs[i]
		if answers[i] {
			result[key] = config.CheckExpression{Expression: strings.TrimSpace(*pair.Held.CheckClause), Resolved: true}
			continue
		}
		result[key] = config.CheckExpression{Expression: pair.Declared, Resolved: true, Differs: true}
	}
	return result, nil
}
