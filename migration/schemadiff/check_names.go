package schemadiff

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/schemaprep"
	"ptah.run/migration/schemadiff/internal/compare"
)

// heldCheckNames names the constraints desired leaves unnamed for a
// comparison with database, as [schemaprep.AssignConstraintNamesAgainst] does:
// an unnamed table CHECK takes the name of the CHECK database holds with the
// same condition before the rest are numbered. A nil database names them as a
// render does.
//
// The pairing asks the same question the comparison asks of a CHECK the two
// sides name alike, so a CHECK it pairs is one the comparison then finds
// unchanged: checks decides where the target registers it and answers, and
// otherwise the conditions are compared as text ([compare.CheckConditionsAlike]).
// session is the connection's probe session, nil on a comparison with no
// connection.
func heldCheckNames(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	caps capability.Capabilities,
	checks schemacomparison.Checks,
	session schemaext.ProbeSession,
) (*schemamodel.Database, error) {
	if database == nil {
		return schemaprep.AssignDefaultConstraintNames(desired, dialect), nil
	}
	pairing := heldCheckPairing{
		database: database, dialect: dialect, semantics: semantics, caps: caps, checks: checks, session: session,
	}
	return schemaprep.AssignConstraintNamesAgainst(ctx, desired, dialect, pairing)
}

// heldCheckPairing is the [schemaprep.CheckPairing] of a comparison with
// database.
type heldCheckPairing struct {
	database  *catalog.Database
	dialect   string
	semantics identifier.Semantics
	caps      capability.Capabilities
	checks    schemacomparison.Checks
	session   schemaext.ProbeSession
}

// Held returns the CHECKs with a condition that database holds on table,
// ordered by name.
func (p heldCheckPairing) Held(table schemamodel.Table) []schemaprep.HeldCheck {
	var held []schemaprep.HeldCheck
	for _, constraint := range p.heldOn(table) {
		held = append(held, schemaprep.HeldCheck{Name: constraint.Name, Expression: strings.TrimSpace(*constraint.CheckClause)})
	}
	return held
}

// Match asks the target about every expression and candidate in one request,
// where it decides CHECK equivalence, and compares the conditions as text
// where it does not or answers nothing.
func (p heldCheckPairing) Match(
	ctx context.Context, table schemamodel.Table, expressions []string, candidates []schemaprep.HeldCheck,
) ([][]bool, error) {
	if p.checks != nil {
		alike, err := p.decided(ctx, table, expressions, candidates)
		if err != nil || alike != nil {
			return alike, err
		}
	}
	alike := make([][]bool, len(expressions))
	for i, expression := range expressions {
		alike[i] = make([]bool, len(candidates))
		for j, candidate := range candidates {
			alike[i][j] = compare.CheckConditionsAlike(expression, candidate.Expression, p.dialect)
		}
	}
	return alike, nil
}

// decided asks the target which candidates each declared CHECK is, and
// returns nil when it answers nothing.
func (p heldCheckPairing) decided(
	ctx context.Context, table schemamodel.Table, expressions []string, candidates []schemaprep.HeldCheck,
) ([][]bool, error) {
	byName := make(map[string]catalog.Constraint)
	for _, constraint := range p.heldOn(table) {
		byName[constraint.Name] = constraint
	}
	request := schemacomparison.ChecksRequest{
		Target: p.dialect, Identifiers: p.semantics, Capabilities: p.caps, Current: p.database, Session: p.session,
	}
	for _, expression := range expressions {
		for _, candidate := range candidates {
			held, found := byName[candidate.Name]
			if !found {
				return nil, fmt.Errorf("compare schemas: table %s holds no CHECK %s", table.QualifiedName(), candidate.Name)
			}
			request.Pairs = append(request.Pairs, schemacomparison.CheckPair{Held: held, Declared: strings.TrimSpace(expression)})
		}
	}
	answers, err := p.checks.EquivalentChecks(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if answers == nil {
		return nil, nil
	}
	if len(answers) != len(request.Pairs) {
		return nil, fmt.Errorf("compare schemas: the target answered %d of %d CHECK comparisons", len(answers), len(request.Pairs))
	}
	alike := make([][]bool, len(expressions))
	for i := range expressions {
		alike[i] = answers[i*len(candidates) : (i+1)*len(candidates)]
	}
	return alike, nil
}

// heldOn returns the CHECKs with a condition that database holds on table,
// ordered by name.
func (p heldCheckPairing) heldOn(table schemamodel.Table) []catalog.Constraint {
	key := exprkey.Table(p.semantics, table.QualifiedName())
	var held []catalog.Constraint
	for _, constraint := range p.database.Constraints {
		if !strings.EqualFold(constraint.Type, "CHECK") || constraint.CheckClause == nil || strings.TrimSpace(*constraint.CheckClause) == "" {
			continue
		}
		if exprkey.TableParts(p.semantics, constraint.Schema, constraint.TableName) == key {
			held = append(held, constraint)
		}
	}
	slices.SortFunc(held, func(a, b catalog.Constraint) int { return cmp.Compare(a.Name, b.Name) })
	return held
}
