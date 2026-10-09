// Package crdbcompare compares CockroachDB row-level TTL facets using captured
// source knowledge. It reads the two values the server rewrites through the
// value they denote.
package crdbcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/internal/ttlsql"
)

// Service compares row-level TTL without database access. Its zero value is
// ready for concurrent use. Creation and removal of a table carry its policy
// with the table; changes describe only tables both sides hold.
type Service struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations. A declaration with complete source knowledge manages the
// policy: a table it leaves without a value requests no TTL. A source that
// could not declare the policy leaves it unmanaged, and the observed policy is
// adopted into the effective declaration so a later rebuild keeps it. Unknown
// current state with managed intent returns an undecided diagnostic, never an
// empty successful diff. Invalid input and cancellation return a zero result.
func (Service) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.CockroachDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: CockroachDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{crdbschema.RowTTLKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported CockroachDB facet comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate CockroachDB facet owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(crdbschema.RowTTLKind, owner.Subject) {
			continue
		}
		if err := compareTable(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func compareTable(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	desired, current, err := tableValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if desired != nil && !request.Capabilities.Has(capability.RowLevelTTL) {
		return &ptaherr.CapabilityError{Dialect: platform.CockroachDB, Feature: string(capability.RowLevelTTL), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s declares row-level TTL, which requires target capability %s", owner.Subject, capability.RowLevelTTL)}
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if knowledge, found := request.Desired.Coverage.SubjectKnowledge(crdbschema.RowTTLKind, owner.Subject); found && knowledge.State == schemaext.Unrepresentable {
		undecided(result, owner.Subject, "the desired source could not describe CockroachDB row-level TTL")
		return nil
	}
	if desired == nil && !known(request.Desired.Coverage, owner.Subject, true) {
		if current != nil {
			return adopt(result, owner.Subject, current)
		}
		return nil
	}
	if (current == nil && !known(request.Current.Coverage, owner.Subject, false)) || limited(request.Current.Coverage, owner.Subject) {
		undecided(result, owner.Subject, "CockroachDB row-level TTL was not inspected")
		return nil
	}
	if desired == nil && current == nil {
		return nil
	}
	if desired != nil && current != nil && ttlsql.Equivalent(desired.Policy, current.Policy) {
		return nil
	}
	change := &crdbdiff.RowTTL{Before: current, After: desired}
	result.Changes = append(result.Changes, schemaext.FacetChange{
		Kind: crdbschema.RowTTLKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change.CloneChange()},
	})
	return nil
}

func tableValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*crdbschema.DesiredRowTTL, *crdbschema.ObservedRowTTL, error) {
	desired, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](values(request.Desired, subject), crdbschema.RowTTLKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*crdbschema.ObservedRowTTL](values(request.Current, subject), crdbschema.RowTTLKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := crdbschema.ValidateDesired(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := crdbschema.ValidateObserved(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// An unmanaged policy is retained in the effective declaration, so a planner
// that rebuilds the table restores it rather than dropping it.
func adopt(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *crdbschema.ObservedRowTTL) error {
	for i, record := range result.Desired.Records {
		if record.Subject.Key() != subject.Key() {
			continue
		}
		values, err := record.Values.With(current.Desired())
		if err != nil {
			return err
		}
		result.Desired.Records[i].Values = values
		return nil
	}
	facets, err := schemaext.NewFacets(current.Desired())
	if err != nil {
		return err
	}
	result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func values(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

// known reports knowledge that makes a missing value an absence. A desired
// default request asks for the engine's default, which is no TTL.
func known(coverage schemaext.Coverage, subject objectidentity.ID, desired bool) bool {
	switch coverage.Lookup(crdbschema.RowTTLKind, subject).State {
	case schemaext.Complete, schemaext.Absent:
		return true
	case schemaext.Defaulted:
		return desired
	default:
		return false
	}
}

func limited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(crdbschema.RowTTLKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func undecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: crdbschema.RowTTLKind, Subject: subject, Reason: reason})
}
