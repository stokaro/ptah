// Package crdbcompare compares CockroachDB row-level TTL facets using captured
// source knowledge. It reads the two values the server rewrites through the
// value they denote.
package crdbcompare

import (
	"context"
	"fmt"
	"maps"
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
	c := comparison{
		request:  request,
		result:   schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired},
		desired:  recordIndex(request.Desired),
		current:  recordIndex(request.Current),
		declared: make(map[objectidentity.Key]int),
	}
	c.result.Desired.Records = slices.Clone(c.result.Desired.Records)
	maps.Copy(c.declared, c.desired)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate CockroachDB facet owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(crdbschema.RowTTLKind, owner.Subject) {
			continue
		}
		if err := c.table(owner); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	result := c.result
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

// comparison holds one request's records indexed by subject, so each owner is
// a lookup rather than a scan of every record.
type comparison struct {
	request schemaext.FacetComparisonRequest
	result  schemaext.FacetComparisonResult
	// desired and current index the request's records; declared indexes the
	// result's desired records, which adopt extends.
	desired, current, declared map[objectidentity.Key]int
}

func recordIndex(state schemaext.FacetState) map[objectidentity.Key]int {
	index := make(map[objectidentity.Key]int, len(state.Records))
	for i, record := range state.Records {
		index[record.Subject.Key()] = i
	}
	return index
}

func (c *comparison) table(owner schemaext.ParentState) error {
	request := c.request
	desired, current, err := c.values(owner.Subject)
	if err != nil {
		return err
	}
	if desired != nil && !request.Capabilities.Has(capability.RowLevelTTL) {
		return &ptaherr.CapabilityError{Dialect: platform.CockroachDB, Feature: string(capability.RowLevelTTL), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s declares row-level TTL, which requires target capability %s", owner.Subject, capability.RowLevelTTL)}
	}
	// A table the plan creates carries its declaration with it, so a source
	// that could not describe the policy is undecided there too: creating the
	// table without it would turn a knowledge limit into a silent omission.
	if knowledge, found := request.Desired.Coverage.SubjectKnowledge(crdbschema.RowTTLKind, owner.Subject); owner.Desired && found && knowledge.State == schemaext.Unrepresentable {
		undecided(&c.result, owner.Subject, "the desired source could not describe CockroachDB row-level TTL")
		return nil
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	return c.held(owner.Subject, desired, current)
}

// held compares the policy of a table both sides hold.
func (c *comparison) held(subject objectidentity.ID, desired *crdbschema.DesiredRowTTL, current *crdbschema.ObservedRowTTL) error {
	request := c.request
	if desired == nil && !known(request.Desired.Coverage, subject, true) {
		if current != nil {
			return c.adopt(subject, current)
		}
		return nil
	}
	// A target established to lack row-level TTL holds no policy, so a
	// declaration of none is met whatever the read asked. Without that fact an
	// uninspected table may hold one, and declaring none is managed intent the
	// comparison cannot decide.
	if desired == nil && current == nil && cannotHoldRowTTL(request.Capabilities) {
		return nil
	}
	if knowledge, found := request.Current.Coverage.SubjectKnowledge(crdbschema.RowTTLKind, subject); found && knowledge.State == schemaext.Unrepresentable {
		undecided(&c.result, subject, "the read found a CockroachDB row-level TTL it could not describe")
		return nil
	}
	if (current == nil && !known(request.Current.Coverage, subject, false)) || limited(request.Current.Coverage, subject) {
		undecided(&c.result, subject, "CockroachDB row-level TTL was not inspected")
		return nil
	}
	if desired == nil && current == nil {
		return nil
	}
	if desired != nil && current != nil && ttlsql.Equivalent(desired.Policy, current.Policy) {
		return nil
	}
	change := &crdbdiff.RowTTL{Before: current, After: desired}
	c.result.Changes = append(c.result.Changes, schemaext.FacetChange{
		Kind: crdbschema.RowTTLKind, Change: schemaext.ChangeRecord{Subject: subject, Value: change.CloneChange()},
	})
	return nil
}

func (c *comparison) values(subject objectidentity.ID) (*crdbschema.DesiredRowTTL, *crdbschema.ObservedRowTTL, error) {
	var desiredFacets, currentFacets schemaext.Facets
	if i, found := c.desired[subject.Key()]; found {
		desiredFacets = c.request.Desired.Records[i].Values
	}
	if i, found := c.current[subject.Key()]; found {
		currentFacets = c.request.Current.Records[i].Values
	}
	desired, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](desiredFacets, crdbschema.RowTTLKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*crdbschema.ObservedRowTTL](currentFacets, crdbschema.RowTTLKind)
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
func (c *comparison) adopt(subject objectidentity.ID, current *crdbschema.ObservedRowTTL) error {
	if i, found := c.declared[subject.Key()]; found {
		values, err := c.result.Desired.Records[i].Values.With(current.Desired())
		if err != nil {
			return err
		}
		c.result.Desired.Records[i].Values = values
		return nil
	}
	facets, err := schemaext.NewFacets(current.Desired())
	if err != nil {
		return err
	}
	c.declared[subject.Key()] = len(c.result.Desired.Records)
	c.result.Desired.Records = append(c.result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
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

// cannotHoldRowTTL reports a capability set that establishes the target has no
// row-level TTL. An unanswered key establishes nothing.
func cannotHoldRowTTL(caps capability.Capabilities) bool {
	return caps.Established(capability.RowLevelTTL) && !caps.Has(capability.RowLevelTTL)
}
