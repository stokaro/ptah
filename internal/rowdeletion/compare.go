package rowdeletion

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Facet describes one owner's row deletion facet to [Facet.Compare]: its
// kind, the capability a declaration needs, and the owner's reading of its own
// values. D is the owner's declaration and O its observation.
type Facet[D, O schemaext.Value] struct {
	// Target is the canonical target the owner serves.
	Target string
	// Kind is the facet's kind.
	Kind schemaext.Kind
	// Capability is the target capability a declaration needs.
	Capability capability.Capability
	// Name names the policy in a diagnostic, such as "Spanner row deletion
	// policy".
	Name string
	// ValidateDesired and ValidateObserved refuse a value the owner cannot
	// read.
	ValidateDesired  func(D) error
	ValidateObserved func(O) error
	// Equivalent reports whether the database already holds the declared
	// policy, reading each interval as the value it denotes in the target's
	// spelling and each column with the target's identifier rules.
	Equivalent func(identifier.Semantics, D, O) bool
	// Change is the change from before to after. A side whose has flag is
	// false is a known absence.
	Change func(before O, hasBefore bool, after D, hasAfter bool) schemaext.ChangeValue
	// Adopt is the declaration that keeps an observed policy as it is.
	Adopt func(O) D
}

// Compare returns a complete comparison and preserves desired declarations.
//
// A declaration with complete source knowledge manages the policy: a table it
// leaves without a value requests none. A source that could not declare a
// policy leaves it unmanaged, and the observed policy is adopted into the
// effective declaration so a rebuild of the table keeps it. Managed intent
// against a table the read did not inspect is undecided, unless the capability
// set establishes that the target has no such policy. A declaration the source
// could not describe is undecided, on a table the plan creates too, and so is
// a policy the read could not describe. A table created or removed by the plan
// carries its policy with it. Invalid input and cancellation return a zero
// result.
func (f Facet[D, O]) Compare(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != f.Target {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: %s comparison on %q", ptaherr.ErrUnsupportedDialect, f.Name, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{f.Kind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported %s comparison kinds", schemaext.ErrInvalidValue, f.Name)
	}
	c := comparison[D, O]{
		facet:    f,
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
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate %s owner", schemaext.ErrInvalidValue, f.Name)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(f.Kind, owner.Subject) {
			continue
		}
		if err := c.table(owner); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return c.result, nil
}

// comparison holds one request's records indexed by subject, so each owner is
// a lookup rather than a scan of every record.
type comparison[D, O schemaext.Value] struct {
	facet   Facet[D, O]
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

// sides is one table's two values, each with whether it is present.
type sides[D, O schemaext.Value] struct {
	desired    D
	hasDesired bool
	current    O
	hasCurrent bool
}

func (c *comparison[D, O]) table(owner schemaext.ParentState) error {
	request := c.request
	values, err := c.values(owner.Subject)
	if err != nil {
		return err
	}
	if values.hasDesired && !request.Capabilities.Has(c.facet.Capability) {
		return &ptaherr.CapabilityError{Dialect: c.facet.Target, Feature: string(c.facet.Capability), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s declares a %s, which requires target capability %s", owner.Subject, c.facet.Name, c.facet.Capability)}
	}
	// A table the plan creates carries its declaration with it, so a source
	// that could not describe the policy is undecided there too.
	if knowledge, found := request.Desired.Coverage.SubjectKnowledge(c.facet.Kind, owner.Subject); owner.Desired && found && knowledge.State == schemaext.Unrepresentable {
		c.undecided(owner.Subject, "the desired source could not describe the "+c.facet.Name)
		return nil
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	return c.held(owner.Subject, values)
}

// held compares the policy of a table both sides hold.
func (c *comparison[D, O]) held(subject objectidentity.ID, values sides[D, O]) error {
	request := c.request
	if !values.hasDesired && !known(request.Desired.Coverage, c.facet.Kind, subject, true) {
		if values.hasCurrent {
			return c.adopt(subject, values.current)
		}
		return nil
	}
	if !values.hasDesired && !values.hasCurrent && cannotHold(request.Capabilities, c.facet.Capability) {
		return nil
	}
	if knowledge, found := request.Current.Coverage.SubjectKnowledge(c.facet.Kind, subject); found && knowledge.State == schemaext.Unrepresentable {
		c.undecided(subject, "the read found a "+c.facet.Name+" it could not describe")
		return nil
	}
	if (!values.hasCurrent && !known(request.Current.Coverage, c.facet.Kind, subject, false)) || limited(request.Current.Coverage, c.facet.Kind, subject) {
		c.undecided(subject, "the "+c.facet.Name+" was not inspected")
		return nil
	}
	if !values.hasDesired && !values.hasCurrent {
		return nil
	}
	if values.hasDesired && values.hasCurrent && c.facet.Equivalent(request.Identifiers, values.desired, values.current) {
		return nil
	}
	change := c.facet.Change(values.current, values.hasCurrent, values.desired, values.hasDesired)
	c.result.Changes = append(c.result.Changes, schemaext.FacetChange{
		Kind: c.facet.Kind, Change: schemaext.ChangeRecord{Subject: subject, Value: change.CloneChange()},
	})
	return nil
}

func (c *comparison[D, O]) values(subject objectidentity.ID) (sides[D, O], error) {
	var desiredFacets, currentFacets schemaext.Facets
	if i, found := c.desired[subject.Key()]; found {
		desiredFacets = c.request.Desired.Records[i].Values
	}
	if i, found := c.current[subject.Key()]; found {
		currentFacets = c.request.Current.Records[i].Values
	}
	var values sides[D, O]
	var err error
	values.desired, values.hasDesired, err = schemaext.FacetAs[D](desiredFacets, c.facet.Kind)
	if err != nil {
		return sides[D, O]{}, err
	}
	values.current, values.hasCurrent, err = schemaext.FacetAs[O](currentFacets, c.facet.Kind)
	if err != nil {
		return sides[D, O]{}, err
	}
	if values.hasDesired {
		if err := c.facet.ValidateDesired(values.desired); err != nil {
			return sides[D, O]{}, err
		}
	}
	if values.hasCurrent {
		if err := c.facet.ValidateObserved(values.current); err != nil {
			return sides[D, O]{}, err
		}
	}
	return values, nil
}

// adopt keeps an unmanaged policy in the effective declaration, so a planner
// that rebuilds the table restores it rather than dropping it.
func (c *comparison[D, O]) adopt(subject objectidentity.ID, current O) error {
	declaration := c.facet.Adopt(current)
	if i, found := c.declared[subject.Key()]; found {
		values, err := c.result.Desired.Records[i].Values.With(declaration)
		if err != nil {
			return err
		}
		c.result.Desired.Records[i].Values = values
		return nil
	}
	facets, err := schemaext.NewFacets(declaration)
	if err != nil {
		return err
	}
	c.declared[subject.Key()] = len(c.result.Desired.Records)
	c.result.Desired.Records = append(c.result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func (c *comparison[D, O]) undecided(subject objectidentity.ID, reason string) {
	c.result.Undecided = append(c.result.Undecided, schemaext.UndecidedChange{Kind: c.facet.Kind, Subject: subject, Reason: reason})
}

// known reports knowledge that makes a missing value an absence. A desired
// default request asks for the engine's default, which is no policy.
func known(coverage schemaext.Coverage, kind schemaext.Kind, subject objectidentity.ID, desired bool) bool {
	switch coverage.Lookup(kind, subject).State {
	case schemaext.Complete, schemaext.Absent:
		return true
	case schemaext.Defaulted:
		return desired
	default:
		return false
	}
}

func limited(coverage schemaext.Coverage, kind schemaext.Kind, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(kind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

// cannotHold reports a capability set that establishes the target has no such
// policy. An unanswered key establishes nothing.
func cannotHold(caps capability.Capabilities, key capability.Capability) bool {
	return caps.Established(key) && !caps.Has(key)
}
