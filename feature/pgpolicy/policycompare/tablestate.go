package policycompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// TableStateService compares the ENABLE and FORCE ROW LEVEL SECURITY switches
// of surviving tables. Its zero value is ready for concurrent use.
//
// The switches are independent flags of the table, so both are compared on
// every table. Where a source describes the switches, a table without the
// facet has both off: a description that declares no enablement for a table
// the server secures asks for row security to be disabled. A source that
// cannot describe them, and a read that did not report them, keep the switches
// the server holds, and so does a declaration that names the table's policies
// and not its switches (stokaro/ptah#2048). A table's creation
// carries its switches, and its removal takes them, so neither is a change
// here.
//
// FORCE survives DISABLE in PostgreSQL, so a table the server holds disabled
// but forced compares unequal to a declaration that enables it without FORCE,
// and the change carries both switches.
type TableStateService struct{}

// CompareFacets returns a complete comparison that retains every declaration.
// Unavailable current state for a surviving table whose switches the
// declaration names is undecided, never an empty successful diff. Inputs remain
// unchanged.
func (TableStateService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: PostgreSQL row-security comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{pgpolicy.TableStateKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported row-security facet comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate row-security table %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(pgpolicy.TableStateKind, owner.Subject) {
			continue
		}
		if err := compareTableState(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func compareTableState(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	desired, current, err := tableStateValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if stateLimited(request.Desired.Coverage, owner.Subject) {
		undecidedState(result, owner.Subject, "the desired source could not describe this table's row-security switches")
		return nil
	}
	desiredKnowledge := request.Desired.Coverage.Lookup(pgpolicy.TableStateKind, owner.Subject)
	currentKnowledge := request.Current.Coverage.Lookup(pgpolicy.TableStateKind, owner.Subject)
	// A declaration that names the table's policies and not its switches
	// requests the owner's default, which keeps the switches the table has
	// (stokaro/ptah#2048). A switch declared elsewhere takes precedence.
	if desired == nil && desiredKnowledge.State == schemaext.Defaulted {
		if stateLimited(request.Current.Coverage, owner.Subject) || current == nil && unknown(currentKnowledge) {
			return nil
		}
		if current == nil {
			current = &pgpolicy.ObservedTableState{}
		}
		return adoptTableState(result, owner.Subject, current)
	}
	// Switches nobody read are decided only where the declaration names
	// them; a table it leaves without the facet keeps what it has, as an
	// unread namespace plans no removal.
	if stateLimited(request.Current.Coverage, owner.Subject) || current == nil && unknown(currentKnowledge) {
		if desired != nil {
			undecidedState(result, owner.Subject, "this table's row-security switches were not read: "+currentKnowledge.Reason)
		}
		return nil
	}
	if desired == nil && unknown(desiredKnowledge) {
		// A source that cannot describe the switches keeps what the server
		// holds. A read that recorded none holds the default, which needs no
		// declaration to keep; adopting it would declare a value neither
		// side carried.
		if current == nil {
			return nil
		}
		return adoptTableState(result, owner.Subject, current)
	}
	if current == nil {
		current = &pgpolicy.ObservedTableState{}
	}
	if desired == nil {
		desired = &pgpolicy.DesiredTableState{}
	}
	if desired.Enabled == current.Enabled && desired.Forced == current.Forced {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{Kind: pgpolicy.TableStateKind,
		Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: &pgpolicy.TableStateChange{
			Before: current, After: desired, Access: pgpolicy.TableStateAccess(current, desired)}}})
	return nil
}

// adoptTableState keeps the switches the server holds in the effective
// declaration of a source that cannot describe them.
func adoptTableState(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *pgpolicy.ObservedTableState) error {
	declared, err := current.Desired()
	if err != nil {
		return err
	}
	for i, record := range result.Desired.Records {
		if record.Subject.Key() != subject.Key() {
			continue
		}
		values, err := record.Values.With(declared)
		if err != nil {
			return err
		}
		result.Desired.Records[i].Values = values
		return nil
	}
	facets, err := schemaext.NewFacets(declared)
	if err != nil {
		return err
	}
	result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func tableStateValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*pgpolicy.DesiredTableState, *pgpolicy.ObservedTableState, error) {
	desired, _, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](facetValues(request.Desired, subject), pgpolicy.TableStateKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*pgpolicy.ObservedTableState](facetValues(request.Current, subject), pgpolicy.TableStateKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := pgpolicy.ValidateDesiredTableState(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := pgpolicy.ValidateObservedTableState(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

func facetValues(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func stateLimited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(pgpolicy.TableStateKind, subject)
	return found && unknown(knowledge)
}

func undecidedState(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgpolicy.TableStateKind, Subject: subject, Reason: reason})
}
