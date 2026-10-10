package chcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chrefresh"
)

// RefreshService compares the refresh schedules of materialized views both
// sides hold. Common view creation, removal and body changes remain
// parent-owned. The zero value is ready for concurrent use without database
// access.
type RefreshService struct{}

// CompareFacets reads both schedules in the spelling the server stores, with
// dependencies qualified by the view's schema, and reports a change when they
// differ. A view without a declared schedule asks for none where the source's
// coverage is complete for the kind; under any other coverage its schedule is
// unmanaged and the observed one is adopted, so a source that cannot state a
// schedule never removes one. A declared schedule that cannot be read is
// undecided, and so is a view the current side did not inspect, unless the
// declaration asks for a plain view and the current side states no schedule
// it could not read.
// Errors and cancellation return no partial result. Inputs remain unchanged.
func (RefreshService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: refresh comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: ClickHouse refresh comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{chschema.RefreshKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported ClickHouse refresh comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindMatView || owner.Subject.Name.Empty() ||
			seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate ClickHouse materialized view owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(chschema.RefreshKind, owner.Subject) {
			continue
		}
		if err := compareRefresh(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

// compareRefresh compares one view both sides hold. A new view is created
// with its declared schedule and a removed one takes its schedule with it.
func compareRefresh(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	if !owner.Desired || !owner.Current {
		return nil
	}
	desired, current, err := refreshValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if refreshLimited(request.Desired.Coverage, owner.Subject) {
		refreshUndecided(result, owner.Subject, "the desired source could not describe the ClickHouse refresh schedule")
		return nil
	}
	declaresNone := request.Desired.Coverage.Lookup(chschema.RefreshKind, owner.Subject).State == schemaext.Complete
	if desired == nil && !declaresNone {
		return adoptRefresh(result, owner.Subject, current)
	}
	if !refreshKnown(request.Current.Coverage, owner.Subject) {
		// A declared plain view is only in doubt when the current side says a
		// schedule exists that it could not read. A side that did not look,
		// such as a document or an account that may not read the catalog of
		// schedules, states none, and a plain declaration asks for nothing a
		// plan could do (stokaro/ptah#4278).
		state := request.Current.Coverage.Lookup(chschema.RefreshKind, owner.Subject).State
		if desired == nil && state != schemaext.Unrepresentable {
			return nil
		}
		refreshUndecided(result, owner.Subject, "the ClickHouse refresh schedule was not fully inspected")
		return nil
	}
	var wanted *chschema.Schedule
	if desired != nil {
		wanted, err = chrefresh.Canonical(&desired.Schedule, refreshSchema(request, owner.Subject))
		if err != nil {
			// The declaration is refused where it is rendered, with the
			// reason; a difference invented here would plan work for a
			// schedule that is never going to be sent.
			refreshUndecided(result, owner.Subject, "the declared ClickHouse refresh schedule cannot be read: "+err.Error())
			return nil
		}
	}
	var reported *chschema.Schedule
	if current != nil {
		// The server stores a dependency qualified when it creates a view, and
		// 24.10 keeps the spelling MODIFY REFRESH was given, so the stored
		// side is read the same way as the declared one.
		reported = &current.Schedule
		if stored, err := chrefresh.Canonical(&current.Schedule, refreshSchema(request, owner.Subject)); err == nil {
			reported = stored
		}
	}
	if sameSchedule(wanted, reported) {
		return nil
	}
	change := &chdiff.Refresh{Before: current}
	if wanted != nil {
		change.After = &chschema.DesiredRefresh{Schedule: *wanted}
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{
		Kind: chschema.RefreshKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change},
	})
	return nil
}

// adoptRefresh keeps the observed schedule of a view whose source leaves it
// unmanaged, so a later replacement recreates the view with it. A view the
// server reports without one adopts nothing.
func adoptRefresh(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *chschema.ObservedRefresh) error {
	if current == nil {
		return nil
	}
	adopted, err := values(result.Desired, subject).With(current.Desired())
	if err != nil {
		return err
	}
	for i := range result.Desired.Records {
		if result.Desired.Records[i].Subject.Key() == subject.Key() {
			result.Desired.Records[i].Values = adopted
			return nil
		}
	}
	result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: adopted})
	return nil
}

func refreshValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*chschema.DesiredRefresh, *chschema.ObservedRefresh, error) {
	desired, _, err := schemaext.FacetAs[*chschema.DesiredRefresh](values(request.Desired, subject), chschema.RefreshKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*chschema.ObservedRefresh](values(request.Current, subject), chschema.RefreshKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := chschema.ValidateDesiredRefresh(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := chschema.ValidateObservedRefresh(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// refreshSchema is the schema a declared dependency is qualified with: the
// one the server reports the view under, or the view's resolved schema when
// the server reports no schedule to take it from.
func refreshSchema(request schemaext.FacetComparisonRequest, subject objectidentity.ID) string {
	for _, record := range request.Current.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Subject.Schema.Source
		}
	}
	return subject.Schema.Source
}

// refreshKnown reports whether the current side establishes the view's
// schedule, including that it has none.
func refreshKnown(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	return coverage.Lookup(chschema.RefreshKind, subject).State == schemaext.Complete
}

func refreshLimited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(chschema.RefreshKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func refreshUndecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: chschema.RefreshKind, Subject: subject, Reason: reason})
}

// sameSchedule compares two schedules read the way the server stores them;
// nil is a plain view.
func sameSchedule(a, b *chschema.Schedule) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
