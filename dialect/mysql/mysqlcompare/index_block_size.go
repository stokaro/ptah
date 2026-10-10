package mysqlcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
)

// IndexBlockSizeService compares the KEY_BLOCK_SIZE hint of MySQL and
// MariaDB indexes. Its zero value is ready for concurrent use.
type IndexBlockSizeService struct{}

// CompareFacets reports a change for an index both sides hold whose
// declared hint differs from the one it holds, where the server retains a
// hint on it: MariaDB on every table, MySQL only on a table with
// ROW_FORMAT=COMPRESSED. Elsewhere the server accepts and discards a hint, so
// a change would be planned on every comparison and never applied.
//
// A declaration without the hint declares none when its source claims to
// describe the hint; a source that makes no claim keeps the hint the index
// holds. A current index without an observation, which only a declaration
// converted to an observation can be, holds what a declaration without a hint
// converts to. An explicit knowledge limit on either side is reported as
// undecided. The declarations are returned as they were. Invalid input and
// cancellation return a zero result.
func (IndexBlockSizeService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: MySQL index block size comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mysqlschema.IndexBlockSizeKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported MySQL index block size comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindIndex || owner.Subject.Name.Empty() || owner.Subject.Parent.Empty() {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: a MySQL index block size attaches to a table's index, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, undecided, err := compareBlockSize(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: mysqlschema.IndexBlockSizeKind,
				Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if undecided != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: mysqlschema.IndexBlockSizeKind, Subject: owner.Subject, Reason: undecided})
		}
	}
	return result, ctx.Err()
}

// compareBlockSize returns the change one index needs, or the reason it
// cannot be decided. Each value is validated, an owner on one side only
// included.
func compareBlockSize(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*mysqldiff.IndexBlockSize, string, error) {
	desired, _, err := schemaext.FacetAs[*mysqlschema.DesiredIndexBlockSize](facetsOf(request.Desired, owner.Subject), mysqlschema.IndexBlockSizeKind)
	if err != nil {
		return nil, "", err
	}
	if desired != nil {
		if err := mysqlschema.ValidateDesiredIndexBlockSize(desired); err != nil {
			return nil, "", err
		}
	}
	current, _, err := schemaext.FacetAs[*mysqlschema.ObservedIndexBlockSize](facetsOf(request.Current, owner.Subject), mysqlschema.IndexBlockSizeKind)
	if err != nil {
		return nil, "", err
	}
	if current != nil {
		if err := mysqlschema.ValidateObservedIndexBlockSize(current); err != nil {
			return nil, "", err
		}
	}
	if !owner.Desired || !owner.Current || !request.Includes(mysqlschema.IndexBlockSizeKind, owner.Subject) {
		return nil, "", nil
	}
	if limited(request.Desired.Coverage, owner.Subject) {
		return nil, "the desired source could not describe the index's MySQL block size", nil
	}
	if limited(request.Current.Coverage, owner.Subject) {
		return nil, "the index's MySQL block size was not fully inspected", nil
	}
	if desired == nil {
		if !describes(request.Desired.Coverage, owner.Subject) {
			return nil, "", nil
		}
		desired = &mysqlschema.DesiredIndexBlockSize{}
	}
	if current == nil {
		if !describes(request.Current.Coverage, owner.Subject) {
			return nil, "", nil
		}
		if current, err = (&mysqlschema.DesiredIndexBlockSize{}).Observed(request.Target); err != nil {
			return nil, "", err
		}
	}
	if !current.Retained || current.KeyBlockSize == desired.KeyBlockSize {
		return nil, "", nil
	}
	return &mysqldiff.IndexBlockSize{Before: new(*current), After: new(*desired)}, "", nil
}

// describes reports a claim that the source describes the hint of subject,
// so a hint it does not hold is a hint the index does not have.
func describes(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(mysqlschema.IndexBlockSizeKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

// limited reports an explicit claim that the source could not describe the
// hint of subject.
func limited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(mysqlschema.IndexBlockSizeKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func facetsOf(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}
