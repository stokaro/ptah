package chresolve

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexRequest resolves data-skipping settings for a common index. Current must
// be a complete usable observation; the caller checks coverage before supplying
// it. Creating establishes index absence and forbids Current. Key expressions
// belong to the common index and are not changed by this resolution.
type IndexRequest struct {
	Desired  *chschema.DesiredIndex
	Current  *chschema.ObservedIndex
	Creating bool
}

// IndexOrigins records the evidence used for each prepared index setting.
type IndexOrigins struct {
	IndexType, Granularity Origin
}

// IndexResult keeps authored and resolved settings separately. Prepared is fully
// explicit intent for planning, not evidence of execution or inspection.
type IndexResult struct {
	Declared chschema.DesiredIndex
	Prepared chschema.DesiredIndex
	Origins  IndexOrigins
}

// Index resolves omitted settings from captured state on existing indexes and
// from creation rules on new indexes. An explicit default always selects Ptah's
// minmax type or granularity 1; it does not inspect server configuration.
// Invalid or contradictory inputs wrap schemaext.ErrInvalidValue. Missing
// evidence required by an omitted setting wraps ErrUnknownCurrent. Any error
// returns a zero result, and neither input is changed.
func Index(request IndexRequest) (IndexResult, error) {
	if err := chschema.ValidateDesiredIndex(request.Desired); err != nil {
		return IndexResult{}, err
	}
	if request.Creating && request.Current != nil {
		return IndexResult{}, fmt.Errorf("%w: a new ClickHouse index cannot have current state", schemaext.ErrInvalidValue)
	}
	current := chschema.ObservedIndex{}
	var indexType *string
	if request.Current != nil {
		if err := chschema.ValidateObservedIndex(request.Current); err != nil {
			return IndexResult{}, err
		}
		current = *request.Current
		indexType = &current.IndexType
	}
	result := IndexResult{Declared: *request.Desired}
	if err := resolveProperty(property{
		name: "index_type", desired: request.Desired.IndexType, current: indexType, fallback: "minmax",
		value: &result.Prepared.IndexType, origin: &result.Origins.IndexType,
	}, request.Creating); err != nil {
		return IndexResult{}, err
	}
	result.Prepared.Granularity.State = chschema.Explicit
	switch {
	case request.Desired.Granularity.State == chschema.Explicit:
		result.Prepared.Granularity.Value = request.Desired.Granularity.Value
		result.Origins.Granularity = Declaration
	case request.Desired.Granularity.State == chschema.Unspecified && !request.Creating:
		if request.Current == nil {
			return IndexResult{}, fmt.Errorf("%w: index granularity", ErrUnknownCurrent)
		}
		result.Prepared.Granularity.Value = current.Granularity
		result.Origins.Granularity = Observation
	default:
		result.Prepared.Granularity.Value = 1
		result.Origins.Granularity = CreationRule
	}
	return result, nil
}
