// Package tsreverse reverses captured TimescaleDB changes and reports what
// restoring a definition cannot recover. It never reads a database.
package tsreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Service reconstructs the inverse of hypertable and continuous-aggregate
// changes from their complete operands. Its zero value is ready for concurrent
// use.
//
// A partitioning change reverses into the opposite partitioning change, which
// its planner refuses: TimescaleDB has no statement that turns a hypertable
// back into an ordinary table. The reversal still says so rather than failing
// here, because refusing loudly in the down plan is what makes an irreversible
// up step visible.
type Service struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. ForwardState is a prediction, never inspection evidence.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: TimescaleDB reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func reverse(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	switch change := record.Value.(type) {
	case *tsdiff.Hypertable:
		return reverseHypertable(record, change)
	case *tsdiff.ContinuousAggregate:
		return reverseAggregate(record, change)
	default:
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a TimescaleDB change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
}

func reverseHypertable(record schemaext.ChangeRecord, change *tsdiff.Hypertable) (schemaext.Reversal, error) {
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &tsdiff.Hypertable{}
	projection := schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: tsschema.HypertableKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.Before, projection.Value = after, after.Clone()
	}
	if change.Before != nil {
		before, err := change.Before.Desired()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.After = before
	}
	strategy, limitations := "restore the previous partitioning", []string{
		"TimescaleDB has no statement that turns a hypertable back into an ordinary table or repartitions one; the table has to be dropped and recreated, and its rows are not restored.",
	}
	if change.Before != nil && change.After == nil {
		strategy = "partition the table again"
		limitations = []string{"Partitioning the table again requires it to be empty; rows written while it was ordinary are not moved into chunks."}
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy, Limitations: limitations}, nil
}

func reverseAggregate(record schemaext.ChangeRecord, change *tsdiff.ContinuousAggregate) (schemaext.Reversal, error) {
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := tsschema.ValidateContinuousAggregateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &tsdiff.ContinuousAggregate{}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: tsschema.ContinuousAggregateKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.Before, projection.Value = after, after.Clone()
	}
	if change.Before != nil {
		before, err := change.Before.Desired()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.After = before
	}
	var strategy string
	switch {
	case change.Before == nil:
		strategy = "drop the created continuous aggregate"
	case change.After == nil:
		strategy = "recreate the dropped continuous aggregate from its captured definition"
	default:
		strategy = "replace the continuous aggregate with its captured definition"
	}
	limitations := []string{"A continuous aggregate is recreated WITH NO DATA; its materialized history is rebuilt only by a refresh."}
	if change.Before == nil {
		limitations = []string{"Dropping the continuous aggregate discards its materialized data."}
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy, Limitations: limitations}, nil
}
