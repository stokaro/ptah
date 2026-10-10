package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalService reverses changes to external data sources and external
// tables by swapping their operands: a created object is dropped, a dropped
// one is created again as it was, and a replaced one is replaced back. No
// external object holds data in YDB, so a rollback restores each one
// completely. A table the forward plan created again over a recreated data
// source keeps equal operands, so the rollback creates it again too when it
// recreates the source.
type ExternalService struct{}

// ReverseChanges returns one reversal per change, in input order.
func (ExternalService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "external objects", capability.ExternalDataSources, reverseExternal)
}

func reverseExternal(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbexternal.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	switch change := record.Value.(type) {
	case *ydbdiff.ExternalDataSource:
		if schemaext.Kind(record.Subject.Kind) != ydbexternal.SourceKind {
			return schemaext.Reversal{}, fmt.Errorf("%w: a data source change names %s", schemaext.ErrInvalidValue, record.Subject.Kind)
		}
		if err := change.Validate(); err != nil {
			return schemaext.Reversal{}, err
		}
		reversed := &ydbdiff.ExternalDataSource{}
		var forward schemaext.Value
		if change.After != nil {
			reversed.Before, forward = change.After.Observed(), change.After.Observed()
		}
		if change.Before != nil {
			reversed.After = change.Before.Desired()
		}
		return externalReversal(record, ydbexternal.SourceKind, reversed, forward, "data source", change.Before == nil, change.After == nil), nil
	case *ydbdiff.ExternalTable:
		if schemaext.Kind(record.Subject.Kind) != ydbexternal.TableKind {
			return schemaext.Reversal{}, fmt.Errorf("%w: an external table change names %s", schemaext.ErrInvalidValue, record.Subject.Kind)
		}
		if err := change.Validate(); err != nil {
			return schemaext.Reversal{}, err
		}
		reversed := &ydbdiff.ExternalTable{}
		var forward schemaext.Value
		if change.After != nil {
			reversed.Before, forward = change.After.Observed(), change.After.Observed()
		}
		if change.Before != nil {
			reversed.After = change.Before.Desired()
		}
		return externalReversal(record, ydbexternal.TableKind, reversed, forward, "external table", change.Before == nil, change.After == nil), nil
	default:
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires an external object change", schemaext.ErrInvalidValue)
	}
}

// externalReversal records reversed as the rollback of record, with the
// state the forward change left, forward, nil where it dropped the object.
func externalReversal(record schemaext.ChangeRecord, kind schemaext.Kind, reversed schemaext.ChangeValue, forward schemaext.Value,
	family string, created, dropped bool,
) schemaext.Reversal {
	strategy := "replace the " + family + " with the definition it had"
	switch {
	case created:
		strategy = "drop the created " + family
	case dropped:
		strategy = "create the dropped " + family + " again with the definition it had"
	}
	return schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		Strategy:     strategy,
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: kind, Value: forward}},
	}
}
