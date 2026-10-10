package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationService reverses async replication changes. A created
// replication is dropped with its replica tables, and one dropped with CASCADE
// is created again, which copies its source from the start. A failed-over
// replication the forward plan dropped is not created again: it replicated
// nothing more, and its tables stayed. A change in place is undone in place,
// in the state the forward change ran in, except for a credential the change
// added, which YDB cannot take away. Every loss is reported.
type AsyncReplicationService struct{}

// TransferService reverses transfer changes. A created transfer is dropped,
// and a dropped one is created again; a change in place is undone in place,
// except for a credential the change added. Rows a transfer wrote stay as
// written, and every loss of a consumer's position is reported.
type TransferService struct{}

// ReverseChanges returns one reversal per change, in input order.
func (AsyncReplicationService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "async replications", capability.AsyncReplication, reverseReplication)
}

// ReverseChanges returns one reversal per change, in input order.
func (TransferService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseStandalone(ctx, request, "transfers", capability.Transfers, reverseTransfer)
}

func reverseReplication(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbreplication.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.AsyncReplication)
	if !ok || schemaext.Kind(record.Subject.Kind) != ydbreplication.ReplicationKind {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires an async replication change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	path := ydbreplication.Display(record.Subject.Schema.Source, record.Subject.Name.Source)
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbreplication.ReplicationKind}
	reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject}}
	switch {
	case change.Before == nil:
		projection.Value = change.After.Observed("")
		reversal.Change.Value = &ydbdiff.AsyncReplication{Before: change.After.Observed("")}
		reversal.Strategy = "drop the created replication with its replica tables"
		reversal.Limitations = []string{fmt.Sprintf("dropping async replication %s drops the replica tables it created, "+
			"with every row it copied", path)}
	case change.After == nil && !change.Cascade():
		reversal.Strategy = "leave the failed-over replication dropped"
		reversal.Limitations = []string{fmt.Sprintf("async replication %s was failed over before it was dropped, so it "+
			"is not created again: it replicated nothing more, and its tables stayed as ordinary tables", path)}
	case change.After == nil:
		reversal.Change.Value = &ydbdiff.AsyncReplication{After: change.Before.Desired()}
		reversal.Strategy = "create the dropped replication again with its connection and items"
		reversal.Limitations = []string{fmt.Sprintf("the replica tables async replication %s dropped are created again "+
			"empty, and the replication copies its source again from the start", path)}
		if change.Before.State == ydbreplication.StatePaused {
			reversal.Limitations = append(reversal.Limitations, fmt.Sprintf("async replication %s was paused when it "+
				"was dropped, and runs once it is created again", path))
		}
	default:
		projection.Value = change.After.Observed(change.Before.State)
		target := ydbreplication.ReplicationRollbackTarget(change.Before.Spec, change.After.Spec)
		reversal.Strategy = "restore the replication's connection and credential in place"
		if !ydbreplication.ReplicationsEqual(target, change.After.Spec) {
			reversal.Change.Value = &ydbdiff.AsyncReplication{Before: change.After.Observed(change.Before.State),
				After: &ydbreplication.DesiredReplication{Spec: target}}
		}
		if target.Connection != change.Before.Spec.Connection {
			reversal.Limitations = []string{fmt.Sprintf("the credential the change gave async replication %s stays, "+
				"since YDB has no statement that takes a credential away", path)}
		}
		if len(reversal.Limitations) == 0 && reversal.Change.Value == nil {
			return schemaext.Reversal{}, fmt.Errorf("%w: async replication operands contain no change", schemaext.ErrInvalidValue)
		}
	}
	reversal.ForwardState = []schemaext.ProjectedValue{projection}
	return reversal, nil
}

func reverseTransfer(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ydbreplication.ValidateIdentity(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := record.Value.(*ydbdiff.Transfer)
	if !ok || schemaext.Kind(record.Subject.Kind) != ydbreplication.TransferKind {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a transfer change", schemaext.ErrInvalidValue)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	path := ydbreplication.Display(record.Subject.Schema.Source, record.Subject.Name.Source)
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbreplication.TransferKind}
	reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject}}
	switch {
	case change.Before == nil:
		projection.Value = change.After.Observed("")
		reversal.Change.Value = &ydbdiff.Transfer{Before: change.After.Observed("")}
		reversal.Strategy = "drop the created transfer"
		reversal.Limitations = []string{fmt.Sprintf("the rows transfer %s wrote into table %s stay", path, change.After.Spec.Target)}
		if change.After.Spec.Consumer == "" {
			reversal.Limitations = append(reversal.Limitations, fmt.Sprintf("dropping transfer %s drops the consumer "+
				"YDB created for it, with its position in topic %s", path, change.After.Spec.Source))
		}
	case change.After == nil:
		reversal.Change.Value = &ydbdiff.Transfer{After: change.Before.Desired()}
		reversal.Strategy = "create the dropped transfer again with its lambda and settings"
		reversal.Limitations = []string{fmt.Sprintf("dropping transfer %s removed the consumer YDB created for it, if it "+
			"had one, with its position in topic %s; the transfer created again does not resume from it",
			path, change.Before.Spec.Source)}
	default:
		projection.Value = change.After.Observed(change.Before.State)
		target := ydbreplication.TransferRollbackTarget(change.Before.Spec, change.After.Spec)
		reversal.Strategy = "restore the transfer's lambda, batch settings and connection in place"
		if !ydbreplication.TransfersEqual(target, change.After.Spec) {
			reversal.Change.Value = &ydbdiff.Transfer{Before: change.After.Observed(change.Before.State),
				After: &ydbreplication.DesiredTransfer{Spec: target}}
		}
		if ydbreplication.CompareTransfer(change.After.Spec, change.Before.Spec).Lambda {
			reversal.Limitations = append(reversal.Limitations, fmt.Sprintf("the rows transfer %s wrote with the "+
				"changed lambda stay as written", path))
		}
		if target.Connection != change.Before.Spec.Connection {
			reversal.Limitations = append(reversal.Limitations, fmt.Sprintf("the credential the change gave transfer "+
				"%s stays, since YDB has no statement that takes a credential away", path))
		}
		if len(reversal.Limitations) == 0 && reversal.Change.Value == nil {
			return schemaext.Reversal{}, fmt.Errorf("%w: transfer operands contain no change", schemaext.ErrInvalidValue)
		}
	}
	reversal.ForwardState = []schemaext.ProjectedValue{projection}
	return reversal, nil
}
