package ydbrender

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationHandler renders one statement on an async replication:
// CREATE ASYNC REPLICATION with its items and the settings the declaration
// names, ALTER ASYNC REPLICATION for a connection or a credential that
// differs, or DROP ASYNC REPLICATION, with CASCADE unless the replication was
// failed over. A statement [ydbreplication.RefuseReplication] refuses is
// refused before anything is written.
func AsyncReplicationHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AsyncReplication{}, ast.StatementExtension,
		func(ctx renderer.ExtensionContext, value *ydbast.AsyncReplication) error {
			return validateReplicationStatement(ctx, value, "async replication ", capability.AsyncReplication,
				func(name string) *ydbreplication.Refusal {
					before, after, state := value.Change.Operands()
					return ydbreplication.RefuseReplication(name, before, after, state, ctx.Capabilities)
				})
		}, renderReplication)
}

// TransferHandler renders one statement on a transfer: CREATE TRANSFER with
// the lambda as written, ALTER TRANSFER for what YDB changes in place, or DROP
// TRANSFER. A statement [ydbreplication.RefuseTransfer] refuses is refused
// before anything is written.
func TransferHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.Transfer{}, ast.StatementExtension,
		func(ctx renderer.ExtensionContext, value *ydbast.Transfer) error {
			return validateReplicationStatement(ctx, value, "transfer ", capability.Transfers,
				func(name string) *ydbreplication.Refusal {
					before, after, state := value.Change.Operands()
					return ydbreplication.RefuseTransfer(name, before, after, state, ctx.Capabilities)
				})
		}, renderTransfer)
}

// replicationStatement is a statement on a replication or a transfer.
type replicationStatement interface {
	Validate() error
	Reference() string
}

// validateReplicationStatement refuses a statement that does not validate, any
// target but YDB by the kind's key, and what refuse refuses.
func validateReplicationStatement(ctx renderer.ExtensionContext, value replicationStatement, subject string,
	key capability.Capability, refuse func(name string) *ydbreplication.Refusal,
) error {
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	name := value.Reference()
	if ctx.Target != "ydb" {
		return (&ydbreplication.Refusal{Subject: subject + name, Key: key}).Err(ctx.Target)
	}
	return refuse(name).Err(ctx.Target)
}

func renderReplication(_ renderer.ExtensionContext, value *ydbast.AsyncReplication) ([]string, error) {
	name, change := value.Reference(), value.Change
	switch {
	case change.Before == nil:
		return []string{ydbreplication.CreateReplicationStatement(name, change.After.Spec)}, nil
	case change.After == nil:
		return []string{ydbreplication.DropReplicationStatement(name, change.Cascade())}, nil
	default:
		return []string{ydbreplication.AlterReplicationStatement(name, change.After.Spec, change.Before.Spec)}, nil
	}
}

func renderTransfer(_ renderer.ExtensionContext, value *ydbast.Transfer) ([]string, error) {
	name, change := value.Reference(), value.Change
	switch {
	case change.Before == nil:
		return []string{ydbreplication.CreateTransferStatement(name, change.After.Spec)}, nil
	case change.After == nil:
		return []string{ydbreplication.DropTransferStatement(name)}, nil
	default:
		return []string{ydbreplication.AlterTransferStatement(name, change.After.Spec, change.Before.Spec)}, nil
	}
}
