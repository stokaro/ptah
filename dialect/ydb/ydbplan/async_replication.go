package ydbplan

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/ydbpath"
)

// AsyncReplicationService plans statements on YDB async replications.
//
// A drop is early, so it runs before anything else the plan changes, and
// with CASCADE it drops the replica tables at each target path, so a table the
// plan creates at one follows it. A creation or a change runs after the common
// statements: a creation creates a replica at each target path, so it follows
// a statement that drops what is there, and it reads the secret its
// connection names by path, as a change does, so the secret's owner creates
// the secret first. A change YDB cannot make in place, which depends on the
// state the replication reported, is refused.
type AsyncReplicationService struct{}

// TransferService plans statements on YDB transfers.
//
// A drop is early and reads what the transfer used: its table, and a topic or
// a changefeed of this database, so the transfer is gone before any of them
// goes. A creation or a change runs after the common statements and reads its
// table, its topic or changefeed, and its secrets, so each is in place first,
// and a change of the table or the topic comes before it.
type TransferService struct{}

// PlanFeatures returns one operation per created, changed or dropped
// replication. A refused change returns no operation from the batch.
func (AsyncReplicationService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return standalonePlanner[*ydbast.AsyncReplication]{family: "replication", scope: "async replication",
		kind: ydbdiff.AsyncReplicationKind, operations: replicationOperations, refusals: replicationRefusals,
		action: func(operation *ydbast.AsyncReplication) plangraph.Action {
			return changeAction(operation.Change.Before == nil, operation.Change.After == nil)
		},
		dependencies: replicationDependencies("async replication"), strategy: replicationStrategy,
		effects: replicationEffects,
		placed: func(operation *ydbast.AsyncReplication) plangraph.Placement {
			return dropsEarly[operation.Change.After == nil]
		},
	}.plan(ctx, request)
}

// PlanFeatures returns one operation per created, changed or dropped
// transfer. A refused change returns no operation from the batch.
func (TransferService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return standalonePlanner[*ydbast.Transfer]{family: "transfer", scope: "transfer",
		kind: ydbdiff.TransferKind, operations: transferOperations, refusals: transferRefusals,
		action: func(operation *ydbast.Transfer) plangraph.Action {
			return changeAction(operation.Change.Before == nil, operation.Change.After == nil)
		},
		dependencies: replicationDependencies("transfer"), strategy: transferStrategy,
		effects: transferEffects,
		placed: func(operation *ydbast.Transfer) plangraph.Placement {
			return dropsEarly[operation.Change.After == nil]
		},
	}.plan(ctx, request)
}

// PlanDeclarations derives one CREATE ASYNC REPLICATION per declared
// replication, sharing the owner's graph rules with migrations.
func (s AsyncReplicationService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return standaloneDeclarations{kind: ydbreplication.ReplicationKind, family: "async replication",
		strategy: "create the declared replication with its items",
		create: func(value schemaext.Value) (schemaext.ChangeValue, bool) {
			replication, ok := value.(*ydbreplication.DesiredReplication)
			if !ok || replication == nil {
				return nil, false
			}
			return &ydbdiff.AsyncReplication{After: replication}, true
		},
		plan: s.PlanFeatures,
	}.planDeclarations(ctx, request)
}

// PlanDeclarations derives one CREATE TRANSFER per declared transfer, sharing
// the owner's graph rules with migrations.
func (s TransferService) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return standaloneDeclarations{kind: ydbreplication.TransferKind, family: "transfer",
		strategy: "create the declared transfer",
		create: func(value schemaext.Value) (schemaext.ChangeValue, bool) {
			transfer, ok := value.(*ydbreplication.DesiredTransfer)
			if !ok || transfer == nil {
				return nil, false
			}
			return &ydbdiff.Transfer{After: transfer}, true
		},
		plan: s.PlanFeatures,
	}.planDeclarations(ctx, request)
}

// changeAction is the action of a change with no before or no after operand.
func changeAction(created, dropped bool) plangraph.Action {
	switch {
	case created:
		return plangraph.Create
	case dropped:
		return plangraph.Drop
	default:
		return plangraph.Alter
	}
}

// dropsEarly places a drop early and every other statement after the common
// statements. It is a lookup rather than a branch because the kind of
// statement selects a placement, not a code path.
var dropsEarly = map[bool]plangraph.Placement{true: plangraph.PlacementEarly, false: plangraph.PlacementDefault}

// replicationDependencies orders one statement against the handoffs at its
// own path and the directories above it; what it reads and the replica paths
// it writes are effects the host orders.
func replicationDependencies(family string) func(plangraph.StepID, objectidentity.ID, objectidentity.ID, plangraph.Action, commonSteps) ([]plangraph.Dependency, error) {
	return func(id plangraph.StepID, _, slot objectidentity.ID, action plangraph.Action, common commonSteps) ([]plangraph.Dependency, error) {
		return schemePathDependencies(family, id, slot, action, common.steps)
	}
}

func replicationOperations(ctx context.Context, request featureplan.Request) ([]standaloneChange[*ydbast.AsyncReplication], error) {
	return standaloneOperations(ctx, request, "async replication",
		func(ref objectidentity.ID, value schemaext.ChangeValue) (*ydbast.AsyncReplication, bool) {
			change, ok := value.(*ydbdiff.AsyncReplication)
			if !ok || schemaext.Kind(ref.Kind) != ydbreplication.ReplicationKind {
				return nil, false
			}
			return &ydbast.AsyncReplication{Schema: ref.Schema.Source, Name: ref.Name.Source, Change: *change}, true
		})
}

func transferOperations(ctx context.Context, request featureplan.Request) ([]standaloneChange[*ydbast.Transfer], error) {
	return standaloneOperations(ctx, request, "transfer",
		func(ref objectidentity.ID, value schemaext.ChangeValue) (*ydbast.Transfer, bool) {
			change, ok := value.(*ydbdiff.Transfer)
			if !ok || schemaext.Kind(ref.Kind) != ydbreplication.TransferKind {
				return nil, false
			}
			return &ydbast.Transfer{Schema: ref.Schema.Source, Name: ref.Name.Source, Change: *change}, true
		})
}

// validatedOperation is a statement that validates its identity and operands.
type validatedOperation interface {
	standalonePayload
	Validate() error
}

// standaloneOperations lowers the request's changes in input order through
// lower, refusing a change of another type, one named twice and one whose
// statement does not validate.
func standaloneOperations[P validatedOperation](ctx context.Context, request featureplan.Request, family string,
	lower func(objectidentity.ID, schemaext.ChangeValue) (P, bool),
) ([]standaloneChange[P], error) {
	changes := make([]standaloneChange[P], 0, len(request.Changes))
	seen := make(map[objectidentity.Key]bool)
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := record.Clone()
		if err != nil {
			return nil, err
		}
		operation, ok := lower(record.Subject, cloned.Value)
		if !ok || seen[record.Subject.Key()] {
			return nil, fmt.Errorf("%w: unexpected or duplicate %s change", schemaext.ErrInvalidValue, family)
		}
		seen[record.Subject.Key()] = true
		if err := operation.Validate(); err != nil {
			return nil, err
		}
		changes = append(changes, standaloneChange[P]{input: index, ref: record.Subject, operation: operation})
	}
	return changes, nil
}

// replicationRefusals refuses, before any operation is returned, a statement
// [ydbreplication.RefuseReplication] refuses: every one on a line without the
// async_replication key, a declaration the line cannot hold, and a change YDB
// cannot make in the state the replication reported.
func replicationRefusals(request featureplan.Request, changes []standaloneChange[*ydbast.AsyncReplication]) []featureplan.Diagnostic {
	return refusalDiagnostics(request, changes, ydbdiff.AsyncReplicationKind, func(operation *ydbast.AsyncReplication) *ydbreplication.Refusal {
		before, after, state := operation.Change.Operands()
		return ydbreplication.RefuseReplication(operation.Reference(), before, after, state, request.Capabilities)
	})
}

// transferRefusals refuses what [ydbreplication.RefuseTransfer] refuses.
func transferRefusals(request featureplan.Request, changes []standaloneChange[*ydbast.Transfer]) []featureplan.Diagnostic {
	return refusalDiagnostics(request, changes, ydbdiff.TransferKind, func(operation *ydbast.Transfer) *ydbreplication.Refusal {
		before, after, state := operation.Change.Operands()
		return ydbreplication.RefuseTransfer(operation.Reference(), before, after, state, request.Capabilities)
	})
}

// refusalDiagnostics is one diagnostic per change refuse refuses.
func refusalDiagnostics[P standalonePayload](request featureplan.Request, changes []standaloneChange[P], kind schemaext.Kind,
	refuse func(P) *ydbreplication.Refusal,
) []featureplan.Diagnostic {
	var diagnostics []featureplan.Diagnostic
	for _, change := range changes {
		if refusal := refuse(change.operation); refusal != nil {
			diagnostics = append(diagnostics, replicationDiagnostic(request, kind, change.input, change.ref, refusal))
		}
	}
	return diagnostics
}

// replicationDiagnostic refuses the change at input for refusal. A refusal
// without a key is a change YDB makes on no line, which is as unsupported as a
// line without the key.
func replicationDiagnostic(request featureplan.Request, kind schemaext.Kind, input int, ref objectidentity.ID, refusal *ydbreplication.Refusal) featureplan.Diagnostic {
	return featureplan.Diagnostic{Change: new(input), Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: string(kind), Object: ref.String(),
		Feature: string(refusal.Key), Message: refusal.Err(request.Target).Error(),
	}}
}

// replicationEffects names what a replication statement does beyond its own
// path: a creation creates a replica at each target, a drop with CASCADE drops
// them, and a creation or a change reads the secrets its connection names by
// path.
func replicationEffects(request featureplan.Request, operation *ydbast.AsyncReplication) ([]plangraph.Effect, error) {
	change := operation.Change
	switch {
	case change.After == nil && change.Cascade():
		return targetEffects(change.Before.Spec.Items, plangraph.Drop), nil
	case change.After == nil:
		return nil, nil
	}
	effects, err := ydbscheme.SecretPathReads(request.DatabasePath,
		change.After.Spec.Connection.TokenSecretPath, change.After.Spec.Connection.PasswordSecretPath)
	if err != nil {
		return nil, err
	}
	if change.Before == nil {
		effects = append(effects, targetEffects(change.After.Spec.Items, plangraph.Create)...)
	}
	return effects, nil
}

// targetEffects is action on the scheme path of each item's target.
func targetEffects(items []ydbreplication.Item, action plangraph.Action) []plangraph.Effect {
	var effects []plangraph.Effect
	seen := make(map[objectidentity.Key]bool)
	for _, item := range items {
		ref, ok := schemePath(item.Target)
		if !ok || seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		effects = append(effects, plangraph.Effect{Subject: ref, Action: action})
	}
	return effects
}

// transferEffects names what a transfer statement reads beyond its own path:
// the table it writes, the topic or changefeed of this database it reads, and,
// for a creation or a change, the secrets its connection names by path. A drop
// reads what the transfer it drops used.
func transferEffects(request featureplan.Request, operation *ydbast.Transfer) ([]plangraph.Effect, error) {
	change := operation.Change
	if change.After == nil {
		return transferUses(request.DatabasePath, change.Before.Spec), nil
	}
	effects, err := ydbscheme.SecretPathReads(request.DatabasePath,
		change.After.Spec.Connection.TokenSecretPath, change.After.Spec.Connection.PasswordSecretPath)
	if err != nil {
		return nil, err
	}
	return append(effects, transferUses(request.DatabasePath, change.After.Spec)...), nil
}

// transferUses is a read of the table spec writes, and of the topic or the
// changefeed of the database at root it reads: a source path names a
// standalone topic or a changefeed of a table, and only the plan knows which
// it holds, so it reads both and the one nothing touches orders nothing.
func transferUses(root string, spec ydbreplication.TransferSpec) []plangraph.Effect {
	var effects []plangraph.Effect
	if table, ok := schemePath(spec.Target); ok {
		effects = append(effects, plangraph.Effect{Subject: table, Action: plangraph.Read})
	}
	if !ydbreplication.LocalSource(spec) {
		return effects
	}
	topics := ydbscheme.TopicPathReads(root, spec.Source)
	effects = append(effects, topics...)
	for _, topic := range topics {
		directory := topic.Subject.Schema.Source
		if directory == "" {
			continue
		}
		schema, table := "", directory
		if slash := strings.LastIndex(directory, "/"); slash >= 0 {
			schema, table = directory[:slash], directory[slash+1:]
		}
		effects = append(effects, plangraph.Effect{Subject: ydbschema.ChangefeedRef(schema, table, topic.Subject.Name.Source),
			Action: plangraph.Read})
	}
	return effects
}

// schemePath is the scheme path of a path relative to the database root, and
// false for one that names no object there.
func schemePath(written string) (objectidentity.ID, bool) {
	schema, name, err := ydbpath.Split(written)
	if err != nil {
		return objectidentity.ID{}, false
	}
	return ydbscheme.Path(schema, name), true
}

func replicationStrategy(operation *ydbast.AsyncReplication) string {
	change := operation.Change
	switch {
	case change.Before == nil:
		return "create the replication, which creates its replica tables and copies its source"
	case change.After == nil && change.Cascade():
		return "drop the replication with its replica tables"
	case change.After == nil:
		return "drop the failed-over replication and keep its tables"
	default:
		return "point the paused replication at the declared connection and credential"
	}
}

func transferStrategy(operation *ydbast.Transfer) string {
	change := operation.Change
	switch {
	case change.Before == nil:
		return "create the transfer, which reads its topic into its table"
	case change.After == nil:
		return "drop the transfer and the consumer YDB created for it"
	default:
		return "change the transfer's lambda, batch settings or connection in place"
	}
}
