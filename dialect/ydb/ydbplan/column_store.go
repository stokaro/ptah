package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/ydbttl"
	"ptah.run/internal/ydbtype"
)

// ColumnStoreService plans column storage changes in place and accounts for
// the storage through table removal and rebuild. Its zero value supports
// concurrent use without database access.
//
// YDB fixes a table's storage kind, hash key and shard count when it creates
// the table, so a change of any of them is refused: it needs an explicit data
// migration. The tiered TTL changes in place. `ALTER TABLE t RESET (TTL)`
// removes the policy the table holds and `ALTER TABLE t SET (TTL = ...)`
// installs the declared one, and a changed policy takes both: YDB keeps no
// data source a table's TTL names, so the old policy lets go of its sources
// before their owner drops or replaces them, and the new one is set once its
// sources exist.
//
// Each statement reads the data sources its policy names, which orders it
// against their owner: the RESET is an early reader, placed before a
// replacement of the source, and the SET reads the source the replacement
// leaves. Both act on the table's TTL setting, the subject the YDB TTL's owner
// writes too: the RESET drops it and the SET creates it, so a table moving
// between a TTL that only deletes and a tiered one lets go of the old setting
// first.
type ColumnStoreService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
func (ColumnStoreService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.YDB {
		return featureplan.Result{}, fmt.Errorf("%w: YDB column storage planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{ydbschema.ColumnStoreKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported YDB column storage parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, err := planStoreChange(request, record, i)
		if err != nil {
			return storeRefusal(ydbdiff.ColumnStoreKind, err, new(i), nil), nil
		}
		result.Contributions = append(result.Contributions, contribution)
		plan := featureplan.ChangePlan{Subject: record.Subject, Kind: ydbdiff.ColumnStoreKind,
			Strategy: "reset the tiered TTL before its sources change and set the declared one after they exist"}
		for _, step := range contribution.Steps {
			plan.Steps = append(plan.Steps, step.ID)
		}
		result.Changes = append(result.Changes, plan)
	}
	if len(request.ParentKinds) == 0 {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		return result, nil
	}
	for i, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		strategy, err := assessStoreParent(table)
		if err != nil {
			return storeRefusal(ydbschema.ColumnStoreKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: ydbschema.ColumnStoreKind, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func storeRefusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	refusal := ttlRefusal(kind, err, change, parent)
	refusal.Diagnostics[0].Problem.Feature = "YDB column storage planning"
	return refusal
}

func planStoreChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], error) {
	var result plangraph.Contribution[featureplan.Operation]
	change, ok := record.Value.(*ydbdiff.ColumnStore)
	if !ok {
		return result, fmt.Errorf("%w: expected a YDB column storage change", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateColumnStore(change); err != nil {
		return result, err
	}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return result, fmt.Errorf("%w: a column storage change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if table.Action != "" && table.Action != featureplan.AlterTable {
		return result, fmt.Errorf("%w: a column storage change requires a table that survives the plan in place", schemaext.ErrInvalidValue)
	}
	if err := refuseStoreChange(request.Capabilities, table, change); err != nil {
		return result, err
	}
	result.Owner = ydbschema.Owner
	setting := table.Subject
	setting.Kind = objectidentity.Kind(ydbschema.TTLKind)
	before, after := change.Before.TTL, change.After.TTL
	// Zero-padded, because a scheduler orders independent steps by name and
	// the statements should keep the order of the changes.
	name := fmt.Sprintf("column-store/%06d", index)
	operation := func(policy *ydbschema.TieredTTL) featureplan.Operation {
		return featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: &ydbast.AlterColumnStoreTTL{Policy: policy.Clone()}}
	}
	impact := (&ydbast.AlterColumnStoreTTL{}).Effect()
	if before != nil {
		result.Steps = append(result.Steps, plangraph.Step[featureplan.Operation]{
			ID: plangraph.StepID{Owner: result.Owner, Name: name + "/reset"}, Payload: operation(nil),
			Transaction: plangraph.TransactionForbidden, Impact: impact, Placement: plangraph.PlacementEarly,
			Effects: slices.Concat([]plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: setting, Action: plangraph.Drop}},
				ydbscheme.TieredTTLReads(request.DatabasePath, before)),
		})
	}
	if after != nil {
		result.Steps = append(result.Steps, plangraph.Step[featureplan.Operation]{
			ID: plangraph.StepID{Owner: result.Owner, Name: name + "/set"}, Payload: operation(after),
			Transaction: plangraph.TransactionForbidden, Impact: impact,
			Effects: slices.Concat([]plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: setting, Action: plangraph.Create}},
				ydbscheme.TieredTTLReads(request.DatabasePath, after)),
		})
	}
	if len(result.Steps) == 2 {
		result.Dependencies = []plangraph.Dependency{{Before: result.Steps[0].ID, After: result.Steps[1].ID}}
	}
	return result, nil
}

// refuseStoreChange refuses, before anything is emitted, a change no
// statement makes in place: a conversion between row and column storage, a
// new hash key or shard count, a tiered TTL on a target without
// capability.TieredTTL, and a policy reading a column the table does not
// declare or of a type YDB reads no TTL from.
func refuseStoreChange(caps capability.Capabilities, table featureplan.Table, change *ydbdiff.ColumnStore) error {
	subject := "table " + quoted(ttlDisplayName(table.Subject))
	if !caps.Has(capability.ColumnStoreTables) {
		return ttlKey(capability.ColumnStoreTables, subject)
	}
	if change.Before == nil || change.After == nil {
		return ttlFact(subject, "changing between row and column storage requires an explicit data migration")
	}
	if !ydbschema.LayoutSatisfied(change.After.ColumnStore, change.Before.ColumnStore) {
		return ttlFact(subject, "changing a column table's hash key or shard count requires an explicit data migration")
	}
	if change.After.TTL == nil {
		return nil
	}
	if !caps.Has(capability.TieredTTL) {
		return ttlKey(capability.TieredTTL, subject)
	}
	return refuseTieredTTLColumn(caps, table.Desired, subject, change.After.TTL)
}

// refuseTieredTTLColumn holds the column a tiered TTL reads to the types YDB
// reads a TTL from, through the type map the table's columns are written with.
// A modification that carries no declaration of the table is left to the
// server, which refuses the statement by itself.
func refuseTieredTTLColumn(caps capability.Capabilities, declaration schemacapture.TableDeclaration, subject string, policy *ydbschema.TieredTTL) error {
	if !declaration.HasTable() {
		return nil
	}
	index := slices.IndexFunc(declaration.Fields, func(field schemamodel.Field) bool { return field.Name == policy.Column })
	if index < 0 {
		return ttlFact(subject, fmt.Sprintf("its TTL reads column %q, which the table does not declare "+
			"(`Cannot enable TTL on unknown column`)", policy.Column))
	}
	mapping, err := ydbtype.Map(declaration.Fields[index].Type, caps)
	if err != nil {
		// A type the map refuses is the column's refusal, which is reported
		// where the column is written.
		return nil
	}
	if reason := ydbttl.ColumnRefusal(policy.Column, mapping.Type, policy.Unit); reason != "" {
		return ttlFact(subject, reason)
	}
	return nil
}

// assessStoreParent accounts for the storage through a table operation.
// Dropping a table removes its storage with it, and a created table's CREATE
// TABLE writes it. The host rebuilds no column table: YDB copies none into a
// new one, so the plan refuses the rebuild before it asks. A table that
// survives keeps its storage.
func assessStoreParent(table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove the storage with the table", nil
	case featureplan.CreateTable:
		return "write the declared column storage into the CREATE TABLE", nil
	case featureplan.RebuildTable:
		return "rebuild a row table, which has no column storage", nil
	case featureplan.AlterTable:
		return "retain the storage unless a planned change in this plan changes its TTL", nil
	default:
		return "", fmt.Errorf("%w: YDB column storage has no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}
