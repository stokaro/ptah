package ydbplan

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// TablePartitioningService plans a change of a row table's settings -- how it
// splits into partitions, its read replicas and its key bloom filter -- in
// place, and accounts for the settings through table removal, creation and
// rebuild. Its zero value supports concurrent use without database access.
//
// `ALTER TABLE t SET (...)` changes the settings in place, one statement per
// table. A setting the declaration leaves out keeps what the table holds, so
// the statement names what the declaration names and the held value of every
// other setting of each group it touches: setting one resets others (see
// [ydbpartition.TableClause]). The settings depend on no column, so the
// statement goes with the table's other in-place changes.
//
// A starting layout (UNIFORM_PARTITIONS, PARTITION_AT_KEYS) is taken only by
// CREATE TABLE. A change that asks for one the table was not created with is
// refused here; the YDB host plans it as a rebuild when the caller asks for
// one (see [TablePartitioningRebuildReason]), and then a rebuilt table's
// CREATE TABLE writes [RebuiltTablePartitioning].
type TablePartitioningService struct{}

var tablePartitioningPlanning = tableFacetPlanning{
	name: "YDB table partitioning", facet: ydbschema.TablePartitioningKind, change: ydbdiff.TablePartitioningKind,
	plan: planTablePartitioningChange, assess: assessTablePartitioningParent,
}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
func (TablePartitioningService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return tablePartitioningPlanning.planFeatures(ctx, request)
}

func planTablePartitioningChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var result plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: ydbdiff.TablePartitioningKind,
		Strategy: "set the settings in place, naming every setting of each group the change touches"}
	change, ok := record.Value.(*ydbdiff.TablePartitioning)
	if !ok {
		return result, plan, fmt.Errorf("%w: expected a YDB table partitioning change", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateTablePartitioning(change); err != nil {
		return result, plan, err
	}
	table, err := surviving(request, record, "table partitioning")
	if err != nil {
		return result, plan, err
	}
	subject := "table " + quoted(ttlDisplayName(table.Subject))
	if err := refuseTablePartitioningChange(request.Capabilities, subject, change); err != nil {
		return result, plan, err
	}
	payload := &ydbast.AlterTablePartitioning{Change: *change.Copy()}
	settings, err := payload.Settings()
	if err != nil {
		return result, plan, ttlFact(subject, err.Error())
	}
	if len(settings) == 0 {
		plan.Strategy = "nothing to change: the table holds every setting the declaration states"
		return result, plan, nil
	}
	result.Owner = ydbschema.Owner
	partitioning := table.Subject
	partitioning.Kind = objectidentity.Kind(ydbschema.TablePartitioningKind)
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		// Zero-padded, because a scheduler orders independent steps by name and
		// the statements should keep the order of the changes.
		ID:          plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("table-partitioning/%06d", index)},
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload},
		Transaction: plangraph.TransactionForbidden,
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: partitioning, Action: plangraph.Alter}},
		Impact:      payload.Effect(),
	}}
	plan.Steps = []plangraph.StepID{result.Steps[0].ID}
	return result, plan, nil
}

// refuseTablePartitioningChange refuses, before anything is emitted, a change
// of subject's settings this target cannot make in place: a setting either
// side states that the target has no key for -- a change that removes read
// replicas needs the key as one that adds them does -- a side YDB could not
// hold, and a starting layout the table was not created with.
func refuseTablePartitioningChange(caps capability.Capabilities, subject string, change *ydbdiff.TablePartitioning) error {
	sides := []*ydbschema.TablePartitioning{&change.After.TablePartitioning}
	if change.Before != nil {
		sides = append(sides, &change.Before.TablePartitioning)
	}
	for _, side := range sides {
		for _, requirement := range ydbpartition.Requirements(side) {
			if !caps.Has(requirement.Key) {
				return ttlKey(requirement.Key, "changing the "+requirement.Settings+" of "+subject)
			}
		}
	}
	operation := &ydbast.AlterTablePartitioning{Change: *change}
	held, desired, err := operation.Resolve()
	if err != nil {
		return ttlFact(subject, err.Error())
	}
	if reason := ydbpartition.TableChangeRefusal(&change.After.TablePartitioning, desired, held); reason != "" {
		return ttlFact(subject, reason)
	}
	return nil
}

// TablePartitioningRebuildReason says why a change of a table's settings can
// be made only by recreating the table, or returns "": it asks for a starting
// layout the table was not created with (see [ydbpartition.TableChangeRefusal]).
// A nil change, and one with a side YDB could not hold, which is refused,
// answer "".
func TablePartitioningRebuildReason(change *ydbdiff.TablePartitioning) string {
	if change == nil || change.After == nil {
		return ""
	}
	operation := &ydbast.AlterTablePartitioning{Change: *change}
	held, desired, err := operation.Resolve()
	if err != nil {
		return ""
	}
	return ydbpartition.TableChangeRefusal(&change.After.TablePartitioning, desired, held)
}

// assessTablePartitioningParent accounts for the settings through a table
// operation. Dropping a table removes its settings with it, and a created
// table's CREATE TABLE writes them. A rebuilt table's CREATE TABLE writes
// [RebuiltTablePartitioning], which is held to the target here so the plan
// refuses before any statement rather than when the rebuild is rendered. A
// table that survives keeps its settings unless a change in the same plan
// changes them.
func assessTablePartitioningParent(caps capability.Capabilities, table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove the settings with the table", nil
	case featureplan.CreateTable:
		return "write the declared settings into the CREATE TABLE", nil
	case featureplan.RebuildTable:
		subject := "rebuilding table " + quoted(ttlDisplayName(table.Subject))
		settings, err := RebuiltTablePartitioning(table.Desired, table.Current)
		if err != nil {
			return "", ttlFact(subject, err.Error())
		}
		for _, requirement := range ydbpartition.Requirements(settings) {
			if !caps.Has(requirement.Key) {
				return "", ttlKey(requirement.Key, subject+" with its "+requirement.Settings)
			}
		}
		return "write every setting into the rebuilt table: the declared ones, and the held value of every other one", nil
	case featureplan.AlterTable:
		return "retain the settings unless a planned change in this plan changes them", nil
	default:
		return "", fmt.Errorf("%w: YDB table partitioning has no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

// RebuiltTablePartitioning is the settings the CREATE TABLE of a rebuild
// writes for a table declared as declaration whose read is observation: each
// setting the declaration names, and the held value of every other one, all
// named, so the new table takes none from the cluster's table profile.
// Measured on 25.1.4.7 and 26.2.1.14, a table created under a dynamic
// configuration of the cluster splits by size where it would not otherwise,
// so a new table left to the profile could change a setting nobody declared.
// The starting layout is the declaration's. A side YDB could not hold, and an
// invalid facet, is an error saying why.
func RebuiltTablePartitioning(declaration schemacapture.TableDeclaration, observation schemacapture.TableObservation) (*ydbschema.TablePartitioning, error) {
	declared, _, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](declaration.Table.Facets, ydbschema.TablePartitioningKind)
	if err != nil {
		return nil, err
	}
	observed, _, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](observation.Table.Facets, ydbschema.TablePartitioningKind)
	if err != nil {
		return nil, err
	}
	var stated, holds *ydbschema.TablePartitioning
	if declared != nil {
		stated = &declared.TablePartitioning
	}
	if observed != nil {
		holds = &observed.TablePartitioning
	}
	current, err := ydbpartition.HeldTable(holds)
	if err != nil {
		return nil, fmt.Errorf("the settings it holds: %w", err)
	}
	settings, err := ydbpartition.ResolveTable(stated, current)
	if err != nil {
		return nil, err
	}
	rebuilt := settings.Explicit()
	if stated != nil {
		layout := stated.Clone()
		rebuilt.UniformPartitions, rebuilt.PartitionAtKeys = layout.UniformPartitions, layout.PartitionAtKeys
	}
	return rebuilt, nil
}
