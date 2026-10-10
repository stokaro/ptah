package ydbplan

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// IndexPartitioningService plans a change of a global index's partitioning in
// place, one `ALTER TABLE t ALTER INDEX i SET (...)` naming every splitting
// setting whenever it changes one, and accounts for the settings of a table's
// indexes through table removal, creation and rebuild. A rebuilt table's
// indexes take [RebuiltIndexPartitioning]. Its zero value supports concurrent
// use without database access.
type IndexPartitioningService struct{}

var indexPartitioningPlanning = tableFacetPlanning{
	name: "YDB index partitioning", facet: ydbschema.IndexPartitioningKind, change: ydbdiff.IndexPartitioningKind,
	plan: planIndexPartitioningChange, assess: assessIndexPartitioningParent,
}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation.
func (IndexPartitioningService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return indexPartitioningPlanning.planFeatures(ctx, request)
}

func planIndexPartitioningChange(request featureplan.Request, record schemaext.ChangeRecord, index int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var result plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: ydbdiff.IndexPartitioningKind,
		Strategy: "set the index's settings in place, naming every splitting setting the change touches"}
	change, ok := record.Value.(*ydbdiff.IndexPartitioning)
	if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
		return result, plan, fmt.Errorf("%w: expected a YDB index partitioning change on a table-owned index", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateIndexPartitioning(change); err != nil {
		return result, plan, err
	}
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: record.Subject.Catalog, Schema: record.Subject.Schema, Name: record.Subject.Parent}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == parent.Key() })
	if position < 0 {
		return result, plan, fmt.Errorf("%w: an index partitioning change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	switch table.Action {
	case "", featureplan.AlterTable:
	case featureplan.RebuildTable:
		// The new table's index is created with every setting, the changed
		// ones among them; see [RebuiltIndexPartitioning].
		plan.Strategy = "create the rebuilt table's index with the declared settings"
		return result, plan, nil
	default:
		return result, plan, fmt.Errorf("%w: an index partitioning change requires a table that survives the plan or is rebuilt", schemaext.ErrInvalidValue)
	}
	subject := fmt.Sprintf("index %q of table %q", record.Subject.Name.Source, ttlDisplayName(table.Subject))
	if !request.Capabilities.Has(capability.IndexPartitioning) {
		return result, plan, ttlKey(capability.IndexPartitioning, "changing the partitioning of "+subject)
	}
	payload := &ydbast.AlterIndexPartitioning{Index: record.Subject.Name.Source, Change: *change.Copy()}
	settings, err := payload.Settings()
	if err != nil {
		return result, plan, ttlFact(subject, err.Error())
	}
	if len(settings) == 0 {
		plan.Strategy = "nothing to change: the index holds every setting the declaration states"
		return result, plan, nil
	}
	result.Owner = ydbschema.Owner
	result.Steps = []plangraph.Step[featureplan.Operation]{{
		ID:          plangraph.StepID{Owner: result.Owner, Name: fmt.Sprintf("index-partitioning/%06d", index)},
		Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: payload},
		Transaction: plangraph.TransactionForbidden,
		Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Alter}},
		Impact:      payload.Effect(),
	}}
	plan.Steps = []plangraph.StepID{result.Steps[0].ID}
	return result, plan, nil
}

// assessIndexPartitioningParent accounts for the settings of a table's
// indexes through a table operation.
func assessIndexPartitioningParent(_ capability.Capabilities, table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove each index's settings with the table", nil
	case featureplan.CreateTable:
		return "set each index's declared settings after the table is created", nil
	case featureplan.RebuildTable:
		return "set every setting each index of the rebuilt table holds: the declared ones, and the held value of every other one", nil
	case featureplan.AlterTable:
		return "retain each index's settings unless a planned change in this plan changes them", nil
	default:
		return "", fmt.Errorf("%w: YDB index partitioning has no plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
	}
}

// RebuiltIndexPartitioning is the settings an index of a rebuilt table is
// given, for the reason [RebuiltTablePartitioning] gives for the table: each
// setting the declaration names, and the held value of every other one, all
// named. held is the index as the read found it under its old name, or the
// zero index where it held none. A side YDB could not hold, and an invalid
// facet, is an error saying why.
func RebuiltIndexPartitioning(declared schemamodel.Index, held catalog.Index) (*ydbschema.IndexPartitioning, error) {
	stated, _, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](declared.Facets, ydbschema.IndexPartitioningKind)
	if err != nil {
		return nil, err
	}
	observed, _, err := schemaext.FacetAs[*ydbschema.ObservedIndexPartitioning](held.Facets, ydbschema.IndexPartitioningKind)
	if err != nil {
		return nil, err
	}
	var holds, states *ydbschema.IndexPartitioning
	if observed != nil {
		holds = &observed.IndexPartitioning
	}
	if stated != nil {
		states = &stated.IndexPartitioning
	}
	current, err := ydbindex.Held(holds)
	if err != nil {
		return nil, fmt.Errorf("the settings it holds: %w", err)
	}
	settings, err := ydbindex.Resolve(states, current)
	if err != nil {
		return nil, err
	}
	return ydbindex.Explicit(settings), nil
}
