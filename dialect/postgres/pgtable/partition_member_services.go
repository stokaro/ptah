package pgtable

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// PartitionMemberService converts, compares and plans partition membership using
// captured data. Its zero value is ready for concurrent use and performs no
// database work.
type PartitionMemberService struct{}

// ConvertFeatures carries a partition's parent and bound across representations.
// A normalization fact stays behind: it describes one declaration and one server.
// Cancellation, invalid values, or invalid directions return no partial batch.
func (PartitionMemberService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertTableFeatures(ctx, request, "partition member", convertPartitionMember)
}

func convertPartitionMember(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch p := value.(type) {
	case *DesiredPartitionMember:
		if from == schemaext.Desired {
			if err := ValidateDesiredPartitionMember(p); err != nil {
				return nil, err
			}
			observed := ObservedPartitionMember(p.PartitionMember)
			return &observed, nil
		}
	case *ObservedPartitionMember:
		if from == schemaext.Observed {
			if err := ValidateObservedPartitionMember(p); err != nil {
				return nil, err
			}
			return &DesiredPartitionMember{PartitionMember: PartitionMember(*p)}, nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s partition member value %T", schemaext.ErrInvalidValue, from, value)
}

// CompareFacets reports a held table whose place in a partition tree changes: a
// partition with another bound or another parent, a partition on one side and an
// ordinary table on the other. Each is a [PartitionMemberChange], which the plan
// carries out by detaching and attaching the table. A declared partition whose
// parent the desired schema removes is refused, because dropping a partitioned
// table drops its partitions. A bound spelled differently and not normalized by
// a connected server stays undecided, and a source with no syntax for a partition
// asks nothing by leaving it out.
func (PartitionMemberService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{PartitionMemberKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid %s comparison kinds", schemaext.ErrInvalidValue, PartitionMemberKind)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		change, reason, err := comparePartitionMember(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: PartitionMemberKind,
				Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: PartitionMemberKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completedPersistence(ctx, result)
}

func comparePartitionMember(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*PartitionMemberChange, string, error) {
	if !HoldsPersistence(owner.Subject) {
		return nil, "", fmt.Errorf("%w: partition member requires a table subject", schemaext.ErrInvalidValue)
	}
	desired, current, err := partitionMemberOperands(request, owner.Subject)
	if err != nil {
		return nil, "", err
	}
	if !request.Includes(PartitionMemberKind, owner.Subject) {
		return nil, "", nil
	}
	if desired != nil {
		if err := RefuseForeignPartitionMember(request.Target); err != nil {
			return nil, "", err
		}
		if err := refuseRemovedPartitionParent(request, owner.Subject, desired); err != nil {
			return nil, "", err
		}
	}
	if !owner.Current || !owner.Desired {
		return nil, "", nil
	}
	switch {
	case desired != nil && current != nil:
		return comparePartitionMembers(request, desired, current)
	case desired != nil:
		if !describesPartitionMembers(request.Current.Coverage, owner.Subject) {
			return nil, "the current source does not say whether the table is a partition", nil
		}
		return (&PartitionMemberChange{After: desired}).Copy(), "", nil
	case current != nil:
		if !describesPartitionMembers(request.Desired.Coverage, owner.Subject) {
			return nil, "", nil
		}
		return (&PartitionMemberChange{Before: current}).Copy(), "", nil
	default:
		return nil, "", nil
	}
}

// refuseRemovedPartitionParent refuses a declared partition whose parent is held
// and undeclared. Dropping a partitioned table drops its partitions, so the plan
// would destroy what the declaration keeps. A parent outside the comparison, such
// as one in a schema the run does not read, is the server's to resolve.
func refuseRemovedPartitionParent(request schemaext.FacetComparisonRequest, subject objectidentity.ID, desired *DesiredPartitionMember) error {
	parent := objectidentity.NewBuilder(request.Identifiers).TableParts(desired.ParentSchema, desired.Parent)
	for _, owner := range request.Owners {
		if owner.Subject.Key() == parent.Key() && owner.Current && !owner.Desired {
			return fmt.Errorf("%w: PostgreSQL table %s is declared a partition of %s, which the database holds and the desired schema does not declare; dropping a partitioned table drops its partitions",
				ptaherr.ErrUnsupportedFeature, subject.Path(), parent.Path())
		}
	}
	return nil
}

// comparePartitionMembers compares a partition with its declaration: another
// parent, or a bound the connected server spells otherwise, is a change.
func comparePartitionMembers(request schemaext.FacetComparisonRequest, desired *DesiredPartitionMember, current *ObservedPartitionMember) (*PartitionMemberChange, string, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	moved := &PartitionMemberChange{Before: current, After: desired}
	if builder.TableParts(desired.ParentSchema, desired.Parent).Key() != builder.TableParts(current.ParentSchema, current.Parent).Key() {
		return moved.Copy(), "", nil
	}
	held := PartitionBoundKey(current.Bound)
	if desired.Normalized != nil {
		if desired.Normalized.Key == held {
			return nil, "", nil
		}
		return moved.Copy(), "", nil
	}
	if PartitionBoundKey(desired.Bound) == held {
		return nil, "", nil
	}
	return nil, "the partition bound needs normalization by the connected server", nil
}

func describesPartitionMembers(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	return slices.Contains([]schemaext.KnowledgeState{schemaext.Complete, schemaext.Absent, schemaext.Defaulted}, coverage.Lookup(PartitionMemberKind, subject).State)
}

func partitionMemberOperands(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*DesiredPartitionMember, *ObservedPartitionMember, error) {
	desired, _, err := schemaext.FacetAs[*DesiredPartitionMember](tableFacets(request.Desired, subject), PartitionMemberKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*ObservedPartitionMember](tableFacets(request.Current, subject), PartitionMemberKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := ValidateDesiredPartitionMember(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := ValidateObservedPartitionMember(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// PlanFeatures plans each [PartitionMemberChange] as a detachment from the parent
// the table holds and an attachment to the declared one, and accounts for
// membership on parent table actions: creation carries the parent and the bound
// on CREATE TABLE ... PARTITION OF, removal drops the partition with the rows it
// holds, and alteration retains both unless a planned move replaces them. A
// rebuild of a partition is refused, because it would need a partition-tree
// strategy.
//
// Every detachment of the plan runs before every attachment, so two partitions
// that trade bounds do not overlap on the way. A detachment runs before the
// table's other changes, which a partition refuses, and an attachment after
// them and after the parent's, so the columns agree when the server checks them.
// Errors and cancellation discard all receipts.
func (PartitionMemberService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{PartitionMemberKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: %s planning takes only its own parent kind", schemaext.ErrInvalidValue, PartitionMemberKind)
	}
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	var detached, attached []plangraph.StepID
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		steps, err := planPartitionMove(request, record, i)
		if err != nil {
			return completedPersistence(ctx, partitionMoveRefusal(err, i))
		}
		plan := featureplan.ChangePlan{Subject: record.Subject, Kind: PartitionMemberChangeKind, Strategy: "detach the table from the parent it holds and attach it to the declared one"}
		for _, step := range steps {
			contribution.Steps = append(contribution.Steps, step)
			plan.Steps = append(plan.Steps, step.ID)
			if step.Payload.Payload.(*MovePartition).Step == PartitionDetach {
				detached = append(detached, step.ID)
			} else {
				attached = append(attached, step.ID)
			}
		}
		result.Changes = append(result.Changes, plan)
	}
	for _, before := range detached {
		for _, after := range attached {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: before, After: after})
		}
	}
	if len(contribution.Steps) > 0 {
		result.Contributions = append(result.Contributions, contribution)
	}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		strategy, err := partitionMemberParent(request.Target, table)
		if err != nil {
			return featureplan.Result{}, err
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: PartitionMemberKind, Action: table.Action, Strategy: strategy})
	}
	return completedPersistence(ctx, result)
}

// planPartitionMove returns the statements of one move, the detachment first.
func planPartitionMove(request featureplan.Request, record schemaext.ChangeRecord, index int) ([]plangraph.Step[featureplan.Operation], error) {
	change, ok := record.Value.(*PartitionMemberChange)
	if !ok || !HoldsPersistence(record.Subject) {
		return nil, fmt.Errorf("%w: partition move requires a table subject and a partition member change", schemaext.ErrInvalidValue)
	}
	if err := ValidatePartitionMemberChange(change); err != nil {
		return nil, err
	}
	if change.After != nil {
		if err := RefuseForeignPartitionMember(request.Target); err != nil {
			return nil, err
		}
	}
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return table.Subject.Key() == record.Subject.Key() })
	if position < 0 {
		return nil, fmt.Errorf("%w: partition move requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[position]
	if !table.Desired.HasTable() || !table.Current.HasTable() || table.Action != "" && table.Action != featureplan.AlterTable {
		return nil, fmt.Errorf("%w: PostgreSQL table %s moves in its partition tree only while it is kept; a table the plan %s cannot be detached or attached",
			ptaherr.ErrUnsupportedFeature, table.Subject.Path(), table.Action)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	membership := table.Subject
	membership.Kind = objectidentity.Kind(PartitionMemberKind)
	var steps []plangraph.Step[featureplan.Operation]
	add := func(step string, parent objectidentity.ID, placement plangraph.Placement) {
		operation := &MovePartition{Change: *change.Copy(), Step: step}
		steps = append(steps, plangraph.Step[featureplan.Operation]{
			ID:      plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("partition-move/%06d/%s", index, step)},
			Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: operation},
			Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: table.Subject, Action: plangraph.Read},
				{Subject: membership, Action: plangraph.Alter}},
			Transaction: plangraph.TransactionAllowed, Impact: operation.Effect(), Placement: placement,
		})
	}
	if change.Before != nil {
		add(PartitionDetach, builder.TableParts(change.Before.ParentSchema, change.Before.Parent), plangraph.PlacementEarly)
	}
	if change.After != nil {
		add(PartitionAttach, builder.TableParts(change.After.ParentSchema, change.After.Parent), "")
	}
	return steps, nil
}

// partitionMoveRefusal reports a move the plan cannot carry out.
func partitionMoveRefusal(err error, change int) featureplan.Result {
	code := schemavalidation.UnsupportedFeature
	if errors.Is(err, schemaext.ErrInvalidValue) {
		code = schemavalidation.InvalidSchema
	}
	return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: code, Kind: string(PartitionMemberChangeKind), Feature: "PostgreSQL table partitions", Message: err.Error()},
		Change:  new(change),
	}}}
}

// ReverseChanges swaps each move's operands, so a rollback returns the table to
// the parent and bound it held.
func (PartitionMemberService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*PartitionMemberChange)
		if !ok || !HoldsPersistence(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires a partition member change on a table", schemaext.ErrInvalidValue)
		}
		inverse, err := change.Reverse()
		if err != nil {
			return nil, err
		}
		reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse},
			Strategy:     "return the table to the parent and bound it held",
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: PartitionMemberKind}}}
		if inverse.Before != nil {
			reversal.ForwardState[0].Value = new(*inverse.Before)
		}
		result = append(result, reversal)
	}
	return completedPersistence(ctx, result)
}

func partitionMemberParent(target string, table featureplan.Table) (string, error) {
	if !HoldsPersistence(table.Subject) {
		return "", fmt.Errorf("%w: partition member parent must name a table", schemaext.ErrInvalidValue)
	}
	desired, _, err := schemaext.FacetAs[*DesiredPartitionMember](table.Desired.Table.Facets, PartitionMemberKind)
	if err != nil {
		return "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedPartitionMember](table.Current.Table.Facets, PartitionMemberKind)
	if err != nil {
		return "", err
	}
	if desired != nil {
		if err := ValidateDesiredPartitionMember(desired); err != nil {
			return "", err
		}
		if err := RefuseForeignPartitionMember(target); err != nil {
			return "", err
		}
	}
	if current != nil {
		if err := ValidateObservedPartitionMember(current); err != nil {
			return "", err
		}
	}
	switch table.Action {
	case featureplan.CreateTable:
		return "CREATE TABLE carries the declared parent and bound", nil
	case featureplan.DropTable:
		return "remove the partition and its rows with the table", nil
	case featureplan.AlterTable:
		return "retain the table's parent and bound unless a planned partition move replaces them", nil
	default:
		if desired != nil || current != nil {
			return "", fmt.Errorf("%w: a PostgreSQL partition has no reconstruction plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
		}
		return "the table has no partition membership to reconstruct", nil
	}
}
