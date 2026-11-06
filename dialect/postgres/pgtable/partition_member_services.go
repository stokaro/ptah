package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
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

// CompareFacets refuses a held table whose membership would have to change: a
// partition with another bound or another parent, a partition on one side and an
// ordinary table on the other, and a declared partition whose parent the desired
// schema removes. PostgreSQL has no statement that changes a bound, and detaching
// and attaching a partition is a cutover with its own locks and validation scans,
// so Ptah plans none of them. A bound spelled differently and not normalized by a
// connected server stays undecided, and a source with no syntax for a partition
// asks nothing by leaving it out.
func (PartitionMemberService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return compareTableOwners(ctx, request, PartitionMemberKind, comparePartitionMember)
}

func comparePartitionMember(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (string, error) {
	if !HoldsPersistence(owner.Subject) {
		return "", fmt.Errorf("%w: partition member requires a table subject", schemaext.ErrInvalidValue)
	}
	desired, current, err := partitionMemberOperands(request, owner.Subject)
	if err != nil {
		return "", err
	}
	if !request.Includes(PartitionMemberKind, owner.Subject) {
		return "", nil
	}
	if desired != nil {
		if err := RefuseForeignPartitionMember(request.Target); err != nil {
			return "", err
		}
		if err := refuseRemovedPartitionParent(request, owner.Subject, desired); err != nil {
			return "", err
		}
	}
	if !owner.Current || !owner.Desired {
		return "", nil
	}
	switch {
	case desired != nil && current != nil:
		return comparePartitionMembers(request, owner.Subject, desired, current)
	case desired != nil:
		if !describesPartitionMembers(request.Current.Coverage, owner.Subject) {
			return "the current source does not say whether the table is a partition", nil
		}
		return "", partitionMemberCutoverError(owner.Subject, "is an ordinary table, and the declaration makes it a partition of "+partitionParentName(desired.PartitionMember))
	case current != nil:
		if !describesPartitionMembers(request.Desired.Coverage, owner.Subject) {
			return "", nil
		}
		return "", partitionMemberCutoverError(owner.Subject, "is a partition of "+partitionParentName(PartitionMember(*current))+", and the declaration makes it an ordinary table")
	default:
		return "", nil
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

func comparePartitionMembers(request schemaext.FacetComparisonRequest, subject objectidentity.ID, desired *DesiredPartitionMember, current *ObservedPartitionMember) (string, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	if builder.TableParts(desired.ParentSchema, desired.Parent).Key() != builder.TableParts(current.ParentSchema, current.Parent).Key() {
		return "", partitionMemberCutoverError(subject, "is a partition of "+partitionParentName(PartitionMember(*current))+", and the declaration makes it a partition of "+partitionParentName(desired.PartitionMember))
	}
	held := PartitionBoundKey(current.Bound)
	if desired.Normalized != nil {
		if desired.Normalized.Key == held {
			return "", nil
		}
		return "", partitionBoundChangeError(subject, desired, current)
	}
	if PartitionBoundKey(desired.Bound) == held {
		return "", nil
	}
	return "the partition bound needs normalization by the connected server", nil
}

func partitionMemberCutoverError(subject objectidentity.ID, state string) error {
	return fmt.Errorf("%w: PostgreSQL table %s %s; a reviewed manual partition cutover is required", ptaherr.ErrUnsupportedFeature, subject.Path(), state)
}

func partitionBoundChangeError(subject objectidentity.ID, desired *DesiredPartitionMember, current *ObservedPartitionMember) error {
	return partitionMemberCutoverError(subject, fmt.Sprintf("is a partition with bound %s, and the declaration gives it %s", current.Bound, desired.Bound))
}

func partitionParentName(member PartitionMember) string {
	if member.ParentSchema == "" {
		return member.Parent
	}
	return member.ParentSchema + "." + member.Parent
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

// PlanFeatures accounts for membership on parent table actions. Creation carries
// the parent and the bound on CREATE TABLE ... PARTITION OF, removal drops the
// partition with the rows it holds, and alteration retains both. A rebuild of a
// partition is refused, because it would need a partition-tree strategy. Errors
// and cancellation discard all parent receipts.
func (PartitionMemberService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return planTableParents(ctx, request, PartitionMemberKind, partitionMemberParent)
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
		return "retain the table's parent and bound", nil
	default:
		if desired != nil || current != nil {
			return "", fmt.Errorf("%w: a PostgreSQL partition has no reconstruction plan for parent action %q", ptaherr.ErrUnsupportedFeature, table.Action)
		}
		return "the table has no partition membership to reconstruct", nil
	}
}
