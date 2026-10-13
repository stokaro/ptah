package pgtable

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// PartitionService converts and compares partition facets using captured data.
// Its zero value is ready for concurrent use and performs no database work.
type PartitionService struct{}

// ConvertFeatures preserves partition keys across schema representations.
// Cancellation, invalid values, or invalid directions return no partial batch.
func (PartitionService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertTableFeatures(ctx, request, "partition", convertPartition)
}

func convertPartition(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch p := value.(type) {
	case *DesiredPartition:
		if from == schemaext.Desired {
			if err := ValidateDesiredPartition(p); err != nil {
				return nil, err
			}
			return &ObservedPartition{Type: p.Type, Parts: slices.Clone(p.Parts)}, nil
		}
	case *ObservedPartition:
		if from == schemaext.Observed {
			if err := ValidateObservedPartition(p); err != nil {
				return nil, err
			}
			return &DesiredPartition{Type: p.Type, Parts: slices.Clone(p.Parts)}, nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s partition value %T", schemaext.ErrInvalidValue, from, value)
}

// CompareFacets retains declarations and refuses changes to the key of a
// surviving table. Missing source knowledge and unresolved expression spelling
// stay undecided. Foreign targets follow the owner's omission or refusal policy.
func (PartitionService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := persistenceContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{PartitionKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid partition comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		reason, err := comparePartition(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: PartitionKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completedPersistence(ctx, result)
}

func comparePartition(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (string, error) {
	if !HoldsPersistence(owner.Subject) {
		return "", fmt.Errorf("%w: partition requires a table subject", schemaext.ErrInvalidValue)
	}
	desired, current, err := partitionOperands(request, owner.Subject)
	if err != nil {
		return "", err
	}
	if !request.Includes(PartitionKind, owner.Subject) {
		return "", nil
	}
	if !SupportsPartition(request.Target) {
		if desired != nil {
			return "", RefuseForeignPartition(request.Target)
		}
		return "", nil
	}
	if !owner.Current || !owner.Desired {
		return "", nil
	}
	for _, coverage := range []schemaext.Coverage{request.Desired.Coverage, request.Current.Coverage} {
		knowledge := coverage.Lookup(PartitionKind, owner.Subject)
		if !coverage.SelectKinds([]schemaext.Kind{PartitionKind}).IsZero() && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
			return "the source does not fully describe the table partition key", nil
		}
	}
	if desired == nil && !describesPartition(request.Desired.Coverage, owner.Subject) {
		return "", nil
	}
	if current == nil && !describesPartition(request.Current.Coverage, owner.Subject) {
		return "the current source does not describe the table partition key", nil
	}
	return comparePartitionKeys(owner.Subject, desired, current)
}

func comparePartitionKeys(subject objectidentity.ID, desired *DesiredPartition, current *ObservedPartition) (string, error) {
	var declared *Partition
	if desired != nil {
		declared = &Partition{Type: desired.Type, Parts: desired.Parts}
	}
	captured := (*Partition)(current)
	if desired != nil && desired.Normalized != nil {
		if desired.Normalized.Key == PartitionComparisonKey(captured) {
			return "", nil
		}
		return "", partitionChangeError(subject)
	}
	if PartitionComparisonKey(declared) == PartitionComparisonKey(captured) {
		return "", nil
	}
	if declared != nil && captured != nil && declared.Type == captured.Type && len(declared.Parts) == len(captured.Parts) {
		for i, part := range declared.Parts {
			if part.Name != "" && captured.Parts[i].Name != "" && part.Name != captured.Parts[i].Name {
				return "", partitionChangeError(subject)
			}
		}
		return "the partition key needs normalization by the connected server", nil
	}
	return "", partitionChangeError(subject)
}

func describesPartition(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	return slices.Contains([]schemaext.KnowledgeState{schemaext.Complete, schemaext.Absent, schemaext.Defaulted}, coverage.Lookup(PartitionKind, subject).State)
}
func partitionChangeError(subject objectidentity.ID) error {
	return fmt.Errorf("%w: PostgreSQL table %s partition key cannot change in place", ptaherr.ErrUnsupportedFeature, subject)
}

// RefuseForeignPartition checks a target's policy for a present declaration.
// Supported targets and targets that report an omission return nil. Targets
// with a different partition grammar return ErrUnsupportedFeature by name.
func RefuseForeignPartition(target string) error {
	switch platform.NormalizeDialect(target) {
	case platform.Postgres, platform.YugabyteDB, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer, platform.Oracle, platform.ClickHouse:
		return nil
	case platform.YDB, platform.CockroachDB, platform.Spanner:
		return fmt.Errorf("%w: PostgreSQL PARTITION BY cannot be rendered on %s", ptaherr.ErrUnsupportedFeature, target)
	default:
		return fmt.Errorf("%w: table partition has no policy for %q", ptaherr.ErrUnsupportedDialect, target)
	}
}

func partitionOperands(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*DesiredPartition, *ObservedPartition, error) {
	desired, _, err := schemaext.FacetAs[*DesiredPartition](tableFacets(request.Desired, subject), PartitionKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*ObservedPartition](tableFacets(request.Current, subject), PartitionKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := ValidateDesiredPartition(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := ValidateObservedPartition(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}
