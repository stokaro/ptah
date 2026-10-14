package oratype

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

// ReverseService reconstructs the inverse of user type changes from their
// operands. Its zero value is ready for concurrent use and never reads a
// database.
//
// The inverse of a creation drops the type, and the inverse of a drop creates
// it again as the catalog held it. A surviving type goes back to its
// observation, so a replaced object type is replaced again with the
// attributes it had.
type ReverseService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. ForwardState is a prediction, never inspection evidence.
func (ReverseService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := checkTarget(ctx, request.Target, "reversal"); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	return result, ctx.Err()
}

func reverse(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := ValidateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	label := Label(record.Subject)
	switch change := record.Value.(type) {
	case *DomainChange:
		if err := validChange(record, DomainKind, change.Validate()); err != nil {
			return schemaext.Reversal{}, err
		}
		if change.After == nil {
			return reversal(record, &DomainChange{After: change.Before.Desired()}, nil, strategy(label, true, false)), nil
		}
		forward := change.After.Observed()
		if change.Before == nil {
			return reversal(record, &DomainChange{Before: forward}, forward.Copy(), strategy(label, false, true)), nil
		}
		return reversal(record, &DomainChange{Before: forward, After: change.Before.Desired(), Differs: change.Differs},
			forward.Copy(), strategy(label, true, true)), nil
	case *ObjectTypeChange:
		if err := validChange(record, ObjectTypeKind, change.Validate()); err != nil {
			return schemaext.Reversal{}, err
		}
		if change.After == nil {
			return reversal(record, &ObjectTypeChange{After: change.Before.Desired()}, nil, strategy(label, true, false)), nil
		}
		forward := change.After.Observed()
		if change.Before == nil {
			return reversal(record, &ObjectTypeChange{Before: forward}, forward.Copy(), strategy(label, false, true)), nil
		}
		return reversal(record, &ObjectTypeChange{Before: forward, After: change.Before.Desired(), Differs: change.Differs},
			forward.Copy(), strategy(label, true, true)), nil
	default:
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires an Oracle user type change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
}

// validChange refuses an invalid change and one of another model than its
// subject names.
func validChange(record schemaext.ChangeRecord, model schemaext.Kind, validation error) error {
	if validation != nil {
		return validation
	}
	if schemaext.Kind(record.Subject.Kind) != model {
		return fmt.Errorf("%w: a %s change for %s", schemaext.ErrInvalidValue, model, record.Subject)
	}
	return nil
}

// reversal assembles one reversal: the forward state at the subject, which is
// absent after a drop, and the strategy the reverse change follows.
func reversal(record schemaext.ChangeRecord, reversed schemaext.ChangeValue, forward schemaext.Value, strategy string) schemaext.Reversal {
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: schemaext.Kind(record.Subject.Kind), Value: forward}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy}
}

// strategy says what a reversal does, by which operand the forward change
// lacked.
func strategy(label string, before, after bool) string {
	switch {
	case !before:
		return "drop the " + label + " the forward plan created"
	case !after:
		return "create the " + label + " again as it was"
	default:
		return "change the " + label + " back to what it was"
	}
}

// ConvertService projects user types between their desired and observed
// representations. Its zero value is ready for concurrent use and never reads
// a database. A projection is a prediction, never a claim that a server was
// inspected.
type ConvertService struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. A
// declaration projects to the observation it predicts, such as
// [DesiredDomain.Observed], and an observation to the declaration that keeps
// it as it is, such as [ObservedDomain.Desired]. Any error, including
// cancellation, returns no partial result.
func (ConvertService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := checkTarget(ctx, request.Target, "conversion"); err != nil {
		return nil, err
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid Oracle user type conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validValue(value, request.From); err != nil {
			return nil, err
		}
		var converted schemaext.Value
		switch typed := value.(type) {
		case *DesiredDomain:
			converted = typed.Observed()
		case *DesiredObjectType:
			converted = typed.Observed()
		case *ObservedDomain:
			converted = typed.Desired()
		case *ObservedObjectType:
			converted = typed.Desired()
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

// The metric names, the ones the common schema statistics and the PostgreSQL
// owner report the two kinds under: a metric counts its objects whoever owns
// them.
const (
	domainsMetric     = "domains"
	objectTypesMetric = "composite_types"
)

// ReportService counts user types for inventory reports. Its zero value is
// ready for concurrent use and reads captured values only.
type ReportService struct{}

// Definitions returns independent report definitions of the two models. The
// display names are the family names an omission report prints.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: DomainKind, DisplayName: "domains", Metrics: []schemaext.MetricDefinition{{Name: domainsMetric, Help: "Domain types"}}},
		{Kind: ObjectTypeKind, DisplayName: "object types", Metrics: []schemaext.MetricDefinition{
			{Name: objectTypesMetric, Help: "Composite types"},
		}},
	}
}

// ReportValues counts each type once, under its model's metric, in the
// representation the request names. A value of another representation or
// model, or an invalid one, is refused, and cancellation returns no partial
// report.
func (ReportService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validValue(value, request.Representation); err != nil {
			return nil, err
		}
		metric := objectTypesMetric
		if value.Kind() == DomainKind {
			metric = domainsMetric
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: metric, Value: 1}}})
	}
	return reports, ctx.Err()
}
