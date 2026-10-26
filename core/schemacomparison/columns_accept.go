package schemacomparison

import (
	"fmt"
	"slices"
	"strings"
)

// AcceptColumns requires exact ordered accounting and returns independent
// receipts. Invalid results wrap ErrColumns and expose no successful prefix.
func AcceptColumns(request ColumnsRequest, result ColumnsResult) (ColumnsResult, error) {
	if !result.Complete || len(result.Tables) != len(request.Tables) {
		return ColumnsResult{}, fmt.Errorf("%w: incomplete comparison", ErrColumns)
	}
	for i, input := range request.Tables {
		output := result.Tables[i]
		if output.Desired != input.DesiredSubject || output.Current != input.CurrentSubject || len(output.Columns) != len(input.Pairs) {
			return ColumnsResult{}, fmt.Errorf("%w: changed table identities or pair inventory", ErrColumns)
		}
		for j, pair := range input.Pairs {
			column := output.Columns[j]
			if column.Desired != pair.Desired || column.Current != pair.Current || len(column.Aspects) != len(pair.Aspects) {
				return ColumnsResult{}, fmt.Errorf("%w: changed column identities or aspect inventory", ErrColumns)
			}
			for k, aspect := range pair.Aspects {
				if slices.Contains(pair.Aspects[:k], aspect) || column.Aspects[k].Aspect != aspect {
					return ColumnsResult{}, fmt.Errorf("%w: duplicate or reordered aspect", ErrColumns)
				}
				if err := acceptColumnAspect(column.Aspects[k]); err != nil {
					return ColumnsResult{}, err
				}
			}
		}
	}
	return result.Clone(), nil
}

func acceptColumnAspect(receipt ColumnAspectReceipt) error {
	switch receipt.Aspect {
	case ColumnType, ColumnDefault:
		if receipt.Difference == nil || receipt.Nullable != nil || receipt.Sequence != nil ||
			(receipt.Difference.Relation != ColumnSame && receipt.Difference.Relation != ColumnChanged) {
			return fmt.Errorf("%w: a difference needs one explicit relation", ErrColumns)
		}
		if receipt.Aspect == ColumnType && receipt.Difference.Relation == ColumnChanged &&
			(strings.TrimSpace(receipt.Difference.Current) == "" || strings.TrimSpace(receipt.Difference.Desired) == "") {
			return fmt.Errorf("%w: changed types need diagnostic spellings", ErrColumns)
		}
	case ColumnNullability:
		if receipt.Nullable == nil || receipt.Difference != nil || receipt.Sequence != nil {
			return fmt.Errorf("%w: nullability needs one explicit boolean", ErrColumns)
		}
	case ColumnSequence:
		if receipt.Sequence == nil || receipt.Nullable != nil || receipt.Difference != nil {
			return fmt.Errorf("%w: sequence comparison needs one receipt", ErrColumns)
		}
		return acceptSequenceChanges(receipt.Sequence.Changes)
	default:
		return fmt.Errorf("%w: unknown column aspect", ErrColumns)
	}
	return nil
}

// Clone returns independent aspect values and receipt inventories.
func (r ColumnsResult) Clone() ColumnsResult {
	r.Tables = slices.Clone(r.Tables)
	for i := range r.Tables {
		r.Tables[i].Columns = slices.Clone(r.Tables[i].Columns)
		for j := range r.Tables[i].Columns {
			column := &r.Tables[i].Columns[j]
			column.Aspects = slices.Clone(column.Aspects)
			for k := range column.Aspects {
				receipt := &column.Aspects[k]
				if receipt.Difference != nil {
					receipt.Difference = new(*receipt.Difference)
				}
				if receipt.Nullable != nil {
					receipt.Nullable = new(*receipt.Nullable)
				}
				if receipt.Sequence != nil {
					receipt.Sequence = &ColumnSequenceChanges{Changes: slices.Clone(receipt.Sequence.Changes)}
				}
			}
		}
	}
	return r
}

// AcceptColumnDefaults retains exact identities and distinguishes an absent
// default from an empty literal. A missing receipt never selects a fallback.
func AcceptColumnDefaults(request ColumnDefaultsRequest, result ColumnDefaultsResult) (ColumnDefaultsResult, error) {
	if !result.Complete || len(result.Columns) != len(request.Columns) {
		return ColumnDefaultsResult{}, fmt.Errorf("%w: incomplete default readback", ErrColumns)
	}
	for i, input := range request.Columns {
		receipt := result.Columns[i]
		if receipt.Subject != input.Subject {
			return ColumnDefaultsResult{}, fmt.Errorf("%w: default readback changed column identity", ErrColumns)
		}
		if err := acceptColumnDefault(input, receipt); err != nil {
			return ColumnDefaultsResult{}, err
		}
	}
	result.Columns = slices.Clone(result.Columns)
	return result, nil
}

func acceptColumnDefault(input ObservedColumnDefault, receipt ColumnDefaultReceipt) error {
	switch receipt.Kind {
	case DefaultAbsent:
		if input.Column.ColumnDefault != nil || receipt.Value != "" {
			return fmt.Errorf("%w: an absent default needs absent input and empty output", ErrColumns)
		}
	case DefaultLiteral:
		if input.Column.ColumnDefault == nil {
			return fmt.Errorf("%w: a literal cannot invent an absent default", ErrColumns)
		}
	case DefaultExpression:
		if input.Column.ColumnDefault == nil || strings.TrimSpace(*input.Column.ColumnDefault) == "" || strings.TrimSpace(receipt.Value) == "" {
			return fmt.Errorf("%w: an expression needs nonempty input and output", ErrColumns)
		}
	default:
		return fmt.Errorf("%w: unknown default kind", ErrColumns)
	}
	return nil
}

func acceptSequenceChanges(changes []ColumnSequenceDifference) error {
	seen := make(map[SequenceAttribute]bool)
	for _, change := range changes {
		if (change.Attribute != SequenceStart && change.Attribute != SequenceIncrement) || seen[change.Attribute] || change.Current == change.Desired {
			return fmt.Errorf("%w: invalid sequence delta", ErrColumns)
		}
		seen[change.Attribute] = true
	}
	return nil
}
