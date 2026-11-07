package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func (*columns) CompareColumns(ctx context.Context, request *protocol.CompareColumnsRequest) (*protocol.ComparedColumns, error) {
	result := &protocol.ComparedColumns{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output := &protocol.ComparedColumnTable{Subject: proto.CloneOf(table.Subject), CurrentSubject: proto.CloneOf(table.CurrentSubject)}
		for _, pair := range table.Pairs {
			di := slices.IndexFunc(table.Desired.Columns, func(c *protocol.DeclaredColumn) bool { return c.Name == pair.DesiredColumn })
			ci := slices.IndexFunc(table.Current.Columns, func(c *protocol.ObservedColumn) bool { return c.Name == pair.CurrentColumn })
			desired, current := table.Desired.Columns[di], table.Current.Columns[ci]
			if desired.Comment == "require complete parents" && (len(table.Desired.Columns) != 2 || len(table.Current.Columns) != 3 || table.Desired.Comment != "parent declaration" || table.Current.Comment != "parent observation") {
				return nil, fmt.Errorf("incomplete parent capture")
			}
			column := &protocol.ComparedColumn{Subject: proto.CloneOf(pair.Subject), CurrentSubject: proto.CloneOf(pair.CurrentSubject)}
			// A host that asks the collation gets it apart from the type;
			// one that does not gets it inside the type.
			collated := !slices.Contains(pair.Aspects, protocol.ColumnAspect_COLUMN_COLLATION)
			for _, aspect := range pair.Aspects {
				receipt := &protocol.ColumnAspectReceipt{Aspect: aspect}
				switch aspect {
				case protocol.ColumnAspect_COLUMN_TYPE:
					held := current.ColumnType
					if held == "" {
						held = current.DataType
					}
					heldType, declaredType := canonicalType(held), canonicalType(desired.Type)
					if collated {
						heldType, declaredType = withCollation(heldType, current.Collation), withCollation(declaredType, desired.Collation)
					}
					receipt.Receipt = &protocol.ColumnAspectReceipt_Difference{Difference: difference(heldType, declaredType)}
				case protocol.ColumnAspect_COLUMN_COLLATION:
					receipt.Receipt = &protocol.ColumnAspectReceipt_Difference{Difference: difference(current.Collation, desired.Collation)}
				case protocol.ColumnAspect_COLUMN_DEFAULT:
					held := current.GetDefaultExpression()
					if value, ok := typedLiteral(held); ok {
						held = value
					}
					value := desired.DefaultExpression
					if desired.DefaultSet {
						value = desired.DefaultValue
					}
					receipt.Receipt = &protocol.ColumnAspectReceipt_Difference{Difference: difference(held, value)}
				case protocol.ColumnAspect_COLUMN_NULLABILITY:
					receipt.Receipt = &protocol.ColumnAspectReceipt_Nullability{Nullability: &protocol.ColumnNullability{Nullable: new(effectiveNullable(table.Desired, desired))}}
				case protocol.ColumnAspect_COLUMN_SEQUENCE:
					changes := &protocol.ColumnSequenceDifferences{}
					if current.IdentityStart != desired.IdentityStart {
						changes.Changes = append(changes.Changes, &protocol.ColumnSequenceDifference{Attribute: protocol.ColumnSequenceDifference_IDENTITY_START, Current: current.IdentityStart, Desired: desired.IdentityStart})
					}
					if current.IdentityIncrement != desired.IdentityIncrement {
						changes.Changes = append(changes.Changes, &protocol.ColumnSequenceDifference{Attribute: protocol.ColumnSequenceDifference_IDENTITY_INCREMENT, Current: current.IdentityIncrement, Desired: desired.IdentityIncrement})
					}
					receipt.Receipt = &protocol.ColumnAspectReceipt_Sequence{Sequence: changes}
				}
				column.Aspects = append(column.Aspects, receipt)
			}
			if pair.DesiredColumn == "incomplete" {
				column.Aspects = nil
			}
			output.Columns = append(output.Columns, column)
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}

func difference(current, desired string) *protocol.ColumnDifference {
	relation := protocol.ColumnDifference_SAME
	if current != desired {
		relation = protocol.ColumnDifference_CHANGED
	}
	return &protocol.ColumnDifference{Relation: relation, Current: current, Desired: desired}
}

// withCollation writes a column type with its collation, as an engine that
// compares collation inside the type spells it.
func withCollation(columnType, collation string) string {
	if collation == "" {
		return columnType
	}
	return columnType + " COLLATE " + collation
}

func typedLiteral(value string) (string, bool) {
	if strings.HasPrefix(value, "typed(") && strings.HasSuffix(value, ")") {
		return strings.TrimSuffix(strings.TrimPrefix(value, "typed("), ")"), true
	}
	return "", false
}

func (*columns) ReadColumnDefaults(ctx context.Context, request *protocol.ReadColumnDefaultsRequest) (*protocol.ReadColumnDefaultsResult, error) {
	result := &protocol.ReadColumnDefaultsResult{Complete: true}
	for _, input := range request.Columns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output := &protocol.ColumnDefaultReceipt{Subject: proto.CloneOf(input.Subject), Kind: protocol.ColumnDefaultReceipt_ABSENT}
		if input.Column.DefaultExpression != nil {
			output.Kind, output.Value = protocol.ColumnDefaultReceipt_EXPRESSION, *input.Column.DefaultExpression
			if value, ok := typedLiteral(output.Value); ok {
				output.Kind, output.Value = protocol.ColumnDefaultReceipt_LITERAL, value
			}
		}
		if input.Column.Name == "missing_default" {
			continue
		}
		result.Columns = append(result.Columns, output)
	}
	return result, nil
}
