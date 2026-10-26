package main

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func (*columns) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	result := &protocol.ProjectedTableCreations{Complete: true}
	for _, input := range request.Tables {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		table := input.Declaration
		output := &protocol.ProjectedTableCreation{Subject: proto.CloneOf(input.Subject), ColumnPrimaryKeysPrepared: true, ColumnPrimaryKeys: keys(table)}
		for i, column := range table.Columns {
			nullable := "NO"
			if effectiveNullable(table, column) {
				nullable = "YES"
			}
			predicted := &protocol.ObservedColumn{Name: column.Name, DataType: canonicalType(column.Type), ColumnType: canonicalType(column.Type), IsNullable: nullable, PrimaryKey: slices.Contains(output.ColumnPrimaryKeys, column.Name), OrdinalPosition: int64(i + 1), Comment: column.Comment,
				AutoIncrement: column.AutoIncrement, IdentityStart: column.IdentityStart, IdentityIncrement: column.IdentityIncrement, IdentityGeneration: column.IdentityGeneration}
			if column.DefaultSet {
				predicted.DefaultExpression = new("typed(" + column.DefaultValue + ")")
			} else if column.DefaultExpression != "" {
				predicted.DefaultExpression = new(column.DefaultExpression)
			}
			output.Columns = append(output.Columns, predicted)
		}
		if table.Name == "incomplete" {
			output.Columns = nil
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}
