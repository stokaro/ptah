package main

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ProjectTableCreations predicts the key membership of each table a plan would
// create, which is what the fixture reads back. It predicts no column and no
// setting, so the host keeps the definitions it converts.
func (p *owner) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	result := &protocol.ProjectedTableCreations{Complete: true}
	for _, input := range request.GetTables() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		declared := input.GetDeclaration()
		output := &protocol.ProjectedTableCreation{Subject: proto.CloneOf(input.GetSubject()), ColumnPrimaryKeysPrepared: true}
		for _, column := range declared.GetColumns() {
			if column.GetPrimary() || slices.Contains(declared.GetPrimaryKey(), column.GetName()) {
				output.ColumnPrimaryKeys = append(output.ColumnPrimaryKeys, column.GetName())
			}
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}
