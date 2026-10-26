package provider

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateCreationColumns(engine *protocol.Engine, input *protocol.DeclaredTable, output *protocol.ProjectedTableCreation) error {
	if !slices.Contains(engine.GetFeatures(), ColumnsFeature) {
		if len(output.GetColumns()) != 0 {
			return columnsError("creation columns were not negotiated")
		}
		return nil
	}
	if len(output.GetColumns()) != len(input.GetColumns()) {
		return columnsError("creation prediction requires every declared column")
	}
	for i, column := range output.GetColumns() {
		if column.GetName() != input.Columns[i].GetName() {
			return columnsError("creation prediction changes column identity or order")
		}
		if column.GetIsNullable() != "YES" && column.GetIsNullable() != "NO" {
			return columnsError("creation prediction requires explicit nullability")
		}
		if !hasObservedColumnType(column) {
			return columnsError("creation prediction requires an effective type")
		}
	}
	return nil
}
