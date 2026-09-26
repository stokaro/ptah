package planner

import "ptah.run/core/ast"

// omitNullBackfill asks every column modification in the plan to set NOT NULL
// without filling the column's NULL rows first. It marks the operations in
// place and leaves every other node as it is.
func omitNullBackfill(nodes []ast.Node) {
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range alter.Operations {
			if modify, ok := operation.(*ast.ModifyColumnOperation); ok {
				modify.OmitNullBackfill = true
			}
		}
	}
}
