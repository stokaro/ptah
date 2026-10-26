// Package columnsemantics binds offline column receipts to shared conversion
// paths without changing captured declarations or observations.
package columnsemantics

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
)

// Column names an exact captured column without delimiter parsing.
type Column struct{ Schema, Table, Name string }

// Defaults interprets the entire observed inventory, including columns absent
// from the desired schema. A selected service must complete every receipt.
func Defaults(ctx context.Context, service schemacomparison.ColumnService, target string, identifiers identifier.Semantics,
	capabilities capability.Capabilities, tables []catalog.Table,
) (map[Column]*schemacomparison.ColumnDefaultReceipt, error) {
	if service == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := schemacomparison.ColumnDefaultsRequest{Target: target, Identifiers: identifiers, Capabilities: capabilities}
	builder := objectidentity.NewBuilder(identifiers)
	seen := make(map[objectidentity.Key]bool)
	var slots []Column
	for _, table := range tables {
		for _, column := range table.Columns {
			subject := builder.ColumnParts(table.Schema, table.Name, column.Name)
			if seen[subject.Key()] {
				return nil, fmt.Errorf("%w: duplicate observed column", schemacomparison.ErrColumns)
			}
			seen[subject.Key()] = true
			request.Columns = append(request.Columns, schemacomparison.ObservedColumnDefault{Subject: subject, Column: column.Clone()})
			slots = append(slots, Column{table.Schema, table.Name, column.Name})
		}
	}
	result, err := service.ReadColumnDefaults(ctx, request.Clone())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	accepted, err := schemacomparison.AcceptColumnDefaults(request, result)
	if err != nil {
		return nil, err
	}
	defaults := make(map[Column]*schemacomparison.ColumnDefaultReceipt, len(slots))
	for i, slot := range slots {
		defaults[slot] = new(accepted.Columns[i])
	}
	return defaults, nil
}
