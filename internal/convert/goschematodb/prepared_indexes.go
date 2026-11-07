package goschematodb

import (
	"context"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

// applyPreparedIndexes describes each index the converted databases hold as
// the target's table preparation states it: its kind as Method, the NULLS
// order of each key part, and its predicate. The comparison reads a declared
// index through the same preparation and compares what it states exactly with
// the current side, so a document converted to stand for the current side has
// to carry the target's spelling rather than the one its source wrote. Without
// this a YDB document compared with itself planned a DROP INDEX and an ADD
// INDEX for every global index: the declared side held the prepared kind
// GLOBAL SYNC and the converted side the kind the source wrote.
//
// dbs are the lowered databases outs were described from, in order. Only the
// tables of a database that declares an index are prepared, in one batch; a
// batch without tables asks the target nothing. What a target leaves unstated
// keeps the description the conversion gave it.
func applyPreparedIndexes(ctx context.Context, outs []*catalog.Database, dbs []*schemamodel.Database, target string, runtime Runtime) error {
	semantics := identifier.ForDialect(target)
	identities := objectidentity.NewBuilder(semantics)
	request := schemapreparation.Request{Target: target, Identifiers: semantics, Capabilities: capability.ForDialect(target)}
	for _, db := range dbs {
		if len(db.Indexes) == 0 {
			continue
		}
		for _, table := range db.Tables {
			// The document stands for the table its CREATE would make, so the
			// target prepares a creation: nothing holds the table yet.
			request.Tables = append(request.Tables, schemapreparation.Table{
				Subject: identities.TableParts(table.Schema, table.Name), Desired: schemacapture.DeclareTable(db, table, semantics),
				CurrentKnowledge: schemaext.Knowledge{State: schemaext.Absent},
			})
		}
	}
	if len(request.Tables) == 0 {
		return nil
	}
	source := request.Clone()
	result, err := runtime.PrepareTables(ctx, request)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	capture, err := schemapreparation.Accept(source, result)
	if err != nil {
		return err
	}
	prepared := make(map[objectidentity.Key]schemapreparation.Table, len(capture.Prepared))
	for _, table := range capture.Prepared {
		prepared[table.Subject.Key()] = table
	}
	for _, out := range outs {
		for i := range out.Indexes {
			index := &out.Indexes[i]
			table, found := prepared[identities.TableParts(index.Schema, index.TableName).Key()]
			if !found {
				continue
			}
			if method, stated := table.IndexMethods[index.Name]; stated {
				index.Method = method
			}
			if orders, stated := table.IndexNulls[index.Name]; stated {
				index.Parts = withNullsOrders(index.Parts, index.Columns, orders)
			}
			if condition, stated := table.IndexConditions[index.Name]; stated {
				index.Condition = condition
			}
		}
	}
	return nil
}

// withNullsOrders returns the key parts of an index with the NULLS order
// preparation states for each, in key order. An index described by its
// columns alone gets a part for each column only where some order is not the
// default, which is how a read reports a held index. Orders that do not number
// the parts leave them as they are.
func withNullsOrders(parts []catalog.IndexPart, columns, orders []string) []catalog.IndexPart {
	if len(parts) == 0 {
		if !slices.ContainsFunc(orders, func(order string) bool { return order != "" }) {
			return parts
		}
		parts = make([]catalog.IndexPart, len(columns))
		for i, column := range columns {
			parts[i] = catalog.IndexPart{Name: column}
		}
	}
	if len(parts) != len(orders) {
		return parts
	}
	parts = slices.Clone(parts)
	for i := range parts {
		parts[i].NullsOrder = orders[i]
	}
	return parts
}
