// Package pgproject predicts PostgreSQL-owned schema effects for reverse plans.
package pgproject

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaprojection"
)

// Constraints projects table-local PostgreSQL constraint effects. Partition
// descendants and server-generated names require captures beyond this contract.
type Constraints struct{}

// ProjectConstraints accounts for key indexes, column key flags, and foreign
// key defaults. It reports unavailable effects without publishing partial state.
func (Constraints) ProjectConstraints(ctx context.Context, request schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
	if ctx == nil {
		return schemaprojection.ConstraintResult{}, fmt.Errorf("%w: context is required", schemaprojection.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	if request.Target != "postgres" {
		return schemaprojection.ConstraintResult{}, fmt.Errorf("%w: PostgreSQL constraint projection received target %q", schemaprojection.ErrInvalid, request.Target)
	}
	if request.Before.Table.Partitioned || request.After.Table.Partitioned {
		return unavailable("partition constraint effects require descendant captures"), nil
	}
	state := request.After.Clone()
	for _, change := range request.Changes {
		if reason := removeBacking(&state, request.Before, change.Before, request.Identifiers); reason != "" {
			return unavailable(reason), nil
		}
	}
	for _, change := range request.Changes {
		if change.After == nil {
			continue
		}
		if reason := addBacking(&state, *change.After, request.Identifiers); reason != "" {
			return unavailable(reason), nil
		}
		normalizeConstraint(&state, change.After.Name, request.Identifiers)
	}
	if reason := projectKeyColumns(&state, request); reason != "" {
		return unavailable(reason), nil
	}
	return schemaprojection.ConstraintResult{State: &state}, nil
}

func unavailable(reason string) schemaprojection.ConstraintResult {
	return schemaprojection.ConstraintResult{Unavailable: reason}
}

func removeBacking(state *schemaprojection.TableState, before schemaprojection.TableState, constraint *catalog.Constraint, semantics identifier.Semantics) string {
	if constraint == nil {
		return ""
	}
	if !constraint.Facets.IsZero() {
		return "constraint facets require their owner's lifecycle projection"
	}
	switch constraint.Type {
	case "CHECK", "FOREIGN KEY":
		return ""
	case "PRIMARY KEY", "UNIQUE", "EXCLUDE":
		key := semantics.IndexIdentityKey(constraint.Name)
		position := slices.IndexFunc(before.Indexes, func(index catalog.Index) bool { return semantics.IndexIdentityKey(index.Name) == key })
		if position < 0 || before.Indexes[position].PartitionAttached {
			return "constraint backing index is missing or partition-attached"
		}
		index := before.Indexes[position]
		if (constraint.Type == "PRIMARY KEY" && !index.IsPrimary) || (constraint.Type == "UNIQUE" && !index.IsUnique) {
			return "captured index does not establish constraint ownership"
		}
		state.Indexes = slices.DeleteFunc(state.Indexes, func(index catalog.Index) bool { return semantics.IndexIdentityKey(index.Name) == key })
		return ""
	default:
		return "unrecognized PostgreSQL constraint kind"
	}
}

func addBacking(state *schemaprojection.TableState, constraint catalog.Constraint, semantics identifier.Semantics) string {
	if !constraint.Facets.IsZero() {
		return "constraint facets require their owner's lifecycle projection"
	}
	switch constraint.Type {
	case "CHECK", "FOREIGN KEY":
		return ""
	case "EXCLUDE":
		return "exclusion index expressions require a resolved index projection"
	case "PRIMARY KEY", "UNIQUE":
	default:
		return "unrecognized PostgreSQL constraint kind"
	}
	if len(constraint.ColumnNamesOrDefault()) == 0 {
		return "constraint key columns are missing"
	}
	for _, name := range append(slices.Clone(constraint.ColumnNamesOrDefault()), constraint.IncludeColumns...) {
		if !slices.ContainsFunc(state.Table.Columns, func(column catalog.Column) bool {
			return semantics.ColumnIdentityKey(column.Name) == semantics.ColumnIdentityKey(name)
		}) {
			return "constraint index column is absent from the accepted table"
		}
	}
	if constraint.UsingMethod != nil && !strings.EqualFold(*constraint.UsingMethod, "btree") {
		return "PostgreSQL key constraints require a btree index"
	}
	key := semantics.IndexConflictKey(constraint.Name)
	for _, index := range state.Indexes {
		if semantics.IndexConflictUnresolved(index.Name) || semantics.IndexConflictKey(index.Name) == key {
			return "constraint backing index conflicts with an accepted index"
		}
	}
	index := catalog.Index{
		Name: constraint.Name, TableName: state.Table.Name, Schema: state.Table.Schema,
		Columns: slices.Clone(constraint.ColumnNamesOrDefault()), Method: "btree", IsUnique: true,
		IsPrimary: constraint.Type == "PRIMARY KEY", IncludeColumns: slices.Clone(constraint.IncludeColumns),
		RequiresExtensions: slices.Clone(constraint.RequiresExtensions),
	}
	if constraint.NullsDistinct != nil {
		index.NullsDistinct = new(*constraint.NullsDistinct)
	}
	for _, column := range index.Columns {
		index.Parts = append(index.Parts, catalog.IndexPart{Name: column})
	}
	state.Indexes = append(state.Indexes, index)
	return ""
}

func normalizeConstraint(state *schemaprojection.TableState, name string, semantics identifier.Semantics) {
	for i := range state.Constraints {
		constraint := &state.Constraints[i]
		if semantics.IndexIdentityKey(constraint.Name) != semantics.IndexIdentityKey(name) {
			continue
		}
		if constraint.Deferrable && constraint.Initially == "" {
			constraint.Initially = "immediate"
		}
		if constraint.Type == "FOREIGN KEY" {
			if constraint.ForeignSchema == "" {
				constraint.ForeignSchema = semantics.DefaultSchema
			}
			if constraint.ForeignColumn == nil && len(constraint.ForeignColumns) > 0 {
				constraint.ForeignColumn = new(constraint.ForeignColumns[0])
			}
			if constraint.DeleteRule == nil {
				constraint.DeleteRule = new("NO ACTION")
			}
			if constraint.UpdateRule == nil {
				constraint.UpdateRule = new("NO ACTION")
			}
		}
	}
}

func projectKeyColumns(state *schemaprojection.TableState, request schemaprojection.ConstraintRequest) string {
	primary := make(map[string]bool)
	unique := make(map[string]bool)
	for _, constraint := range state.Constraints {
		for _, name := range constraint.ColumnNamesOrDefault() {
			key := request.Identifiers.ColumnIdentityKey(name)
			switch constraint.Type {
			case "PRIMARY KEY":
				primary[key] = true
			case "UNIQUE":
				unique[key] = len(constraint.ColumnNamesOrDefault()) == 1 || unique[key]
			}
		}
	}
	for i := range state.Table.Columns {
		column := &state.Table.Columns[i]
		key := request.Identifiers.ColumnIdentityKey(column.Name)
		column.IsPrimaryKey, column.IsUnique = primary[key], unique[key]
		if !primary[key] {
			continue
		}
		if request.Capabilities.Has(capability.NamedNotNullConstraints) && column.NotNullConstraintName == "" {
			for _, original := range request.Before.Table.Columns {
				if request.Identifiers.ColumnIdentityKey(original.Name) == key {
					column.NotNullConstraintName = original.NotNullConstraintName
				}
			}
			if column.NotNullConstraintName == "" {
				return "primary key adds a server-named NOT NULL constraint"
			}
		}
		column.IsNullable = "NO"
	}
	return ""
}
