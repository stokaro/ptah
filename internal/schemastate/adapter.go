package schemastate

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/tableref"
)

// sliceScope is the family set every adapter in this package describes. It is
// one value so a catalog read and an authoring read cannot disagree about what
// the slice covers.
var sliceScope = []objectidentity.Kind{
	objectidentity.KindTable,
	objectidentity.KindColumn,
	objectidentity.KindConstraint,
}

// foreignKeyConstraintType is how both sources spell the family in their
// untyped Type field.
const foreignKeyConstraintType = "FOREIGN KEY"

// FromCatalog builds canonical state from a live catalog read.
//
// It is a peer of [FromDescription] rather than a conversion into its shape.
// The tree currently converts in both directions -- internal/convert/toschema
// one way and internal/convert/fromschema the other -- and ADR 0001 decision 6
// deletes the direction question by having both readers produce this state
// directly.
func FromCatalog(schema *dbschematypes.DBSchema, dialect string, semantics identifier.Semantics) (*State, error) {
	state := New(dialect, sliceScope...)
	builder := objectidentity.NewBuilder(semantics)

	for _, table := range schema.Tables {
		id := builder.TableParts(table.Schema, table.Name)
		columns := columnsFromCatalog(table, builder)
		if existing, collided := state.Add(Object{
			ID:         id,
			Table:      &Table{Columns: columns},
			Provenance: Provenance{Source: "catalog", Location: "information_schema.tables"},
		}); collided {
			return nil, fmt.Errorf("catalog reports two tables with one identity: %s and %s", existing.ID, id)
		}
	}

	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, foreignKeyConstraintType) {
			continue
		}
		key, err := foreignKeyFromCatalog(constraint, builder)
		if err != nil {
			return nil, err
		}
		id := builder.ConstraintParts(constraint.Schema, constraint.TableName, constraint.Name)
		if existing, collided := state.Add(Object{
			ID:         id,
			ForeignKey: key,
			Provenance: Provenance{Source: "catalog", Location: "information_schema.table_constraints"},
		}); collided {
			return nil, fmt.Errorf("catalog reports two foreign keys with one identity: %s and %s", existing.ID, id)
		}
	}
	return state, nil
}

// columnsFromCatalog collects a table's columns, keyed through the identity
// model so the two sources resolve one column to one identity.
func columnsFromCatalog(
	table dbschematypes.DBTable,
	builder objectidentity.Builder,
) []Column {
	columns := make([]Column, 0, len(table.Columns))
	for _, column := range table.Columns {
		columns = append(columns, Column{
			ID:       builder.ColumnParts(table.Schema, table.Name, column.Name),
			Type:     column.DataType,
			Nullable: strings.EqualFold(column.IsNullable, "YES"),
			Unique:   column.IsPrimaryKey || column.IsUnique,
		})
	}
	return columns
}

// foreignKeyFromCatalog reads the constraint's referential facts.
//
// A referential action the model does not understand is refused here rather
// than carried as a string. The alternative is a plan that renders a clause
// whose behavior on delete nobody in this codebase can state.
func foreignKeyFromCatalog(
	constraint dbschematypes.DBConstraint,
	builder objectidentity.Builder,
) (*ForeignKey, error) {
	if constraint.ForeignTable == nil {
		return nil, fmt.Errorf(
			"catalog reports foreign key %q on %q with no referenced table, so nothing names what it references",
			constraint.Name, constraint.TableName)
	}
	onDelete, err := ParseReferentialAction(derefOrEmpty(constraint.DeleteRule))
	if err != nil {
		return nil, fmt.Errorf("foreign key %q on %q: ON DELETE: %w", constraint.Name, constraint.TableName, err)
	}
	onUpdate, err := ParseReferentialAction(derefOrEmpty(constraint.UpdateRule))
	if err != nil {
		return nil, fmt.Errorf("foreign key %q on %q: ON UPDATE: %w", constraint.Name, constraint.TableName, err)
	}
	referencedSchema := constraint.ForeignSchema
	if referencedSchema == "" {
		referencedSchema = constraint.Schema
	}
	return &ForeignKey{
		Columns:           constraint.ColumnNamesOrDefault(),
		ReferencedTable:   builder.TableParts(referencedSchema, *constraint.ForeignTable),
		ReferencedColumns: constraint.ForeignColumnsOrDefault(),
		OnDelete:          Action{Source: derefOrEmpty(constraint.DeleteRule), Normalized: onDelete},
		OnUpdate:          Action{Source: derefOrEmpty(constraint.UpdateRule), Normalized: onUpdate},
	}, nil
}

// FromDescription builds canonical state from an authoring source.
//
// Foreign keys arrive two ways in that model -- a field-level `Foreign` tag and
// a table-level Constraint of type FOREIGN KEY -- and both produce the same
// object here. Keeping them as two shapes past the adapter is what makes a
// later stage answer one question twice.
func FromDescription(
	description *goschema.Database,
	dialect string,
	semantics identifier.Semantics,
) (*State, error) {
	state := New(dialect, sliceScope...)
	builder := objectidentity.NewBuilder(semantics)
	tablesByStruct := map[string]goschema.Table{}

	for _, table := range description.Tables {
		tablesByStruct[table.StructName] = table
		id := builder.TableParts(table.Schema, table.Name)
		columns := columnsFromDescription(description, table, builder)
		if existing, collided := state.Add(Object{
			ID:         id,
			Table:      &Table{Columns: columns},
			Provenance: Provenance{Source: "description", Location: table.StructName},
		}); collided {
			return nil, fmt.Errorf("description declares two tables with one identity: %s and %s", existing.ID, id)
		}
	}

	for _, field := range description.Fields {
		if strings.TrimSpace(field.Foreign) == "" {
			continue
		}
		owner, ok := tablesByStruct[field.StructName]
		if !ok {
			return nil, fmt.Errorf(
				"field %q declares a foreign key but no table declares struct %q, so nothing owns the constraint",
				field.Name, field.StructName)
		}
		key, err := foreignKeyFromField(field, owner, builder)
		if err != nil {
			return nil, err
		}
		if err := addForeignKey(state, builder, owner, foreignKeyName(field, owner), key, field.StructName); err != nil {
			return nil, err
		}
	}

	for _, constraint := range description.Constraints {
		if !strings.EqualFold(constraint.Type, foreignKeyConstraintType) {
			continue
		}
		owner, ok := tablesByStruct[constraint.StructName]
		if !ok {
			return nil, fmt.Errorf(
				"constraint %q declares a foreign key but no table declares struct %q",
				constraint.Name, constraint.StructName)
		}
		key, err := foreignKeyFromConstraint(constraint, builder, owner)
		if err != nil {
			return nil, err
		}
		if err := addForeignKey(state, builder, owner, constraint.Name, key, constraint.StructName); err != nil {
			return nil, err
		}
	}
	return state, nil
}

// addForeignKey records a constraint under the identity its owning table gives
// it, refusing a second one with the same identity.
func addForeignKey(
	state *State,
	builder objectidentity.Builder,
	owner goschema.Table,
	name string,
	key *ForeignKey,
	location string,
) error {
	id := builder.ConstraintParts(owner.Schema, owner.Name, name)
	if existing, collided := state.Add(Object{
		ID:         id,
		ForeignKey: key,
		Provenance: Provenance{Source: "description", Location: location},
	}); collided {
		return fmt.Errorf("description declares two foreign keys with one identity: %s and %s", existing.ID, id)
	}
	return nil
}

// columnsFromDescription collects the columns a description declares for a
// table, matched by the struct that owns them.
func columnsFromDescription(
	description *goschema.Database,
	table goschema.Table,
	builder objectidentity.Builder,
) []Column {
	columns := make([]Column, 0)
	for _, field := range description.Fields {
		if field.StructName != table.StructName {
			continue
		}
		columns = append(columns, Column{
			ID:       builder.ColumnParts(table.Schema, table.Name, field.Name),
			Type:     field.Type,
			Nullable: field.Nullable,
			Unique:   field.Primary || field.Unique,
		})
	}
	return columns
}

// foreignKeyName resolves the constraint name a field-level foreign key
// carries, or the name the renderer would generate for it.
func foreignKeyName(field goschema.Field, owner goschema.Table) string {
	if name := strings.TrimSpace(field.ForeignKeyName); name != "" {
		return name
	}
	return fmt.Sprintf("fk_%s_%s", owner.Name, field.Name)
}

// foreignKeyFromField reads the `table(column)` spelling a field-level foreign
// key carries.
func foreignKeyFromField(
	field goschema.Field,
	owner goschema.Table,
	builder objectidentity.Builder,
) (*ForeignKey, error) {
	table, column, err := splitFieldReference(field.Foreign)
	if err != nil {
		return nil, fmt.Errorf("field %q on %q: %w", field.Name, owner.Name, err)
	}
	onDelete, err := ParseReferentialAction(field.OnDelete)
	if err != nil {
		return nil, fmt.Errorf("field %q on %q: ON DELETE: %w", field.Name, owner.Name, err)
	}
	onUpdate, err := ParseReferentialAction(field.OnUpdate)
	if err != nil {
		return nil, fmt.Errorf("field %q on %q: ON UPDATE: %w", field.Name, owner.Name, err)
	}
	return &ForeignKey{
		Columns:           []string{field.Name},
		ReferencedTable:   referencedTable(table, owner, builder),
		ReferencedColumns: []string{column},
		OnDelete:          Action{Source: field.OnDelete, Normalized: onDelete},
		OnUpdate:          Action{Source: field.OnUpdate, Normalized: onUpdate},
	}, nil
}

// foreignKeyFromConstraint reads a table-level foreign key.
func foreignKeyFromConstraint(
	constraint goschema.Constraint,
	builder objectidentity.Builder,
	owner goschema.Table,
) (*ForeignKey, error) {
	onDelete, err := ParseReferentialAction(constraint.OnDelete)
	if err != nil {
		return nil, fmt.Errorf("constraint %q: ON DELETE: %w", constraint.Name, err)
	}
	onUpdate, err := ParseReferentialAction(constraint.OnUpdate)
	if err != nil {
		return nil, fmt.Errorf("constraint %q: ON UPDATE: %w", constraint.Name, err)
	}
	referenced := constraint.ForeignColumns
	if len(referenced) == 0 && strings.TrimSpace(constraint.ForeignColumn) != "" {
		referenced = []string{constraint.ForeignColumn}
	}
	return &ForeignKey{
		Columns:           constraint.Columns,
		ReferencedTable:   referencedTable(constraint.ForeignTable, owner, builder),
		ReferencedColumns: referenced,
		OnDelete:          Action{Source: constraint.OnDelete, Normalized: onDelete},
		OnUpdate:          Action{Source: constraint.OnUpdate, Normalized: onUpdate},
	}, nil
}

// referencedTable resolves the referenced table's identity in the referencing
// table's schema when the reference carries none of its own.
//
// Resolving here rather than at comparison time is ADR 0001 invariant 3: the
// adapter knows its source's defaulting rule, and a later stage re-parsing the
// name would have to guess it.
func referencedTable(reference string, owner goschema.Table, builder objectidentity.Builder) objectidentity.ID {
	ref, ok := tableref.Parse(reference)
	if !ok {
		return builder.TableParts(owner.Schema, reference)
	}
	if strings.TrimSpace(ref.Schema) == "" {
		return builder.TableParts(owner.Schema, ref.Name)
	}
	return builder.TableParts(ref.Schema, ref.Name)
}

// splitFieldReference parses the `table(column)` spelling.
//
// A reference with no column is refused rather than defaulted to a primary key
// this adapter cannot see. Guessing would target whichever column happens to be
// the key, and the plan would then act on that one.
func splitFieldReference(reference string) (table, column string, err error) {
	trimmed := strings.TrimSpace(reference)
	open := strings.LastIndex(trimmed, "(")
	if open < 0 || !strings.HasSuffix(trimmed, ")") {
		return "", "", fmt.Errorf(
			"foreign key reference %q names no column; the spelling is table(column)", reference)
	}
	table = strings.TrimSpace(trimmed[:open])
	column = strings.TrimSpace(trimmed[open+1 : len(trimmed)-1])
	if table == "" || column == "" {
		return "", "", fmt.Errorf("foreign key reference %q is missing its table or its column", reference)
	}
	return table, column, nil
}

func derefOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
