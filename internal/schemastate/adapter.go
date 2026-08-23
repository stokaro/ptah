package schemastate

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/normalize"
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
	objectidentity.KindPolicy,
	objectidentity.KindGrant,
	objectidentity.KindRole,
}

// The constraint families both sources spell in their untyped Type field.
const (
	foreignKeyConstraintType = "FOREIGN KEY"
	uniqueConstraintType     = "UNIQUE"
	primaryKeyConstraintType = "PRIMARY KEY"
)

// uniquenessConstraintTypes are the constraint families that guarantee a column
// list is a key. A primary key is one of them: it forbids NULL as well, which
// is not what a foreign key's reference asks about.
var uniquenessConstraintTypes = []string{uniqueConstraintType, primaryKeyConstraintType}

// guaranteesUniqueness reports whether a constraint's untyped family name is one
// that makes its columns a key.
func guaranteesUniqueness(constraintType string) bool {
	for _, family := range uniquenessConstraintTypes {
		if strings.EqualFold(constraintType, family) {
			return true
		}
	}
	return false
}

// FromCatalog builds canonical state from a live catalog read.
//
// It is a peer of [FromDescription] rather than a conversion into its shape.
// The tree currently converts in both directions -- internal/convert/toschema
// one way and internal/convert/fromschema the other -- and ADR 0001 decision 6
// deletes the direction question by having both readers produce this state
// directly.
func FromCatalog(schema *dbschematypes.DBSchema, dialect string, semantics identifier.Semantics) (*State, error) {
	// The read's own record of what it did not look at travels with it, for
	// the reason [FromDescription] carries the description's: a state that
	// dropped it would be silent about a family the reader never opened, and a
	// comparison reads that silence as absence. The current side gates
	// ADDITIONS -- an object a description names and a read did not report is
	// a creation only when the read looked -- so losing it here plans a CREATE
	// for something that is already there (stokaro/ptah#1276).
	state := New(dialect, sliceScope...).WithCoverage(schema.NotDescribed)
	builder := objectidentity.NewBuilder(semantics)

	for _, table := range schema.Tables {
		id := builder.TableParts(table.Schema, table.Name)
		columns := columnsFromCatalog(table, builder)
		if existing, collided := state.Add(Object{
			ID: id,
			Table: &Table{
				Columns:         columns,
				EstimatedRows:   table.EstimatedRows,
				RowStatsUnknown: table.RowStatsUnknown,
				Strict:          table.Strict,
				WithoutRowID:    table.WithoutRowID,
			},
			Provenance: Provenance{Source: coverage.Observed, Location: "information_schema.tables"},
		}); collided {
			return nil, fmt.Errorf("catalog reports two tables with one identity: %s and %s", existing.ID, id)
		}
		if err := addColumnKeys(state, builder, table.Schema, table.Name, columns,
			coverage.Observed, "information_schema.columns"); err != nil {
			return nil, err
		}
	}

	for _, constraint := range schema.Constraints {
		if guaranteesUniqueness(constraint.Type) {
			if err := addUniqueKey(state, builder,
				constraint.Schema, constraint.TableName, constraint.Name,
				constraint.ColumnNamesOrDefault(), coverage.Observed, "information_schema.table_constraints",
				strings.EqualFold(constraint.Type, uniqueConstraintType),
			); err != nil {
				return nil, err
			}
			continue
		}
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
			Provenance: Provenance{Source: coverage.Observed, Location: "information_schema.table_constraints"},
		}); collided {
			return nil, fmt.Errorf("catalog reports two foreign keys with one identity: %s and %s", existing.ID, id)
		}
	}
	if err := PoliciesFromCatalog(state, schema, builder); err != nil {
		return nil, err
	}
	if err := RolesFromCatalog(state, schema, builder); err != nil {
		return nil, err
	}
	if err := GrantsFromCatalog(state, schema, builder); err != nil {
		return nil, err
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
			ID: builder.ColumnParts(table.Schema, table.Name, column.Name),
			// RawType and not DataType: a PostgreSQL read reports
			// `code varchar(50)` as DataType "character varying" with the
			// width in a field of its own, so a reader taking DataType alone
			// holds a type with no width -- and every varchar column in the
			// database then reads as modified against the declaration that
			// created it (stokaro/ptah#1662).
			Type:       column.RawType(),
			Nullable:   strings.EqualFold(column.IsNullable, "YES"),
			Unique:     column.IsPrimaryKey || column.IsUnique,
			PrimaryKey: column.IsPrimaryKey,
			Default:    derefOrEmpty(column.ColumnDefault),
			HasDefault: column.ColumnDefault != nil,
			// A catalog reports one string for both kinds of default, so which
			// kind it is has to be decided rather than read. normalize is where
			// that decision already lives, out of the same package
			// migration/schemadiff asks.
			DefaultIsExpression: normalize.IsDefaultExpr(derefOrEmpty(column.ColumnDefault)),
			GeneratedExpression: derefOrEmpty(column.GeneratedExpression),
			GeneratedKind:       column.GeneratedKind,
			IdentityGeneration:  column.IdentityGeneration,
			Charset:             column.Charset,
			Collate:             column.Collate,
			AutoIncrement:       column.IsAutoIncrement,
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
	// The description's own record of what it declines to describe travels
	// with it. Dropping it here is what turns a partial read into a drop: the
	// state would be silent about a family and a comparison would read that
	// silence as absence (stokaro/ptah#1028).
	state := New(dialect, sliceScope...).WithCoverage(description.NotDescribed)
	builder := objectidentity.NewBuilder(semantics)
	tablesByStruct := make(map[string]goschema.Table)

	for _, table := range description.Tables {
		tablesByStruct[table.StructName] = table
		id := builder.TableParts(table.Schema, table.Name)
		columns := columnsFromDescription(description, table, builder)
		if existing, collided := state.Add(Object{
			ID: id,
			Table: &Table{
				Columns:      columns,
				Strict:       table.Strict,
				WithoutRowID: table.WithoutRowID,
				Engine:       table.Engine,
				Charset:      table.Charset,
				Collate:      table.Collate,
				// A description says what a table should look like and nothing
				// about what is in it. Leaving the pair zero would claim the
				// table is empty, which is the answer that lets an ADD COLUMN
				// NOT NULL through.
				RowStatsUnknown: true,
			},
			Provenance: Provenance{Source: coverage.Declared, Location: table.StructName},
		}); collided {
			return nil, fmt.Errorf("description declares two tables with one identity: %s and %s", existing.ID, id)
		}
		if err := addColumnKeys(state, builder, table.Schema, table.Name, columns,
			coverage.Declared, table.StructName); err != nil {
			return nil, err
		}
		// A composite key is declared on the table and covers a list no single
		// column's flag can express.
		if err := addUniqueKey(state, builder, table.Schema, table.Name,
			compositeKeyName(table.Name), table.PrimaryKey,
			coverage.Declared, table.StructName, false); err != nil {
			return nil, err
		}
	}
	if err := declaredUniqueKeys(state, description, builder, tablesByStruct); err != nil {
		return nil, err
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
	if err := PoliciesFromDescription(state, description, builder); err != nil {
		return nil, err
	}
	if err := RolesFromDescription(state, description, builder); err != nil {
		return nil, err
	}
	if err := GrantsFromDescription(state, description, builder); err != nil {
		return nil, err
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
		Provenance: Provenance{Source: coverage.Declared, Location: location},
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
	// A composite key is declared on the TABLE and a single-column one on the
	// field, and both name columns of this table. A reader that took only the
	// field flag described a table with no key at all whenever the author used
	// the composite spelling, and the CREATE it renders would carry none.
	compositeKey := make(map[string]bool, len(table.PrimaryKey))
	for _, name := range table.PrimaryKey {
		compositeKey[strings.TrimSpace(name)] = true
	}
	for _, field := range description.Fields {
		if field.StructName != table.StructName {
			continue
		}
		columns = append(columns, Column{
			ID:       builder.ColumnParts(table.Schema, table.Name, field.Name),
			Type:     field.Type,
			Nullable: field.Nullable,
			// A column in a COMPOSITE key is not unique on its own, so it does
			// not become Unique here. That is conservative in the safe
			// direction -- it blocks a foreign key the target might accept
			// rather than planning one it refuses -- and it is the limitation
			// stokaro/ptah#1662 records against unique constraints as objects.
			Unique:     field.Primary || field.Unique,
			PrimaryKey: field.Primary || compositeKey[field.Name],
			Default:    declaredDefault(field),
			HasDefault: field.DefaultSet || strings.TrimSpace(field.DefaultExpr) != "",
			// A description keeps the two kinds apart in its own model, so this
			// is a read rather than a decision.
			DefaultIsExpression: strings.TrimSpace(field.DefaultExpr) != "",
			GeneratedExpression: field.GeneratedExpression,
			GeneratedKind:       field.GeneratedKind,
			IdentityGeneration:  field.IdentityGeneration,
			Charset:             field.Charset,
			Collate:             field.Collate,
			UpdateExpression:    field.UpdateExpression,
			Check:               field.Check,
			CheckName:           field.CheckName,
			AutoIncrement:       field.AutoInc,
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

// declaredDefault is the default a field declares, in whichever of the two
// spellings the authoring model uses. An expression and a literal are one fact
// here: both answer "does a row without a value get one", and a comparison that
// kept them apart would report a modification for a column nobody changed.
func declaredDefault(field goschema.Field) string {
	if expression := strings.TrimSpace(field.DefaultExpr); expression != "" {
		return expression
	}
	return field.Default
}

// addUniqueKey records one uniqueness guarantee under the identity its owning
// table gives it, refusing a second one with the same identity.
func addUniqueKey(
	state *State,
	builder objectidentity.Builder,
	schema, table, name string,
	columns []string,
	source coverage.Provenance,
	location string,
	standalone bool,
) error {
	if len(columns) == 0 {
		return nil
	}
	id := builder.ConstraintParts(schema, table, name)
	if existing, collided := state.Add(Object{
		ID:         id,
		UniqueKey:  &UniqueKey{Columns: columns, Standalone: standalone},
		Provenance: Provenance{Source: source, Location: location},
	}); collided {
		return fmt.Errorf("two uniqueness guarantees carry one identity: %s and %s", existing.ID, id)
	}
	return nil
}

// singleColumnKeyName is the identity a column's own UNIQUE or PRIMARY KEY flag
// gets when no constraint name reaches Ptah.
//
// A flag on a column is a real uniqueness guarantee and every source spells it
// without a name, so one has to be derived or the guarantee cannot be an object
// at all. It is derived from the column, which is what makes it stable: the
// same column read twice produces the same identity.
func singleColumnKeyName(column string) string {
	return "ptah_unique_" + column
}

// addColumnKeys records the uniqueness a column's own flag declares.
//
// Every source carries at least one of these, and most carry only these for a
// single-column key: a catalog reports IsPrimaryKey and IsUnique on the column
// row rather than always emitting a constraint row, and an authoring model
// spells a one-column key on the field. Deriving an object from the flag is
// what lets one rule answer the uniqueness question for both shapes.
func addColumnKeys(
	state *State,
	builder objectidentity.Builder,
	schema, table string,
	columns []Column,
	source coverage.Provenance,
	location string,
) error {
	for _, column := range columns {
		if !column.Unique {
			continue
		}
		name := column.ID.Name.Source
		if err := addUniqueKey(state, builder, schema, table,
			singleColumnKeyName(name), []string{name}, source, location, false); err != nil {
			return err
		}
	}
	return nil
}

// compositeKeyName is the identity a table-level primary key declaration gets.
// The authoring model spells it without a name, so one is derived from the
// table it belongs to.
func compositeKeyName(table string) string {
	return "ptah_pk_" + table
}

// declaredUniqueKeys records the uniqueness a description declares away from
// its columns: a named UNIQUE or PRIMARY KEY constraint, and a unique index.
//
// A unique INDEX is a uniqueness guarantee the same way a unique CONSTRAINT is
// -- PostgreSQL accepts a foreign key against either -- so both become the same
// object rather than two shapes a later stage has to ask twice.
func declaredUniqueKeys(
	state *State,
	description *goschema.Database,
	builder objectidentity.Builder,
	tablesByStruct map[string]goschema.Table,
) error {
	for _, constraint := range description.Constraints {
		if !guaranteesUniqueness(constraint.Type) {
			continue
		}
		owner, ok := tablesByStruct[constraint.StructName]
		if !ok {
			return fmt.Errorf(
				"constraint %q declares a uniqueness guarantee but no table declares struct %q",
				constraint.Name, constraint.StructName)
		}
		if err := addUniqueKey(state, builder, owner.Schema, owner.Name, constraint.Name,
			constraint.Columns, coverage.Declared, constraint.StructName,
			strings.EqualFold(constraint.Type, uniqueConstraintType)); err != nil {
			return err
		}
	}
	for _, index := range description.Indexes {
		if !index.Unique {
			continue
		}
		owner, ok := tablesByStruct[index.StructName]
		if !ok {
			return fmt.Errorf(
				"index %q declares a uniqueness guarantee but no table declares struct %q",
				index.Name, index.StructName)
		}
		// A unique index is not standalone HERE: it is an index, and indexes
		// are their own family in this issue. It is an object so that a foreign
		// key against its columns is recognized.
		if err := addUniqueKey(state, builder, owner.Schema, owner.Name, index.Name,
			index.Fields, coverage.Declared, index.StructName, false); err != nil {
			return err
		}
	}
	return nil
}
