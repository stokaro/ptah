package compare

import (
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Constraints compares constraint definitions between generated and database schemas.
//
// This function identifies differences in table-level constraints such as EXCLUDE,
// CHECK, UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints. It compares constraints
// defined through Go struct annotations with constraints that exist in the database.
//
// # Constraint Types Supported
//
//   - EXCLUDE: PostgreSQL EXCLUDE constraints for preventing conflicts
//   - CHECK: Table-level CHECK constraints for data validation
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
//
// # Comparison Logic
//
// The function performs constraint comparison by:
//  1. **Constraint Discovery**: Creates lookup maps for efficient constraint comparison
//  2. **Addition Detection**: Identifies constraints in generated schema but not in database
//  3. **Removal Detection**: Identifies constraints in database but not in generated schema
//
// # Database Schema Constraints
//
// The function currently focuses on constraints defined through schema annotations.
// Database-introspected constraints are not yet fully supported, so this function
// primarily detects constraint additions from the generated schema.
//
// # Example Usage
//
//	// Compare constraints between schemas
//	compare.Constraints(generated, database, diff)
//
//	// Check for constraint changes
//	if len(diff.ConstraintsAdded) > 0 {
//		log.Printf("Found %d new constraints to add", len(diff.ConstraintsAdded))
//	}
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: Schema difference structure to populate with constraint changes
//
// # Limitations
//
//   - Database constraint introspection is not yet fully implemented
//   - Currently focuses on constraint additions from generated schema
//   - Constraint modifications are not yet detected
func Constraints(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff, opts *config.CompareOptions) {
	dialect := ""
	if opts != nil {
		dialect = opts.Dialect
	}

	// Create maps for detailed constraint comparison
	genConstraints := make(map[string]goschema.Constraint)
	for _, constraint := range generated.Constraints {
		// Use table.constraint_name as the key for comparison to handle constraints with same names in different tables
		key := constraint.Table + "." + constraint.Name
		genConstraints[key] = constraint
	}

	// Synthesize table-level Constraint entries from field-level `check=`
	// annotations so they participate in drift comparison alongside table
	// constraints from `//migrator:schema:constraint`. Only synthesized for
	// columns that already exist in the database — new tables/columns get
	// their CHECK inline via CREATE TABLE / ALTER TABLE ADD COLUMN, and
	// double-emitting an ALTER TABLE ADD CONSTRAINT would fail because the
	// constraint is created in the same migration step.
	for _, synthesized := range synthesizeFieldLevelCheckConstraints(generated, database) {
		key := synthesized.Table + "." + synthesized.Name
		// Don't clobber an explicit table-level constraint that happens to
		// share the same name.
		if _, exists := genConstraints[key]; !exists {
			genConstraints[key] = synthesized
		}
	}

	for _, synthesized := range synthesizeTablePrimaryKeyConstraints(generated, database, dialect) {
		key := synthesized.Table + "." + synthesized.Name
		// Don't clobber an explicit table-level constraint that happens to
		// share the same name.
		if _, exists := genConstraints[key]; !exists {
			genConstraints[key] = synthesized
		}
	}

	// Synthesize table-level Constraint entries from field-level `foreign=`
	// annotations so on_delete / on_update drift on an existing field-level
	// FK participates in comparison (issue #189), mirroring the field-level
	// CHECK synthesis above. Only synthesized for columns that already exist
	// in the database — new tables/columns get their FK inline via CREATE
	// TABLE / ALTER TABLE ADD CONSTRAINT, so emitting an ADD CONSTRAINT here
	// would double-create it in the same migration step.
	//
	// synthesizedFKKeys records the table.constraint_name of every synthesized
	// field-level FK so isFieldLevelConstraint can let the matching DB-side FK
	// through to the comparison instead of filtering it out — otherwise
	// foreignKeyConstraintChanged would never run for field-level FKs.
	synthesizedFKKeys := make(map[string]struct{})
	for _, synthesized := range synthesizeFieldLevelForeignKeyConstraints(generated, database) {
		key := synthesized.Table + "." + synthesized.Name
		synthesizedFKKeys[key] = struct{}{}
		// Don't clobber an explicit table-level constraint that happens to
		// share the same name.
		if _, exists := genConstraints[key]; !exists {
			genConstraints[key] = synthesized
		}
	}

	// Create map of existing database constraints, filtering out field-level constraints
	dbConstraints := make(map[string]types.DBConstraint)
	for _, constraint := range database.Constraints {
		// Skip field-level constraints that are represented in field definitions
		if isFieldLevelConstraint(constraint, generated, synthesizedFKKeys) {
			continue
		}

		// Use table.constraint_name as the key for comparison
		key := constraint.QualifiedTableName() + "." + constraint.Name
		dbConstraints[key] = constraint
	}

	// Find added constraints (constraints in generated schema but not in database)
	for constraintKey, genConstraint := range genConstraints {
		if _, exists := dbConstraints[constraintKey]; !exists {
			diff.ConstraintsAdded = append(diff.ConstraintsAdded, genConstraint.Name)
			diff.ConstraintsAddedWithTables = appendConstraintAddition(diff.ConstraintsAddedWithTables, genConstraint)
		}
	}

	// Find removed constraints (constraints in database but not in generated schema)
	for constraintKey, dbConstraint := range dbConstraints {
		if _, exists := genConstraints[constraintKey]; !exists {
			diff.ConstraintsRemoved = append(diff.ConstraintsRemoved, dbConstraint.Name)
			diff.ConstraintsRemovedWithTables = appendConstraintRemoval(diff.ConstraintsRemovedWithTables, dbConstraint)
		}
	}

	// Find modified constraints (constraints that exist in both but have different definitions)
	for constraintKey, genConstraint := range genConstraints {
		if dbConstraint, exists := dbConstraints[constraintKey]; exists {
			if constraintDefinitionsChanged(genConstraint, dbConstraint, dialect) {
				// For now, treat modified constraints as removed + added
				// In the future, we could add a ConstraintsModified field to SchemaDiff
				diff.ConstraintsRemoved = append(diff.ConstraintsRemoved, dbConstraint.Name)
				diff.ConstraintsRemovedWithTables = appendConstraintRemoval(diff.ConstraintsRemovedWithTables, dbConstraint)
				diff.ConstraintsAdded = append(diff.ConstraintsAdded, genConstraint.Name)
				diff.ConstraintsAddedWithTables = appendConstraintAddition(diff.ConstraintsAddedWithTables, genConstraint)
			}
		}
	}

	// Sort for consistent output. Planners pair the bare name lists with the
	// *WithTables slices through name-keyed maps, not by index, so each list
	// can be sorted independently.
	sort.Strings(diff.ConstraintsAdded)
	sort.Strings(diff.ConstraintsRemoved)
	sort.Slice(diff.ConstraintsAddedWithTables, func(i, j int) bool {
		a, b := diff.ConstraintsAddedWithTables[i], diff.ConstraintsAddedWithTables[j]
		if a.TableName != b.TableName {
			return a.TableName < b.TableName
		}
		return a.Name < b.Name
	})
	sort.Slice(diff.ConstraintsRemovedWithTables, func(i, j int) bool {
		a, b := diff.ConstraintsRemovedWithTables[i], diff.ConstraintsRemovedWithTables[j]
		if a.TableName != b.TableName {
			return a.TableName < b.TableName
		}
		return a.Name < b.Name
	})
}

// appendConstraintRemoval records the table-qualified removal info for a
// database constraint that is being dropped (or modified, which is expressed as
// remove + add). Dialects that need the owning table and a type-specific drop
// syntax — MySQL/MariaDB FOREIGN KEY uses DROP FOREIGN KEY rather than DROP
// CONSTRAINT — read this parallel slice instead of the bare name list.
func appendConstraintRemoval(infos []difftypes.ConstraintRemovalInfo, dbConstraint types.DBConstraint) []difftypes.ConstraintRemovalInfo {
	return append(infos, difftypes.ConstraintRemovalInfo{
		Name:      dbConstraint.Name,
		TableName: dbConstraint.QualifiedTableName(),
		Type:      dbConstraint.Type,
	})
}

// appendConstraintAddition records the table-qualified definition of a
// constraint that is being added (or modified, which is expressed as remove +
// add). The bare ConstraintsAdded name list cannot disambiguate a field-level
// FK whose name repeats across several tables — the canonical case being an
// embedded inline-relation mixin (e.g. fk_entity_tenant on every table that
// embeds a tenant-aware base struct, issue #197). Planners read this parallel
// slice to emit one correctly-targeted ALTER TABLE per real host table instead
// of re-deriving the table from a field's Go struct name (which, for a mixin,
// is not a table at all). For a unique-named constraint this carries exactly
// one entry and matches the previous field-scan behavior.
func appendConstraintAddition(infos []difftypes.ConstraintAdditionInfo, genConstraint goschema.Constraint) []difftypes.ConstraintAdditionInfo {
	return append(infos, difftypes.ConstraintAdditionInfo{
		Name:            genConstraint.Name,
		TableName:       genConstraint.Table,
		Type:            genConstraint.Type,
		Columns:         append([]string(nil), genConstraint.Columns...),
		NullsDistinct:   cloneBoolPtr(genConstraint.NullsDistinct),
		CheckExpression: genConstraint.CheckExpression,
		ForeignTable:    genConstraint.ForeignTable,
		ForeignColumn:   genConstraint.ForeignColumn,
		ForeignColumns:  append([]string(nil), genConstraint.ForeignColumnsOrDefault()...),
		OnDelete:        genConstraint.OnDelete,
		OnUpdate:        genConstraint.OnUpdate,
	})
}

// constraintDefinitionsChanged compares constraint definitions between generated and database schemas
// to detect if a constraint needs to be recreated due to definition changes.
func constraintDefinitionsChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint, dialect string) bool {
	// Basic constraint type comparison
	if genConstraint.Type != dbConstraint.Type {
		return true
	}

	// Type-specific comparisons
	switch genConstraint.Type {
	case "EXCLUDE":
		return excludeConstraintChanged(genConstraint, dbConstraint)
	case "CHECK":
		return checkConstraintChanged(genConstraint, dbConstraint)
	case "UNIQUE":
		return uniqueConstraintChanged(genConstraint, dbConstraint)
	case "PRIMARY KEY":
		return primaryKeyConstraintChanged(genConstraint, dbConstraint)
	case "FOREIGN KEY":
		return foreignKeyConstraintChanged(genConstraint, dbConstraint, dialect)
	default:
		// For unknown constraint types, assume no change
		return false
	}
}

func primaryKeyConstraintChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint) bool {
	return !slices.Equal(genConstraint.Columns, dbConstraint.ColumnNamesOrDefault())
}

// excludeConstraintChanged compares EXCLUDE constraint definitions
func excludeConstraintChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint) bool {
	// Compare using method
	if genConstraint.UsingMethod != getStringValue(dbConstraint.UsingMethod) {
		return true
	}

	// Compare exclude elements
	if genConstraint.ExcludeElements != getStringValue(dbConstraint.ExcludeElements) {
		return true
	}

	// Compare WHERE condition
	if genConstraint.WhereCondition != getStringValue(dbConstraint.WhereCondition) {
		return true
	}

	return false
}

// checkConstraintChanged compares CHECK constraint definitions.
func checkConstraintChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint) bool {
	dbClause := getStringValue(dbConstraint.CheckClause)
	if strings.TrimSpace(genConstraint.CheckExpression) == "" || strings.TrimSpace(dbClause) == "" {
		return false
	}
	if checkExpressionHasUnsupportedRewrite(genConstraint.CheckExpression, dbClause) {
		return false
	}
	return normalizeCheckExpression(genConstraint.CheckExpression) != normalizeCheckExpression(dbClause)
}

// uniqueConstraintChanged compares UNIQUE constraint definitions
func uniqueConstraintChanged(genConstraint goschema.Constraint, dbConstraint types.DBConstraint) bool {
	return !stringSetsEqual(genConstraint.Columns, dbConstraint.ColumnNamesOrDefault()) ||
		!boolPtrEqual(genConstraint.NullsDistinct, dbConstraint.NullsDistinct)
}
