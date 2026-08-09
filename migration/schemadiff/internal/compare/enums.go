package compare

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Enums performs comprehensive enum type comparison between generated and database schemas.
//
// This function handles the comparison of enum type definitions, which is particularly
// complex due to database-specific enum implementations and the challenges of enum
// value modification across different database systems.
//
// # Database-Specific Enum Handling
//
// **PostgreSQL**:
//   - Native ENUM types with CREATE TYPE statements
//   - Supports adding enum values but not removing them easily
//   - Enum values are stored in system catalogs
//
// **MySQL/MariaDB**:
//   - Inline ENUM syntax in column definitions
//   - Supports both adding and removing enum values
//   - Enum values are part of column type definition
//
// **SQLite**:
//   - No native enum support
//   - Uses CHECK constraints for enum-like behavior
//
// # Comparison Algorithm
//
// The function performs comparison in three phases:
//  1. **Enum Discovery**: Creates lookup maps for efficient enum comparison
//  2. **Addition/Removal Detection**: Identifies new and removed enum types
//  3. **Value Modification Analysis**: Compares enum values for existing types
//
// # Example Scenarios
//
// **New enum detection**:
//   - Generated schema defines "status_type" enum
//   - Database doesn't have "status_type" enum
//   - Result: "status_type" added to diff.EnumsAdded
//
// **Enum value addition**:
//   - Both have "priority_level" enum
//   - Generated: ["low", "medium", "high", "critical"]
//   - Database: ["low", "medium", "high"]
//   - Result: EnumDiff with ValuesAdded=["critical"]
//
// **Enum value removal** (problematic):
//   - Generated: ["active", "inactive"]
//   - Database: ["active", "inactive", "deprecated"]
//   - Result: EnumDiff with ValuesRemoved=["deprecated"]
//   - Note: May require manual intervention in PostgreSQL
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.EnumsAdded: Enum types that need to be created
//   - diff.EnumsRemoved: Enum types that exist in database but not in target schema
//   - diff.EnumsModified: Enum types with value differences
//
// # Migration Considerations
//
// Enum modifications can be complex:
//   - Adding values is generally safe
//   - Removing values may require data migration
//   - PostgreSQL enum removal requires recreating the enum type
//   - MySQL enum changes require ALTER TABLE statements
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
// Enums compares enum types under the connection's default schema.
//
// It delegates to [EnumsWithSemantics] with zero semantics, which is the
// no-default-schema rule: a blank schema stays blank and only matches a blank
// one. Callers that have a live connection have a default schema and should
// pass it.
func Enums(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	EnumsWithSemantics(generated, database, diff, identifier.Semantics{})
}

// EnumsWithSemantics compares enum types by (schema, name) identity.
//
// An enum is a TYPE, and a type's identity is its schema and its name together:
// public.mood and extra.mood hold different values, so keying on the bare name
// made a read covering two schemas collapse them into one and report a changed
// enum as converged. Every name this puts on the diff is qualified wherever the
// enum names a schema, which is what lets the planner emit
// `CREATE TYPE "extra"."mood"` instead of creating it wherever the connection
// happens to point (stokaro/ptah#1276).
//
// The matching rule is the one views and functions use, and it is deliberately
// ASYMMETRIC, because a blank schema means a different thing on each side:
//
//   - On the DATABASE side, blank means the connection's own schema, which the
//     reader blanks by convention. It is canonicalized against
//     semantics.DefaultSchema before anything is compared.
//   - On the GENERATED side, blank means UNQUALIFIED -- the author named no
//     schema. A Go annotation cannot name one at all, so an introspected
//     brownfield schema round-tripped through annotations arrives here with an
//     enum that names none against a read that names the schema it was scoped
//     to. Such a name matches a uniquely-named enum wherever it lives, and
//     nothing when two schemas hold that name, because guessing there is what
//     would attribute the type to a schema it does not belong to.
//
// Both halves are load-bearing and each was measured. Without the first, an
// `enum` block's `schema = schema.public` -- which is mandatory, the pinned
// Atlas community binary v1.3.0 having no reading for a block without one --
// did not match the read's blank, and a `schema inspect` document applied back
// to its own database planned
//
//	CREATE TYPE "public"."p_color" AS ENUM ('red', 'green');
//	DROP TYPE IF EXISTS "p_color" CASCADE;
//
// Without the second, `ptah introspect --schemas <s>` followed by a comparison
// of the generated annotations against the same read reported
//
//	EnumsAdded:   {"status_type"}
//	EnumsRemoved: {"<s>.status_type"}
//
// for a schema nothing had changed. Both on PostgreSQL 17.10.
func EnumsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	dbByIdentity := make(map[tableIdentity]types.DBEnum, len(database.Enums))
	dbByName := make(map[string][]types.DBEnum, len(database.Enums))
	for _, enum := range database.Enums {
		schema, name := enumParts(enum.Schema, enum.Name)
		dbByIdentity[newTableIdentity(schema, name, semantics)] = enum
		dbByName[semantics.TableIdentityKey(name)] = append(dbByName[semantics.TableIdentityKey(name)], enum)
	}

	matched := make(map[string]struct{}, len(database.Enums))
	for _, genEnum := range generated.Enums {
		dbEnum, exists := findDatabaseEnum(genEnum, dbByIdentity, dbByName, semantics)
		if !exists {
			diff.EnumsAdded = append(diff.EnumsAdded, genEnum.QualifiedName())
			continue
		}
		matched[dbEnum.QualifiedName()] = struct{}{}
		enumDiff := EnumValues(genEnum, dbEnum)
		if len(enumDiff.ValuesAdded) > 0 || len(enumDiff.ValuesRemoved) > 0 {
			diff.EnumsModified = append(diff.EnumsModified, enumDiff)
		}
	}

	for _, enum := range database.Enums {
		if _, ok := matched[enum.QualifiedName()]; ok {
			continue
		}
		diff.EnumsRemoved = append(diff.EnumsRemoved, enum.QualifiedName())
	}

	// Sort for consistent output
	sort.Strings(diff.EnumsAdded)
	sort.Strings(diff.EnumsRemoved)
	sort.Slice(diff.EnumsModified, func(i, j int) bool {
		return diff.EnumsModified[i].EnumName < diff.EnumsModified[j].EnumName
	})
}

// findDatabaseEnum resolves one generated enum against the read.
func findDatabaseEnum(
	genEnum goschema.Enum,
	dbByIdentity map[tableIdentity]types.DBEnum,
	dbByName map[string][]types.DBEnum,
	semantics identifier.Semantics,
) (types.DBEnum, bool) {
	schema, name := enumParts(genEnum.Schema, genEnum.Name)
	if strings.TrimSpace(schema) != "" {
		enum, ok := dbByIdentity[newTableIdentity(schema, name, semantics)]
		return enum, ok
	}
	candidates := dbByName[semantics.TableIdentityKey(name)]
	if len(candidates) != 1 {
		return types.DBEnum{}, false
	}
	return candidates[0], true
}

// enumParts splits one enum into the schema and name to compare it under.
//
// It reads a qualifier out of the NAME when the schema field is empty, because
// not every producer fills the field: a SQL schema file loaded through
// internal/convert/toschema names an enum `public.e1` outright. Without this,
// that spelling and the reader's separate (public, e1) were two different
// enums.
//
// tableref.Parse is what separates a qualifier from a literal dot -- an enum
// named "tenant.data" is one quoted part and stays whole -- so this cannot turn
// a name into a schema it never had.
func enumParts(schema, name string) (string, string) {
	if strings.TrimSpace(schema) != "" {
		return schema, name
	}
	if ref, ok := tableref.Parse(name); ok && ref.Qualified {
		return ref.Schema, ref.Name
	}
	return "", name
}

// EnumValues performs detailed value-level comparison between generated and database enum types.
//
// This function analyzes the specific values within an enum type to determine what
// changes are needed to bring the database enum in line with the generated enum
// definition. It uses set-based comparison for efficient value difference detection.
//
// # Algorithm Details
//
// The function uses a set-based approach for optimal performance:
//  1. **Set Creation**: Converts value slices to boolean maps for O(1) lookup
//  2. **Addition Detection**: Finds values in generated enum but not in database
//  3. **Removal Detection**: Finds values in database enum but not in generated
//  4. **Result Sorting**: Ensures deterministic output for consistent migrations
//
// # Example Scenarios
//
// **Value addition**:
//
//	```
//	Generated: ["draft", "published", "archived", "deleted"]
//	Database:  ["draft", "published", "archived"]
//	Result:    ValuesAdded=["deleted"], ValuesRemoved=[]
//	```
//
// **Value removal**:
//
//	```
//	Generated: ["active", "inactive"]
//	Database:  ["active", "inactive", "deprecated", "obsolete"]
//	Result:    ValuesAdded=[], ValuesRemoved=["deprecated", "obsolete"]
//	```
//
// **Mixed changes**:
//
//	```
//	Generated: ["pending", "approved", "rejected", "canceled"]
//	Database:  ["pending", "approved", "denied"]
//	Result:    ValuesAdded=["rejected", "canceled"], ValuesRemoved=["denied"]
//	```
//
// # Performance Characteristics
//
// - Time Complexity: O(n + m) where n=generated values, m=database values
// - Space Complexity: O(n + m) for the boolean maps
// - Optimized for large enum value sets with efficient set operations
//
// # Parameters
//
//   - genEnum: Generated enum definition from Go struct annotations
//   - dbEnum: Current database enum from introspection
//
// # Return Value
//
// Returns an EnumDiff containing:
//   - EnumName: Name of the enum being compared
//   - ValuesAdded: Values that need to be added to the database enum
//   - ValuesRemoved: Values that exist in database but not in generated enum
//
// # Migration Implications
//
// **Adding values**: Generally safe operation across all databases
// **Removing values**: May require careful consideration:
//   - Check if removed values are used in existing data
//   - PostgreSQL requires enum recreation for value removal
//   - MySQL allows value removal but may affect existing data
//
// # Output Consistency
//
// Value lists are sorted alphabetically to ensure deterministic migration
// generation and reliable testing across multiple runs.
func EnumValues(genEnum goschema.Enum, dbEnum types.DBEnum) difftypes.EnumDiff {
	// The qualified name, so the planner can name the type it has to alter.
	// It is the bare name for every enum that names no schema, which is every
	// enum a Go annotation can declare (stokaro/ptah#1276).
	enumDiff := difftypes.EnumDiff{EnumName: genEnum.QualifiedName()}

	// Create sets for comparison
	genValues := make(map[string]bool)
	for _, value := range genEnum.Values {
		genValues[value] = true
	}

	dbValues := make(map[string]bool)
	for _, value := range dbEnum.Values {
		dbValues[value] = true
	}

	// Find added and removed values
	for value := range genValues {
		if !dbValues[value] {
			enumDiff.ValuesAdded = append(enumDiff.ValuesAdded, value)
		}
	}

	for value := range dbValues {
		if !genValues[value] {
			enumDiff.ValuesRemoved = append(enumDiff.ValuesRemoved, value)
		}
	}

	// Sort for consistent output
	sort.Strings(enumDiff.ValuesAdded)
	sort.Strings(enumDiff.ValuesRemoved)

	return enumDiff
}
