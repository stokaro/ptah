package compare

import (
	"sort"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
func Enums(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	// Create maps for quick lookup
	genEnums := make(map[string]goschema.Enum)
	for _, enum := range generated.Enums {
		genEnums[enum.Name] = enum
	}

	dbEnums := make(map[string]types.DBEnum)
	for _, enum := range database.Enums {
		dbEnums[enum.Name] = enum
	}

	// Find added and removed enums
	for enumName := range genEnums {
		if _, exists := dbEnums[enumName]; !exists {
			diff.EnumsAdded = append(diff.EnumsAdded, enumName)
		}
	}

	for enumName := range dbEnums {
		if _, exists := genEnums[enumName]; !exists {
			diff.EnumsRemoved = append(diff.EnumsRemoved, enumName)
		}
	}

	// Find modified enums
	for enumName, genEnum := range genEnums {
		if dbEnum, exists := dbEnums[enumName]; exists {
			enumDiff := EnumValues(genEnum, dbEnum)
			if len(enumDiff.ValuesAdded) > 0 || len(enumDiff.ValuesRemoved) > 0 {
				diff.EnumsModified = append(diff.EnumsModified, enumDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(diff.EnumsAdded)
	sort.Strings(diff.EnumsRemoved)
	sort.Slice(diff.EnumsModified, func(i, j int) bool {
		return diff.EnumsModified[i].EnumName < diff.EnumsModified[j].EnumName
	})
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
	enumDiff := difftypes.EnumDiff{EnumName: genEnum.Name}

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
