package compare

import (
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// SearchColumnByName searches for a specific column difference by name within a slice of column diffs.
//
// This utility function provides efficient lookup of column differences by name, which is
// commonly needed when processing migration results or analyzing specific column changes.
// It performs a linear search through the provided slice and returns a pointer to the
// first matching ColumnDiff.
//
// # Search Algorithm
//
// The function uses a simple linear search with O(n) time complexity:
//  1. **Iteration**: Loops through each ColumnDiff in the provided slice
//  2. **Name Matching**: Compares ColumnName field with the target column name
//  3. **Early Return**: Returns immediately upon finding the first match
//  4. **Not Found**: Returns nil if no matching column is found
//
// # Use Cases
//
// **Migration Analysis**:
//   - Check if a specific column has changes before generating migration SQL
//   - Retrieve detailed change information for a particular column
//   - Validate expected changes in automated tests
//
// **Conditional Processing**:
//   - Apply different migration strategies based on specific column changes
//   - Skip certain operations if particular columns are not modified
//   - Generate warnings for potentially dangerous column modifications
//
// # Example Usage
//
// **Finding a specific column change**:
//
//	```go
//	tableDiff := compare.TableColumns(genTable, dbTable, generated)
//	emailDiff := compare.ColumnByName(tableDiff.ColumnsModified, "email")
//	if emailDiff != nil {
//	    if _, hasTypeChange := emailDiff.Changes["type"]; hasTypeChange {
//	        log.Println("Email column type is changing")
//	    }
//	}
//	```
//
// **Validation in tests**:
//
//	```go
//	result := compare.TableColumns(genTable, dbTable, generated)
//	nameDiff := compare.ColumnByName(result.ColumnsModified, "name")
//	c.Assert(nameDiff, qt.IsNotNil)
//	c.Assert(nameDiff.Changes["type"], qt.Equals, "varchar -> text")
//	```
//
// **Conditional migration logic**:
//
//	```go
//	for _, tableDiff := range schemaDiff.TablesModified {
//	    if pkDiff := compare.ColumnByName(tableDiff.ColumnsModified, "id"); pkDiff != nil {
//	        if _, hasPKChange := pkDiff.Changes["primary_key"]; hasPKChange {
//	            // Handle primary key changes with special care
//	            generatePrimaryKeyMigration(tableDiff.TableName, pkDiff)
//	        }
//	    }
//	}
//	```
//
// # Parameters
//
//   - diffs: Slice of ColumnDiff structures to search through
//   - columnName: Name of the column to find (case-sensitive exact match)
//
// # Return Value
//
// Returns a pointer to the first ColumnDiff with matching ColumnName, or nil if not found.
// The returned pointer references the original ColumnDiff in the slice, so modifications
// will affect the original data structure.
//
// # Performance Considerations
//
// - Time Complexity: O(n) where n is the number of column diffs
// - Space Complexity: O(1) - no additional memory allocation
// - For large numbers of columns, consider using a map-based lookup if called frequently
//
// # Thread Safety
//
// This function is read-only and thread-safe when used concurrently on the same data.
// However, if the underlying slice is being modified concurrently, appropriate
// synchronization is required.
//
// # Edge Cases
//
// - Empty slice: Returns nil immediately
// - Nil slice: Returns nil immediately (no panic)
// - Duplicate column names: Returns the first match encountered
// - Case sensitivity: Performs exact string matching (case-sensitive)
func SearchColumnByName(diffs []difftypes.ColumnDiff, columnName string) *difftypes.ColumnDiff {
	for _, diff := range diffs {
		if diff.ColumnName == columnName {
			return &diff
		}
	}
	return nil
}
