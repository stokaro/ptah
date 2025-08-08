package goschema

import (
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
)

func sortTablesProcessQueue(queue *[]string, sorted *[]Table, dependencies map[string][]string, inDegree map[string]int, tableMap map[string]Table) {
	for len(*queue) > 0 {
		// Remove first element from queue
		current := (*queue)[0]
		*queue = (*queue)[1:]

		// Add to sorted result if table exists
		if table, exists := tableMap[current]; exists {
			*sorted = append(*sorted, table)
		}

		// Reduce in-degree of tables that depend on the current table
		for tableName, deps := range dependencies {
			for _, dep := range deps {
				if dep != current {
					continue
				}
				inDegree[tableName]--
				if inDegree[tableName] == 0 {
					*queue = append(*queue, tableName)
				}
			}
		}
	}
}

func checkForCircularDependencies(r *Database, sorted *[]Table) {
	if len(*sorted) == len(r.Tables) {
		// No circular dependencies
		return
	}

	slog.Warn("Circular dependency detected in foreign key relationships. Some tables may not be ordered correctly.")
	// Add remaining tables to the end
	for _, table := range r.Tables {
		found := false
		for _, sortedTable := range *sorted {
			if sortedTable.Name == table.Name {
				found = true
				break
			}
		}
		if !found {
			*sorted = append(*sorted, table)
		}
	}
}

// sortTablesByDependencies performs topological sort to order tables by their dependencies.
//
// This method implements Kahn's algorithm for topological sorting to determine the correct
// order for creating database tables. Tables with no dependencies are created first,
// followed by tables that depend on them, ensuring that foreign key constraints can be
// satisfied during migration execution.
//
// Algorithm steps:
//  1. Calculate in-degrees (number of dependencies) for each table
//  2. Initialize queue with tables that have no dependencies (in-degree 0)
//  3. Process queue: remove table, add to sorted result, reduce in-degrees of dependent tables
//  4. Continue until all tables are processed or circular dependency is detected
//
// Circular dependency handling:
//   - If circular dependencies are detected, a warning is logged
//   - Remaining tables are appended to the end of the sorted list
//   - This allows migration to proceed, but manual intervention may be needed
//
// The method modifies the Tables slice in-place, reordering it according to dependency
// requirements. This ensures that CREATE TABLE statements can be executed in the
// returned order without foreign key constraint violations.
func sortTablesByDependencies(r *Database) {
	// Create a map for quick table lookup
	tableMap := make(map[string]Table)
	for _, table := range r.Tables {
		tableMap[table.Name] = table
	}

	// Perform topological sort using Kahn's algorithm
	var sorted []Table
	inDegree := make(map[string]int)

	// Calculate in-degrees (how many dependencies each table has)
	for tableName := range r.Dependencies {
		inDegree[tableName] = 0
	}
	for tableName, deps := range r.Dependencies {
		inDegree[tableName] = len(deps)
	}

	// Find tables with no dependencies (in-degree 0)
	var queue []string
	for tableName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, tableName)
		}
	}

	// Process queue
	sortTablesProcessQueue(&queue, &sorted, r.Dependencies, inDegree, tableMap)

	// Check for circular dependencies
	checkForCircularDependencies(r, &sorted)

	// Update the tables slice with sorted order
	r.Tables = sorted
}

// buildDependencyGraph analyzes foreign key relationships to build a dependency graph.
//
// This method examines all fields and embedded fields to identify foreign key relationships
// and builds a dependency graph that maps each table to the tables it depends on. This
// information is crucial for determining the correct order of table creation to satisfy
// foreign key constraints.
//
// The analysis process:
//  1. Initializes empty dependency lists for all tables
//  2. Scans all fields for foreign key references (field.Foreign attribute)
//  3. Scans embedded fields with relation mode for references (embedded.Ref attribute)
//  4. Extracts referenced table names from foreign key specifications
//  5. Maps each table to its list of dependencies
//
// Foreign key format examples:
//   - "users(id)" -> depends on "users" table
//   - "categories(uuid)" -> depends on "categories" table
//
// The resulting dependency graph is stored in the Dependencies field and used by
// sortTablesByDependencies() to perform topological sorting.
func buildDependencyGraph(r *Database) {
	// Initialize dependencies map for all tables
	for _, table := range r.Tables {
		r.Dependencies[table.Name] = []string{}
	}

	// Analyze foreign key relationships
	for _, field := range r.Fields {
		if field.Foreign == "" {
			continue
		}
		// Parse foreign key reference (e.g., "users(id)" -> "users")
		refTable := strings.Split(field.Foreign, "(")[0]

		// Find the table that contains this field
		for _, table := range r.Tables {
			if table.StructName != field.StructName {
				continue
			}
			// Add dependency: table depends on refTable
			if !slices.Contains(r.Dependencies[table.Name], refTable) {
				r.Dependencies[table.Name] = append(r.Dependencies[table.Name], refTable)
			}
			break
		}
	}

	// Analyze embedded field relationships (relation mode)
	for _, embedded := range r.EmbeddedFields {
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}

		// Parse embedded relation reference (e.g., "users(id)" -> "users")
		refTable := strings.Split(embedded.Ref, "(")[0]

		// Find the table that contains this embedded field
		for _, table := range r.Tables {
			if table.StructName != embedded.StructName {
				continue
			}
			// Add dependency: table depends on refTable
			if !slices.Contains(r.Dependencies[table.Name], refTable) {
				r.Dependencies[table.Name] = append(r.Dependencies[table.Name], refTable)
			}
			break
		}
	}

	// Analyze function dependencies (functions may call other functions)
	buildFunctionDependencies(r)
}

// buildFunctionDependencies analyzes function body content to identify function-to-function dependencies.
//
// This method examines function bodies to identify calls to other functions and builds
// dependency relationships. This ensures that functions are created in the correct order
// when one function calls another.
//
// The analysis process:
//  1. Scans each function's body for function calls
//  2. Identifies references to other functions defined in the same schema
//  3. Builds dependency relationships between functions
//  4. Stores dependencies in a separate map for function ordering
//
// Function call detection:
//   - Looks for function names followed by parentheses in function bodies
//   - Only considers functions that are defined in the current schema
//   - Handles both simple calls and calls within expressions
//
// Example:
//
//	Function A calls Function B -> Function A depends on Function B
//	Function B must be created before Function A
func buildFunctionDependencies(r *Database) {
	// Create a map of all function names for quick lookup
	functionNames := make(map[string]bool)
	for _, function := range r.Functions {
		functionNames[function.Name] = true
	}

	// Initialize function dependencies map if it doesn't exist
	if r.FunctionDependencies == nil {
		r.FunctionDependencies = make(map[string][]string)
	}

	// Initialize dependencies for all functions
	for _, function := range r.Functions {
		r.FunctionDependencies[function.Name] = []string{}
	}

	// Pre-compile all regex patterns for better performance (avoid compilation in nested loops)
	regexMap := make(map[string]*regexp.Regexp)
	for otherFunctionName := range functionNames {
		pattern := `\b` + regexp.QuoteMeta(otherFunctionName) + `\s*\(`
		regexMap[otherFunctionName] = regexp.MustCompile(pattern)
	}

	// Analyze each function's body for calls to other functions
	for _, function := range r.Functions {
		body := function.Body
		depMap := make(map[string]bool)

		// Look for function calls in the body using pre-compiled regexes
		for otherFunctionName := range functionNames {
			if otherFunctionName == function.Name {
				continue // Skip self-references
			}

			// Use pre-compiled regex to match function calls: function_name(
			// This matches the function name as a word, optional whitespace, then '('
			// This avoids false positives in comments or string literals
			re := regexMap[otherFunctionName]
			if re.FindStringIndex(body) != nil {
				// Add dependency: current function depends on the called function
				depMap[otherFunctionName] = true
			}
		}

		// Convert depMap keys to a slice and assign to FunctionDependencies
		deps := make([]string, 0, len(depMap))
		for dep := range depMap {
			deps = append(deps, dep)
		}
		r.FunctionDependencies[function.Name] = deps
	}
}

// sortFunctionsByDependencies performs topological sort to order functions by their dependencies.
//
// This method implements Kahn's algorithm for topological sorting to determine the correct
// order for creating PostgreSQL functions. Functions with no dependencies are created first,
// followed by functions that depend on them, ensuring that function calls can be resolved
// during function creation.
func sortFunctionsByDependencies(r *Database) {
	if len(r.Functions) == 0 {
		return
	}

	functionMap := buildFunctionMap(r.Functions)
	sorted := performTopologicalSort(r.FunctionDependencies, functionMap)
	handleCircularDependencies(&sorted, r.Functions)
	r.Functions = sorted
}

// buildFunctionMap creates a map for quick function lookup by name.
func buildFunctionMap(functions []Function) map[string]Function {
	functionMap := make(map[string]Function)
	for _, function := range functions {
		functionMap[function.Name] = function
	}
	return functionMap
}

// performTopologicalSort implements Kahn's algorithm for function dependency sorting.
func performTopologicalSort(dependencies map[string][]string, functionMap map[string]Function) []Function {
	var sorted []Function
	inDegree := calculateInDegrees(dependencies)
	queue := findZeroDegreeNodes(inDegree)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if function, exists := functionMap[current]; exists {
			sorted = append(sorted, function)
		}

		queue = updateInDegreesAndQueue(current, dependencies, inDegree, queue)
	}

	return sorted
}

// calculateInDegrees calculates how many dependencies each function has.
func calculateInDegrees(dependencies map[string][]string) map[string]int {
	inDegree := make(map[string]int)
	for functionName := range dependencies {
		inDegree[functionName] = len(dependencies[functionName])
	}
	return inDegree
}

// findZeroDegreeNodes finds functions with no dependencies.
func findZeroDegreeNodes(inDegree map[string]int) []string {
	var queue []string
	for functionName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, functionName)
		}
	}
	return queue
}

// updateInDegreesAndQueue reduces in-degrees and updates the processing queue.
func updateInDegreesAndQueue(current string, dependencies map[string][]string, inDegree map[string]int, queue []string) []string {
	for functionName, deps := range dependencies {
		for _, dep := range deps {
			if dep == current {
				inDegree[functionName]--
				if inDegree[functionName] == 0 {
					queue = append(queue, functionName)
				}
			}
		}
	}
	return queue
}

// handleCircularDependencies detects and handles circular dependencies in function relationships.
func handleCircularDependencies(sorted *[]Function, allFunctions []Function) {
	if len(*sorted) != len(allFunctions) {
		slog.Warn("Circular dependency detected in function relationships. Some functions may not be ordered correctly.")
		addRemainingFunctions(sorted, allFunctions)
	}
}

// addRemainingFunctions adds any functions not included in the sorted list to the end.
func addRemainingFunctions(sorted *[]Function, allFunctions []Function) {
	for _, function := range allFunctions {
		if !isFunctionInSorted(function, *sorted) {
			*sorted = append(*sorted, function)
		}
	}
}

// isFunctionInSorted checks if a function is already in the sorted list.
func isFunctionInSorted(function Function, sorted []Function) bool {
	for _, sortedFunction := range sorted {
		if sortedFunction.Name == function.Name {
			return true
		}
	}
	return false
}

// deduplicate removes duplicate entities that may be defined in multiple files.
//
// During recursive parsing, the same entity might be encountered multiple times
// if it's defined in different files or referenced across packages. This method
// ensures that each unique entity appears only once in the final result.
//
// The deduplication process handles:
//   - Tables: Deduplicated by table name
//   - Fields: Deduplicated by struct name + field name combination
//   - Indexes: Deduplicated by struct name + index name combination
//   - Enums: Deduplicated by enum name
//   - Embedded Fields: Deduplicated by struct name + embedded type name combination
//
// This method modifies the PackageParseResult in-place, replacing the original
// slices with deduplicated versions. The order of entities may change during
// this process, but dependency ordering is handled separately.
func deduplicate(r *Database) {
	// deduplicate tables by name
	tableMap := make(map[string]Table)
	for _, table := range r.Tables {
		tableMap[table.Name] = table
	}
	r.Tables = make([]Table, 0, len(tableMap))
	for _, table := range tableMap {
		r.Tables = append(r.Tables, table)
	}

	// deduplicate fields by struct name and field name (preserving order)
	fieldSeen := make(map[string]bool)
	var deduplicatedFields []Field
	for _, field := range r.Fields {
		key := field.StructName + "." + field.Name
		if !fieldSeen[key] {
			fieldSeen[key] = true
			deduplicatedFields = append(deduplicatedFields, field)
		}
	}
	r.Fields = deduplicatedFields

	// deduplicate indexes by struct name and index name (preserving order)
	indexSeen := make(map[string]bool)
	var deduplicatedIndexes []Index
	for _, index := range r.Indexes {
		key := index.StructName + "." + index.Name
		if !indexSeen[key] {
			indexSeen[key] = true
			deduplicatedIndexes = append(deduplicatedIndexes, index)
		}
	}
	r.Indexes = deduplicatedIndexes

	// deduplicate enums by name (preserving order)
	enumSeen := make(map[string]bool)
	var deduplicatedEnums []Enum
	for _, enum := range r.Enums {
		if !enumSeen[enum.Name] {
			enumSeen[enum.Name] = true
			deduplicatedEnums = append(deduplicatedEnums, enum)
		}
	}
	r.Enums = deduplicatedEnums

	// deduplicate embedded fields by struct name and embedded type name (preserving order)
	embeddedSeen := make(map[string]bool)
	var deduplicatedEmbedded []EmbeddedField
	for _, embedded := range r.EmbeddedFields {
		key := embedded.StructName + "." + embedded.EmbeddedTypeName
		if !embeddedSeen[key] {
			embeddedSeen[key] = true
			deduplicatedEmbedded = append(deduplicatedEmbedded, embedded)
		}
	}
	r.EmbeddedFields = deduplicatedEmbedded

	// deduplicate extensions by name
	extensionMap := make(map[string]Extension)
	for _, extension := range r.Extensions {
		extensionMap[extension.Name] = extension
	}
	r.Extensions = make([]Extension, 0, len(extensionMap))

	// Sort extension names for consistent ordering
	extensionNames := make([]string, 0, len(extensionMap))
	for name := range extensionMap {
		extensionNames = append(extensionNames, name)
	}
	sort.Strings(extensionNames)

	// Add extensions in sorted order
	for _, name := range extensionNames {
		r.Extensions = append(r.Extensions, extensionMap[name])
	}

	// deduplicate functions by name (preserving order)
	functionSeen := make(map[string]bool)
	var deduplicatedFunctions []Function
	for _, function := range r.Functions {
		if !functionSeen[function.Name] {
			functionSeen[function.Name] = true
			deduplicatedFunctions = append(deduplicatedFunctions, function)
		}
	}
	r.Functions = deduplicatedFunctions

	// deduplicate RLS policies by name (preserving order)
	rlsPolicySeen := make(map[string]bool)
	var deduplicatedPolicies []RLSPolicy
	for _, policy := range r.RLSPolicies {
		if !rlsPolicySeen[policy.Name] {
			rlsPolicySeen[policy.Name] = true
			deduplicatedPolicies = append(deduplicatedPolicies, policy)
		}
	}
	r.RLSPolicies = deduplicatedPolicies

	// deduplicate RLS enabled tables by table name (preserving order)
	rlsEnabledSeen := make(map[string]bool)
	var deduplicatedRLSTables []RLSEnabledTable
	for _, rlsTable := range r.RLSEnabledTables {
		if !rlsEnabledSeen[rlsTable.Table] {
			rlsEnabledSeen[rlsTable.Table] = true
			deduplicatedRLSTables = append(deduplicatedRLSTables, rlsTable)
		}
	}
	r.RLSEnabledTables = deduplicatedRLSTables
}
