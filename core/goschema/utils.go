package goschema

import (
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Global regex cache for function dependency analysis
var (
	regexCache = make(map[string]*regexp.Regexp)
	regexMutex sync.RWMutex
)

// getCachedRegex returns a cached regex pattern or creates and caches a new one
func getCachedRegex(functionName string) *regexp.Regexp {
	pattern := `\b` + regexp.QuoteMeta(functionName) + `\s*\(`

	regexMutex.RLock()
	if regex, exists := regexCache[pattern]; exists {
		regexMutex.RUnlock()
		return regex
	}
	regexMutex.RUnlock()

	regexMutex.Lock()
	defer regexMutex.Unlock()

	// Double-check in case another goroutine added it while we were waiting
	if regex, exists := regexCache[pattern]; exists {
		return regex
	}

	regex := regexp.MustCompile(pattern)
	regexCache[pattern] = regex
	return regex
}

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
					*queue = insertSortedString(*queue, tableName)
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
			if sortedTable.QualifiedName() == table.QualifiedName() {
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
		tableMap[table.QualifiedName()] = table
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
	sort.Strings(queue)

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
	initializeDependencyMaps(r)
	analyzeFieldForeignKeys(r)
	analyzeEmbeddedFieldRelations(r)
	buildFunctionDependencies(r)
}

// Finalize prepares a programmatically constructed Database for rendering.
//
// Parsers that do not go through ParseDir still need the same derived metadata
// as Go annotations: deduplicated declarations, dependency maps, self-referencing
// foreign keys, and dependency-ordered tables/functions.
func Finalize(r *Database) {
	if r.Dependencies == nil {
		r.Dependencies = make(map[string][]string)
	}
	if r.FunctionDependencies == nil {
		r.FunctionDependencies = make(map[string][]string)
	}
	if r.SelfReferencingForeignKeys == nil {
		r.SelfReferencingForeignKeys = make(map[string][]SelfReferencingFK)
	}

	Deduplicate(r)
	normalizeTableScopedNames(r)
	buildDependencyGraph(r)
	sortTablesByDependencies(r)
	sortFunctionsByDependencies(r)
}

func normalizeTableScopedNames(r *Database) {
	if r == nil {
		return
	}
	for i := range r.Constraints {
		constraint := &r.Constraints[i]
		table := resolveTableReference(r.Tables, constraint.StructName, constraint.Table)
		if table == nil {
			continue
		}
		constraint.Table = table.QualifiedName()
		if constraint.ForeignTable != "" {
			constraint.ForeignTable = resolveReferenceTableName(r.Tables, *table, constraint.ForeignTable)
		}
	}
	for i := range r.Indexes {
		index := &r.Indexes[i]
		if table := resolveTableReference(r.Tables, index.StructName, index.TableName); table != nil {
			index.TableName = table.QualifiedName()
		}
	}
	for i := range r.RLSPolicies {
		policy := &r.RLSPolicies[i]
		if table := resolveTableReference(r.Tables, policy.StructName, policy.Table); table != nil {
			policy.Table = table.QualifiedName()
		}
	}
	for i := range r.RLSEnabledTables {
		rlsEnabled := &r.RLSEnabledTables[i]
		if table := resolveTableReference(r.Tables, rlsEnabled.StructName, rlsEnabled.Table); table != nil {
			rlsEnabled.Table = table.QualifiedName()
		}
	}
	for i := range r.Grants {
		grant := &r.Grants[i]
		grant.Canonicalize()
		if grant.OnTable == "" {
			continue
		}
		if table := resolveTableReference(r.Tables, grant.StructName, grant.OnTable); table != nil {
			grant.OnTable = table.QualifiedName()
		}
	}
	for i := range r.Triggers {
		trigger := &r.Triggers[i]
		if table := resolveTableReference(r.Tables, trigger.StructName, trigger.Table); table != nil {
			trigger.Table = table.QualifiedName()
		}
	}
	// Views and MaterializedViews: no table-scoped normalization applied here.
	// Decision: unlike Triggers/Constraints/Grants/Indexes/RLS which reference a .Table,
	// Views/MaterializedViews declare a standalone .Name (which may include schema prefix
	// e.g. "public.foo" or be unqualified). They are not "table scoped" from a host table
	// struct in the same manner. If view names need struct-to-name resolution or schema
	// inference in future (e.g. schema-scoped Go files), extend this loop similarly.
	// Current behavior preserved for compatibility with YAML and existing parser paths.
	for i := range r.Views {
		_ = &r.Views[i] // name left as-parsed
	}
	for i := range r.MaterializedViews {
		_ = &r.MaterializedViews[i]
	}
}

func resolveTableReference(tables []Table, structName, tableName string) *Table {
	tableName = strings.TrimSpace(tableName)
	for i := range tables {
		table := &tables[i]
		if tableName == "" && table.StructName == structName {
			return table
		}
		if tableName != "" && table.StructName == structName && (table.Name == tableName || table.QualifiedName() == tableName) {
			return table
		}
	}
	if tableName == "" || strings.Contains(tableName, ".") {
		return nil
	}
	var match *Table
	for i := range tables {
		table := &tables[i]
		if table.Name != tableName {
			continue
		}
		if match != nil {
			return nil
		}
		match = table
	}
	return match
}

// initializeDependencyMaps initializes the dependency tracking maps
func initializeDependencyMaps(r *Database) {
	// Initialize dependencies map for all tables
	for _, table := range r.Tables {
		r.Dependencies[table.QualifiedName()] = []string{}
	}

	// Initialize self-referencing foreign keys tracking
	if r.SelfReferencingForeignKeys == nil {
		r.SelfReferencingForeignKeys = make(map[string][]SelfReferencingFK)
	}
}

// analyzeFieldForeignKeys analyzes foreign key relationships from regular fields
func analyzeFieldForeignKeys(r *Database) {
	for _, field := range r.Fields {
		if field.Foreign == "" {
			continue
		}

		refTable := strings.Split(field.Foreign, "(")[0]
		table := findTableByStructName(r.Tables, field.StructName)
		if table == nil {
			continue
		}

		processForeignKeyDependency(r, *table, refTable, SelfReferencingFK{
			FieldName:      field.Name,
			Foreign:        field.Foreign,
			ForeignKeyName: field.ForeignKeyName,
			OnDelete:       field.OnDelete,
			OnUpdate:       field.OnUpdate,
		})
	}
}

// analyzeEmbeddedFieldRelations analyzes foreign key relationships from embedded fields
func analyzeEmbeddedFieldRelations(r *Database) {
	for _, embedded := range r.EmbeddedFields {
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}

		refTable := strings.Split(embedded.Ref, "(")[0]
		table := findTableByStructName(r.Tables, embedded.StructName)
		if table == nil {
			continue
		}

		processForeignKeyDependency(r, *table, refTable, SelfReferencingFK{
			FieldName:      embedded.Field,
			Foreign:        embedded.Ref,
			ForeignKeyName: generateForeignKeyName(table.Name, embedded.Field),
			OnDelete:       embedded.OnDelete,
			OnUpdate:       embedded.OnUpdate,
		})
	}
}

// findTableByStructName finds a table by its struct name
func findTableByStructName(tables []Table, structName string) *Table {
	for _, table := range tables {
		if table.StructName == structName {
			return &table
		}
	}
	return nil
}

// processForeignKeyDependency processes a foreign key dependency, handling self-references appropriately
func processForeignKeyDependency(r *Database, table Table, refTable string, selfRefFK SelfReferencingFK) {
	tableName := table.QualifiedName()
	refTable = resolveReferenceTableName(r.Tables, table, refTable)
	if tableName == refTable {
		// Track self-referencing foreign key for deferred constraint creation
		r.SelfReferencingForeignKeys[tableName] = append(r.SelfReferencingForeignKeys[tableName], selfRefFK)
	} else if !slices.Contains(r.Dependencies[tableName], refTable) {
		// Add dependency: table depends on refTable (only for non-self-referencing FKs)
		r.Dependencies[tableName] = append(r.Dependencies[tableName], refTable)
	}
}

func resolveReferenceTableName(tables []Table, current Table, refTable string) string {
	if strings.Contains(refTable, ".") {
		return refTable
	}
	for _, table := range tables {
		if table.Schema == current.Schema && table.Name == refTable {
			return table.QualifiedName()
		}
	}
	var match string
	for _, table := range tables {
		if table.Name != refTable {
			continue
		}
		if match != "" {
			return refTable
		}
		match = table.QualifiedName()
	}
	if match != "" {
		return match
	}
	return refTable
}

// generateForeignKeyName generates a consistent foreign key constraint name
// following the convention: fk_{table_name}_{field_name}
func generateForeignKeyName(tableName, fieldName string) string {
	return "fk_" + strings.ToLower(tableName) + "_" + strings.ToLower(fieldName)
}

// processEmbeddedFields processes embedded fields and generates corresponding schema fields based on embedding modes.
//
// This function expands embedded struct fields into individual database fields according to their embedding mode.
// It's essential to call this BEFORE buildDependencyGraph() to ensure that foreign keys from embedded fields
// are properly included in the dependency analysis.
//
// Supported embedding modes:
//   - "inline": Expands embedded struct fields as individual table columns
//   - "json": Creates a single JSON/JSONB column for the embedded struct
//   - "relation": Creates a foreign key field linking to another table
//   - "skip": Completely ignores the embedded field
//
// Parameters:
//   - embeddedFields: Collection of embedded field definitions to process
//   - originalFields: Complete collection of schema fields from all parsed structs
//
// Returns:
//   - Combined slice of Field containing both original fields and generated fields from embedded processing
func processEmbeddedFields(embeddedFields []EmbeddedField, originalFields []Field) []Field {
	// Estimate capacity: original fields + estimated embedded fields
	// Each embedded field could potentially generate multiple fields
	estimatedEmbeddedFields := len(embeddedFields) * 2 // Conservative estimate
	estimatedCapacity := len(originalFields) + estimatedEmbeddedFields

	// Pre-allocate slice with estimated capacity for better performance
	allFields := make([]Field, len(originalFields), estimatedCapacity)
	copy(allFields, originalFields)

	// Process embedded fields for each struct
	structNames := UniqueStructNames(embeddedFields)
	for _, structName := range structNames {
		generatedFields := processEmbeddedFieldsForStruct(embeddedFields, originalFields, structName)
		allFields = append(allFields, generatedFields...)
	}

	return allFields
}

// UniqueStructNames extracts the distinct StructName values from the given
// embedded fields, sorted alphabetically so callers process embedded structs
// in a deterministic order (issue #59).
func UniqueStructNames(embeddedFields []EmbeddedField) []string {
	structNameMap := make(map[string]bool)
	for _, embedded := range embeddedFields {
		structNameMap[embedded.StructName] = true
	}
	return slices.Sorted(maps.Keys(structNameMap))
}

// processEmbeddedFieldsForStruct processes embedded fields for a specific struct and generates corresponding schema fields.
//
// This function implements the core logic for transforming embedded fields into database schema fields
// according to their specified embedding mode. It processes only embedded fields that belong to the
// specified structName.
//
// Parameters:
//   - embeddedFields: Collection of embedded field definitions to process
//   - allFields: Complete collection of schema fields from all parsed structs
//   - structName: Name of the target struct to process embedded fields for
//
// Returns:
//   - Slice of Field representing the generated database fields for the specified struct
func processEmbeddedFieldsForStruct(embeddedFields []EmbeddedField, allFields []Field, structName string) []Field {
	var generatedFields []Field

	// Process each embedded field definition
	for _, embedded := range embeddedFields {
		// Filter: only process embedded fields for the target struct
		if embedded.StructName != structName {
			continue
		}

		switch embedded.Mode {
		case "inline":
			// INLINE MODE: Expand embedded struct fields as individual table columns
			generatedFields = processEmbeddedInlineMode(generatedFields, embedded, allFields, embeddedFields, structName)
		case "json":
			// JSON MODE: Create a single JSON/JSONB column for the embedded struct
			generatedFields = processEmbeddedJSONMode(generatedFields, embedded, structName)
		case "relation":
			// RELATION MODE: Create a foreign key field linking to another table
			generatedFields = processEmbeddedRelationMode(generatedFields, embedded, structName)
		case "skip":
			// SKIP MODE: Completely ignore this embedded field
			continue
		default:
			// DEFAULT MODE: Fall back to inline behavior for unrecognized modes
			generatedFields = processEmbeddedInlineMode(generatedFields, embedded, allFields, embeddedFields, structName)
		}
	}

	return generatedFields
}

// processEmbeddedInlineMode handles inline mode embedded fields by expanding them as individual table columns.
// This function now supports recursive embedded field processing to handle nested embedded structs.
func processEmbeddedInlineMode(generatedFields []Field, embedded EmbeddedField, allFields []Field, allEmbeddedFields []EmbeddedField, structName string) []Field {
	// INLINE MODE: Expand embedded struct fields as individual table columns
	generatedFields = processEmbeddedInlineModeRecursive(generatedFields, embedded, allFields, allEmbeddedFields, structName)

	return generatedFields
}

// processEmbeddedInlineModeRecursive recursively processes embedded fields in inline mode.
// This handles nested embedded structs by recursively expanding embedded fields within embedded types.
func processEmbeddedInlineModeRecursive(generatedFields []Field, embedded EmbeddedField, allFields []Field, allEmbeddedFields []EmbeddedField, structName string) []Field {
	// Step 1: Add direct fields from the embedded type
	for _, field := range allFields {
		if field.StructName != embedded.EmbeddedTypeName {
			continue
		}
		// Clone the field and reassign to target struct
		newField := field
		newField.StructName = structName

		// Apply prefix to column name if specified
		if embedded.Prefix != "" {
			newField.Name = embedded.Prefix + field.Name
		}

		generatedFields = append(generatedFields, newField)
	}

	// Step 2: Recursively process embedded fields within the embedded type
	for _, nestedEmbedded := range allEmbeddedFields {
		if nestedEmbedded.StructName != embedded.EmbeddedTypeName {
			continue
		}

		// Only process inline mode embedded fields recursively
		if nestedEmbedded.Mode == "inline" {
			// Create a new embedded field with the target struct name and combined prefix
			recursiveEmbedded := nestedEmbedded
			recursiveEmbedded.StructName = structName

			// Combine prefixes: if the parent has a prefix, prepend it to the nested prefix
			if embedded.Prefix != "" {
				if recursiveEmbedded.Prefix != "" {
					recursiveEmbedded.Prefix = embedded.Prefix + recursiveEmbedded.Prefix
				} else {
					recursiveEmbedded.Prefix = embedded.Prefix
				}
			}

			// Recursively process the nested embedded field
			generatedFields = processEmbeddedInlineModeRecursive(generatedFields, recursiveEmbedded, allFields, allEmbeddedFields, structName)
		}
	}

	return generatedFields
}

// processEmbeddedJSONMode handles JSON mode embedded fields by creating a single JSON/JSONB column.
func processEmbeddedJSONMode(generatedFields []Field, embedded EmbeddedField, structName string) []Field {
	// JSON MODE: Serialize embedded struct into a single JSON/JSONB column
	columnName := embedded.Name
	if columnName == "" {
		// Auto-generate column name: "Meta" -> "meta_data"
		columnName = strings.ToLower(embedded.EmbeddedTypeName) + "_data"
	}

	columnType := embedded.Type
	if columnType == "" {
		columnType = "JSONB" // Default to PostgreSQL JSONB for best performance
	}

	// Create the JSON field
	generatedFields = append(generatedFields, Field{
		StructName: structName,
		FieldName:  embedded.EmbeddedTypeName,
		Name:       columnName,
		Type:       columnType,
		Nullable:   embedded.Nullable,
		Comment:    embedded.Comment,
		Overrides:  embedded.Overrides,
	})

	return generatedFields
}

// processEmbeddedRelationMode handles relation mode embedded fields by creating foreign key fields.
func processEmbeddedRelationMode(generatedFields []Field, embedded EmbeddedField, structName string) []Field {
	// RELATION MODE: Create a foreign key field linking to another table
	if embedded.Field == "" || embedded.Ref == "" {
		// Skip incomplete relation definitions - both field name and reference are required
		return generatedFields
	}

	// Intelligent type inference based on reference pattern
	refType := "INTEGER" // Default assumption: numeric primary key
	if strings.Contains(embedded.Ref, "VARCHAR") || strings.Contains(embedded.Ref, "TEXT") ||
		strings.Contains(strings.ToLower(embedded.Ref), "uuid") {
		// Reference suggests string-based key (likely UUID)
		refType = "VARCHAR(36)" // Standard UUID length
	}

	// Generate automatic foreign key constraint name following convention
	foreignKeyName := generateForeignKeyName(structName, embedded.Field)

	// Create platform-specific overrides for MySQL/MariaDB compatibility
	// MySQL/MariaDB use INT for SERIAL types, so foreign keys should also use INT
	overrides := make(map[string]map[string]string)
	if refType == "INTEGER" {
		overrides["mysql"] = map[string]string{"type": "INT"}
		overrides["mariadb"] = map[string]string{"type": "INT"}
	}

	// Create the foreign key field
	generatedFields = append(generatedFields, Field{
		StructName:     structName,
		FieldName:      embedded.EmbeddedTypeName,
		Name:           embedded.Field,    // e.g., "user_id"
		Type:           refType,           // INTEGER or VARCHAR(36)
		Nullable:       embedded.Nullable, // Can the relationship be optional?
		Foreign:        embedded.Ref,      // e.g., "users(id)"
		ForeignKeyName: foreignKeyName,    // e.g., "fk_posts_user_id"
		OnDelete:       embedded.OnDelete, // ON DELETE action (CASCADE, SET NULL, etc.) — keeps the walker/planner path in sync with fromschema (#117).
		OnUpdate:       embedded.OnUpdate,
		Comment:        embedded.Comment, // Documentation for the relationship
		Overrides:      overrides,        // Platform-specific type overrides
	})

	return generatedFields
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

	// Analyze each function's body for calls to other functions
	for _, function := range r.Functions {
		body := function.Body
		depMap := make(map[string]bool)

		// Look for function calls in the body using cached regexes
		for otherFunctionName := range functionNames {
			if otherFunctionName == function.Name {
				continue // Skip self-references
			}

			// Use cached regex to match function calls: function_name(
			// This matches the function name as a word, optional whitespace, then '('
			// This avoids false positives in comments or string literals
			re := getCachedRegex(otherFunctionName)
			if re.FindStringIndex(body) != nil {
				// Add dependency: current function depends on the called function
				depMap[otherFunctionName] = true
			}
		}

		// Convert depMap keys to a sorted slice and assign to FunctionDependencies
		r.FunctionDependencies[function.Name] = slices.Sorted(maps.Keys(depMap))
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
	sort.Strings(queue)
	return queue
}

// updateInDegreesAndQueue reduces in-degrees and updates the processing queue.
func updateInDegreesAndQueue(current string, dependencies map[string][]string, inDegree map[string]int, queue []string) []string {
	for functionName, deps := range dependencies {
		for _, dep := range deps {
			if dep == current {
				inDegree[functionName]--
				if inDegree[functionName] == 0 {
					queue = insertSortedString(queue, functionName)
				}
			}
		}
	}
	return queue
}

func insertSortedString(values []string, value string) []string {
	index, _ := slices.BinarySearch(values, value)
	values = append(values, "")
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
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

// Deduplicate removes duplicate entities that may be defined in multiple files.
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
//   - Views and materialized views: Deduplicated by name
//   - Triggers: Deduplicated by table name + trigger name combination
//   - Constraints: Deduplicated by table + constraint name
//   - Grants: Deduplicated by role + privileges + (table or schema) target
//   - Roles: Deduplicated by role name
//
// All 15 Database slice collections are now covered (previously only a subset
// of appended collections were deduplicated, and the five dropped by ParseFS
// were never reached). This prevents duplicate emits when objects are declared
// across files.
//
// This method modifies the Database in-place, replacing the original
// slices with deduplicated versions. The order of entities may change during
// this process, but dependency ordering is handled separately.
func Deduplicate(r *Database) {
	// Deduplicate tables by schema-qualified name - preserve order
	tableSeen := make(map[string]bool)
	var deduplicatedTables []Table
	for _, table := range r.Tables {
		if !tableSeen[table.QualifiedName()] {
			tableSeen[table.QualifiedName()] = true
			deduplicatedTables = append(deduplicatedTables, table)
		}
	}
	r.Tables = deduplicatedTables

	// Deduplicate fields by struct name and field name - preserve order
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

	// Deduplicate indexes by struct name and index name - preserve order
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

	// Deduplicate enums by name - preserve order
	enumSeen := make(map[string]bool)
	var deduplicatedEnums []Enum
	for _, enum := range r.Enums {
		if !enumSeen[enum.Name] {
			enumSeen[enum.Name] = true
			deduplicatedEnums = append(deduplicatedEnums, enum)
		}
	}
	r.Enums = deduplicatedEnums

	// Deduplicate embedded fields by struct name and embedded type name - preserve order
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

	// Deduplicate extensions by name
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

	// Deduplicate functions by name - preserve order
	functionSeen := make(map[string]bool)
	var deduplicatedFunctions []Function
	for _, function := range r.Functions {
		if !functionSeen[function.Name] {
			functionSeen[function.Name] = true
			deduplicatedFunctions = append(deduplicatedFunctions, function)
		}
	}
	r.Functions = deduplicatedFunctions

	deduplicateSchemaObjects(r)

	// Deduplicate RLS policies by name - preserve order
	rlsPolicySeen := make(map[string]bool)
	var deduplicatedRLSPolicies []RLSPolicy
	for _, policy := range r.RLSPolicies {
		if !rlsPolicySeen[policy.Name] {
			rlsPolicySeen[policy.Name] = true
			deduplicatedRLSPolicies = append(deduplicatedRLSPolicies, policy)
		}
	}
	r.RLSPolicies = deduplicatedRLSPolicies

	// Deduplicate RLS enabled tables by table name - preserve order
	rlsEnabledSeen := make(map[string]bool)
	var deduplicatedRLSEnabled []RLSEnabledTable
	for _, rlsTable := range r.RLSEnabledTables {
		if !rlsEnabledSeen[rlsTable.Table] {
			rlsEnabledSeen[rlsTable.Table] = true
			deduplicatedRLSEnabled = append(deduplicatedRLSEnabled, rlsTable)
		}
	}
	r.RLSEnabledTables = deduplicatedRLSEnabled
}

func deduplicateSchemaObjects(r *Database) {
	r.Views = deduplicateViews(r.Views)
	r.MaterializedViews = deduplicateMaterializedViews(r.MaterializedViews)
	r.Triggers = deduplicateTriggers(r.Triggers)
	r.Constraints = deduplicateConstraints(r.Constraints)
	r.Grants = deduplicateGrants(r.Grants)
	r.Roles = deduplicateRoles(r.Roles)
}

func deduplicateViews(views []View) []View {
	seen := make(map[string]bool)
	deduplicated := make([]View, 0, len(views))
	for _, view := range views {
		if !seen[view.Name] {
			seen[view.Name] = true
			deduplicated = append(deduplicated, view)
		}
	}
	return deduplicated
}

func deduplicateMaterializedViews(views []MaterializedView) []MaterializedView {
	seen := make(map[string]bool)
	deduplicated := make([]MaterializedView, 0, len(views))
	for _, view := range views {
		view.Canonicalize()
		if !seen[view.Name] {
			seen[view.Name] = true
			deduplicated = append(deduplicated, view)
		}
	}
	return deduplicated
}

func deduplicateTriggers(triggers []Trigger) []Trigger {
	seen := make(map[string]bool)
	deduplicated := make([]Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		trigger.Canonicalize()
		key := trigger.Table + "." + trigger.Name
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, trigger)
		}
	}
	return deduplicated
}

// deduplicateConstraints dedups table-level constraints by (StructName + name).
// Uses StructName (the declaring Go type) so dedup happens before normalizeTableScopedNames
// qualifies .Table (which may be empty when annotation omits `table=` and relies on struct assoc).
// This matches the pattern used for indexes (StructName + Name).
func deduplicateConstraints(constraints []Constraint) []Constraint {
	seen := make(map[string]bool)
	deduplicated := make([]Constraint, 0, len(constraints))
	for _, c := range constraints {
		key := c.StructName + "." + c.Name
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, c)
		}
	}
	return deduplicated
}

// deduplicateGrants dedups by role + privileges + target (table or schema).
// Uses Canonicalize for stable privilege list.
func deduplicateGrants(grants []Grant) []Grant {
	seen := make(map[string]bool)
	deduplicated := make([]Grant, 0, len(grants))
	for _, g := range grants {
		g.Canonicalize()
		privs := strings.Join(g.Privileges, ",")
		key := g.Role + "|" + privs + "|t:" + g.OnTable + "|s:" + g.OnSchema
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, g)
		}
	}
	return deduplicated
}

// deduplicateRoles dedups roles by name (roles are global per DB).
func deduplicateRoles(roles []Role) []Role {
	seen := make(map[string]bool)
	deduplicated := make([]Role, 0, len(roles))
	for _, r := range roles {
		if !seen[r.Name] {
			seen[r.Name] = true
			deduplicated = append(deduplicated, r)
		}
	}
	return deduplicated
}
