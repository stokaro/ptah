package goschema

import (
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/tableref"
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

// sortTablesByDependencies performs topological sort to order tables by their dependencies.
//
// Strongly connected components are collapsed into a condensation graph before
// sorting. This keeps every cycle contiguous while still honoring dependencies
// into and out of the cycle. Component members and ready components are ordered
// by qualified table name so source declaration order cannot affect the result.
func sortTablesByDependencies(r *Database) {
	tableMap := make(map[string]Table, len(r.Tables))
	for _, table := range r.Tables {
		tableMap[table.QualifiedName()] = table
	}
	adjacency := internalTableDependencies(tableMap, r.Dependencies)
	components := stronglyConnectedTableComponents(adjacency)
	componentDependencies, componentDependents := tableComponentEdges(components, adjacency)
	r.Tables = orderTableComponents(tableMap, components, componentDependencies, componentDependents)
}

func internalTableDependencies(tableMap map[string]Table, dependencies map[string][]string) map[string][]string {
	adjacency := make(map[string][]string, len(tableMap))
	for tableName := range tableMap {
		internal := make(map[string]struct{})
		for _, dependency := range dependencies[tableName] {
			if _, exists := tableMap[dependency]; exists {
				internal[dependency] = struct{}{}
			}
		}
		adjacency[tableName] = slices.Sorted(maps.Keys(internal))
	}
	return adjacency
}

func stronglyConnectedTableComponents(adjacency map[string][]string) [][]string {
	visited := make(map[string]bool, len(adjacency))
	postorder := make([]string, 0, len(adjacency))
	for _, tableName := range slices.Sorted(maps.Keys(adjacency)) {
		appendTableDependencyPostorder(tableName, adjacency, visited, &postorder)
	}

	clear(visited)
	reversed := reverseTableDependencies(adjacency)
	components := make([][]string, 0, len(adjacency))
	for _, tableName := range slices.Backward(postorder) {
		component := collectTableDependencyComponent(tableName, reversed, visited)
		if len(component) == 0 {
			continue
		}
		sort.Strings(component)
		components = append(components, component)
	}
	return components
}

func appendTableDependencyPostorder(tableName string, adjacency map[string][]string, visited map[string]bool, postorder *[]string) {
	if visited[tableName] {
		return
	}
	visited[tableName] = true
	for _, dependency := range adjacency[tableName] {
		appendTableDependencyPostorder(dependency, adjacency, visited, postorder)
	}
	*postorder = append(*postorder, tableName)
}

func reverseTableDependencies(adjacency map[string][]string) map[string][]string {
	reversed := make(map[string][]string, len(adjacency))
	for tableName := range adjacency {
		reversed[tableName] = nil
	}
	for tableName, dependencies := range adjacency {
		for _, dependency := range dependencies {
			reversed[dependency] = append(reversed[dependency], tableName)
		}
	}
	for tableName := range reversed {
		sort.Strings(reversed[tableName])
	}
	return reversed
}

func collectTableDependencyComponent(tableName string, adjacency map[string][]string, visited map[string]bool) []string {
	if visited[tableName] {
		return nil
	}
	visited[tableName] = true
	component := []string{tableName}
	for _, dependency := range adjacency[tableName] {
		component = append(component, collectTableDependencyComponent(dependency, adjacency, visited)...)
	}
	return component
}

func tableComponentEdges(
	components [][]string,
	adjacency map[string][]string,
) (componentDependencies, componentDependents []map[int]struct{}) {
	componentByTable := make(map[string]int, len(adjacency))
	componentDependencies = make([]map[int]struct{}, len(components))
	componentDependents = make([]map[int]struct{}, len(components))
	for componentIndex, component := range components {
		componentDependencies[componentIndex] = make(map[int]struct{})
		componentDependents[componentIndex] = make(map[int]struct{})
		for _, tableName := range component {
			componentByTable[tableName] = componentIndex
		}
	}

	for tableName, dependencies := range adjacency {
		componentIndex := componentByTable[tableName]
		for _, dependency := range dependencies {
			dependencyComponent := componentByTable[dependency]
			if componentIndex == dependencyComponent {
				continue
			}
			componentDependencies[componentIndex][dependencyComponent] = struct{}{}
			componentDependents[dependencyComponent][componentIndex] = struct{}{}
		}
	}
	return componentDependencies, componentDependents
}

func orderTableComponents(
	tableMap map[string]Table,
	components [][]string,
	componentDependencies []map[int]struct{},
	componentDependents []map[int]struct{},
) []Table {
	remainingDependencies := make([]int, len(components))
	queue := make([]int, 0, len(components))
	for componentIndex, dependencies := range componentDependencies {
		remainingDependencies[componentIndex] = len(dependencies)
		if len(dependencies) == 0 {
			queue = insertSortedTableComponent(queue, componentIndex, components)
		}
	}

	ordered := make([]Table, 0, len(tableMap))
	for len(queue) > 0 {
		componentIndex := queue[0]
		queue = queue[1:]
		for _, tableName := range components[componentIndex] {
			ordered = append(ordered, tableMap[tableName])
		}
		for dependentComponent := range componentDependents[componentIndex] {
			remainingDependencies[dependentComponent]--
			if remainingDependencies[dependentComponent] == 0 {
				queue = insertSortedTableComponent(queue, dependentComponent, components)
			}
		}
	}
	return ordered
}

func insertSortedTableComponent(queue []int, componentIndex int, components [][]string) []int {
	componentName := components[componentIndex][0]
	position := sort.Search(len(queue), func(i int) bool {
		return components[queue[i]][0] >= componentName
	})
	return slices.Insert(queue, position, componentIndex)
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
//  4. Scans table-level FOREIGN KEY constraints
//  5. Extracts referenced table names from foreign key specifications
//  6. Maps each table to its list of dependencies
//
// Foreign key format examples:
//   - "users(id)" -> depends on "users" table
//   - "categories(uuid)" -> depends on "categories" table
//
// The resulting dependency graph is stored in the Dependencies field and used by
// sortTablesByDependencies() to perform topological sorting.
func buildDependencyGraph(r *Database) {
	resetDependencyMaps(r)
	analyzeFieldForeignKeys(r)
	analyzeEmbeddedFieldRelations(r)
	analyzeConstraintForeignKeys(r)
	buildFunctionDependencies(r)
}

// Finalize prepares a programmatically constructed Database for rendering.
//
// Parsers that do not go through ParseDir still need the same derived metadata
// as Go annotations: deduplicated declarations, dependency maps, self-referencing
// foreign keys, and dependency-ordered tables/functions.
func Finalize(r *Database) {
	restoreCompositeHelperDefinitions(r)
	r.Fields = processEmbeddedFields(r.EmbeddedFields, r.Fields)
	normalizeTableScopedNames(r)
	Deduplicate(r)
	stashCompositeHelperDefinitions(r)
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
	// Unlike Triggers/Constraints/Grants/Indexes/RLS, which reference a .Table,
	// Views/MaterializedViews declare a standalone .Name (which may include a
	// schema prefix, e.g. "public.foo", or be unqualified). They are not table
	// scoped from a host table struct in the same manner. Current behavior is
	// preserved for compatibility with YAML and existing parser paths.
}

func resolveTableReference(tables []Table, structName, tableName string) *Table {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		for i := range tables {
			if tables[i].StructName == structName {
				return &tables[i]
			}
		}
		return nil
	}
	for i := range tables {
		if tables[i].QualifiedName() == tableName {
			return &tables[i]
		}
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return nil
	}
	for i := range tables {
		if tables[i].StructName == structName && tables[i].Name == ref.Name {
			return &tables[i]
		}
	}
	var match *Table
	for i := range tables {
		table := &tables[i]
		if table.Name != ref.Name {
			continue
		}
		if match != nil {
			return nil
		}
		match = table
	}
	return match
}

type indexTableRefKey struct {
	structName string
	tableName  string
}

type indexTableMatch struct {
	qualifiedName string
	ambiguous     bool
}

type indexTableResolver struct {
	empty             bool
	firstByStruct     map[string]indexTableMatch
	byStructReference map[indexTableRefKey]indexTableMatch
	byPlainName       map[string]indexTableMatch
	byQualifiedName   map[string]indexTableMatch
}

func newIndexTableResolver(relations []indexRelation) *indexTableResolver {
	resolver := &indexTableResolver{
		empty:             len(relations) == 0,
		firstByStruct:     make(map[string]indexTableMatch, len(relations)),
		byStructReference: make(map[indexTableRefKey]indexTableMatch, len(relations)*2),
		byPlainName:       make(map[string]indexTableMatch, len(relations)),
		byQualifiedName:   make(map[string]indexTableMatch, len(relations)),
	}
	for _, relation := range relations {
		addIndexTableMatch(resolver.firstByStruct, relation.structName, relation.qualifiedName)
		resolver.addStructReference(relation.structName, relation.qualifiedName, relation.qualifiedName)
		addIndexTableMatch(resolver.byPlainName, relation.name, relation.qualifiedName)
		addIndexTableMatch(resolver.byQualifiedName, relation.qualifiedName, relation.qualifiedName)
	}
	return resolver
}

// indexRelation is something an index can belong to.
//
// Two kinds qualify, and only two. A table is the obvious one. A materialized
// view is the other: PostgreSQL accepts CREATE INDEX on one, and a UNIQUE index
// on one is the precondition REFRESH MATERIALIZED VIEW CONCURRENTLY checks --
// measured on 18.4, where the concurrent form without it answers `cannot
// refresh materialized view "public.mv" concurrently` and names the fix in its
// HINT. A plain view has no storage to index (stokaro/ptah#1725).
type indexRelation struct {
	structName    string
	name          string
	qualifiedName string
}

// indexRelations collects the relations an index may name.
//
// A struct declaring both a table and a materialized view makes its entry
// AMBIGUOUS rather than picking one, the same way two tables sharing a struct
// name already do -- addIndexTableMatch marks the second entry and resolve
// answers "" for it. That is what keeps such an index on the table path: the
// table-only resolver is unambiguous there, and the split below treats
// "resolved as a table" as the deciding fact.
func indexRelations(tables []Table, views []MaterializedView) []indexRelation {
	relations := make([]indexRelation, 0, len(tables)+len(views))
	for _, table := range tables {
		relations = append(relations, indexRelation{
			structName:    table.StructName,
			name:          table.Name,
			qualifiedName: table.QualifiedName(),
		})
	}
	for _, view := range views {
		relations = append(relations, indexRelation{
			structName:    view.StructName,
			name:          view.Name,
			qualifiedName: view.Name,
		})
	}
	return relations
}

func (r *indexTableResolver) addStructReference(structName, tableName, qualifiedName string) {
	key := indexTableRefKey{structName: structName, tableName: tableName}
	match, exists := r.byStructReference[key]
	if !exists {
		r.byStructReference[key] = indexTableMatch{qualifiedName: qualifiedName}
		return
	}
	if match.qualifiedName == qualifiedName {
		return
	}
	match.ambiguous = true
	r.byStructReference[key] = match
}

func addIndexTableMatch(matches map[string]indexTableMatch, key, qualifiedName string) {
	match, exists := matches[key]
	if !exists {
		matches[key] = indexTableMatch{qualifiedName: qualifiedName}
		return
	}
	match.ambiguous = true
	matches[key] = match
}

func (r *indexTableResolver) resolve(index Index) string {
	tableName := strings.TrimSpace(index.TableName)
	if tableName == "" {
		match := r.firstByStruct[index.StructName]
		if match.ambiguous {
			return ""
		}
		return match.qualifiedName
	}
	key := indexTableRefKey{structName: index.StructName, tableName: tableName}
	if match, exists := r.byStructReference[key]; exists {
		if match.ambiguous {
			return ""
		}
		return match.qualifiedName
	}
	if r.empty {
		return tableName
	}
	if match, exists := r.byQualifiedName[tableName]; exists {
		return match.qualifiedName
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return ""
	}
	match := r.byPlainName[ref.Name]
	if match.ambiguous {
		return ""
	}
	return match.qualifiedName
}

// ResolveIndexTableNames resolves every index owner in one indexed pass.
// Explicit table references and struct-based associations are matched against
// tables. An empty result entry means the owner is missing or ambiguous.
// When tables is empty, an explicit owner is retained for indexes-only inputs.
//
// It answers for tables alone. A caller holding the whole description should
// use [ResolveIndexOwners], which also resolves an index declared on a
// materialized view; this one is kept for the call sites whose question really
// is about tables -- whether a table column is backed by a unique index, for
// instance, where a matview's index is not an answer.
func ResolveIndexTableNames(indexes []Index, tables []Table) []string {
	return resolveIndexOwners(indexes, indexRelations(tables, nil))
}

// ResolveIndexOwners resolves every index owner against the relations an index
// can belong to: tables and materialized views.
//
// An index whose struct declares a materialized view resolved to nothing
// before, and the two surfaces then disagreed about what that meant. `schema
// render` fell back to the Go STRUCT name and emitted `CREATE UNIQUE INDEX ...
// ON "MV"`, which PostgreSQL answers `relation "MV" does not exist`; `schema
// apply` refused with a sentence naming a position in a slice rather than the
// index or the view (stokaro/ptah#1725).
func ResolveIndexOwners(indexes []Index, tables []Table, views []MaterializedView) []string {
	return resolveIndexOwners(indexes, indexRelations(tables, views))
}

func resolveIndexOwners(indexes []Index, relations []indexRelation) []string {
	resolver := newIndexTableResolver(relations)
	owners := make([]string, len(indexes))
	for position, index := range indexes {
		owners[position] = resolver.resolve(index)
	}
	return owners
}

// resetDependencyMaps discards derived state before rebuilding it from the
// current schema declarations. Finalization is intentionally idempotent.
func resetDependencyMaps(r *Database) {
	r.Dependencies = make(map[string][]string, len(r.Tables))
	r.FunctionDependencies = make(map[string][]string, len(r.Functions))
	r.SelfReferencingForeignKeys = make(map[string][]SelfReferencingFK)

	for _, table := range r.Tables {
		r.Dependencies[table.QualifiedName()] = []string{}
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

		processForeignKeyDependency(r, *table, refTable, &SelfReferencingFK{
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

		processForeignKeyDependency(r, *table, refTable, &SelfReferencingFK{
			FieldName:      embedded.Field,
			Foreign:        embedded.Ref,
			ForeignKeyName: generateForeignKeyName(table.Name, embedded.Field),
			OnDelete:       embedded.OnDelete,
			OnUpdate:       embedded.OnUpdate,
		})
	}
}

func analyzeConstraintForeignKeys(r *Database) {
	for _, constraint := range r.Constraints {
		if constraint.ForeignTable == "" || strings.ToUpper(constraint.Type) != "FOREIGN KEY" {
			continue
		}
		table := resolveTableReference(r.Tables, constraint.StructName, constraint.Table)
		if table == nil {
			continue
		}
		// Table-level constraints retain their structured local and referenced
		// column lists. They must not be projected into SelfReferencingFK, whose
		// field-level shape is intentionally single-column and lossy.
		processForeignKeyDependency(r, *table, constraint.ForeignTable, nil)
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
func processForeignKeyDependency(r *Database, table Table, refTable string, selfRefFK *SelfReferencingFK) {
	tableName := table.QualifiedName()
	refTable = resolveReferenceTableName(r.Tables, table, refTable)
	if tableName == refTable {
		if selfRefFK != nil && !containsSelfReferencingForeignKey(r.SelfReferencingForeignKeys[tableName], *selfRefFK) {
			r.SelfReferencingForeignKeys[tableName] = append(r.SelfReferencingForeignKeys[tableName], *selfRefFK)
		}
	} else if !slices.Contains(r.Dependencies[tableName], refTable) {
		// Add dependency: table depends on refTable (only for non-self-referencing FKs)
		r.Dependencies[tableName] = append(r.Dependencies[tableName], refTable)
	}
}

func containsSelfReferencingForeignKey(foreignKeys []SelfReferencingFK, candidate SelfReferencingFK) bool {
	for _, foreignKey := range foreignKeys {
		if foreignKey.FieldName == candidate.FieldName &&
			foreignKey.Foreign == candidate.Foreign &&
			foreignKey.OnDelete == candidate.OnDelete &&
			foreignKey.OnUpdate == candidate.OnUpdate {
			return true
		}
	}
	return false
}

func resolveReferenceTableName(tables []Table, current Table, refTable string) string {
	ref, ok := tableref.Parse(refTable)
	if !ok {
		return refTable
	}
	if ref.Qualified {
		return QualifyTableName(ref.Schema, ref.Name)
	}
	for _, table := range tables {
		if table.Schema == current.Schema && table.Name == ref.Name {
			return table.QualifiedName()
		}
	}
	var match string
	for _, table := range tables {
		if table.Name != ref.Name {
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
	sourceFields := make([]Field, 0, len(originalFields))
	for _, field := range originalFields {
		if !field.GeneratedFromEmbedded {
			sourceFields = append(sourceFields, field)
		}
	}

	// Estimate capacity: original fields + estimated embedded fields
	// Each embedded field could potentially generate multiple fields
	estimatedEmbeddedFields := len(embeddedFields) * 2 // Conservative estimate
	estimatedCapacity := len(sourceFields) + estimatedEmbeddedFields

	// Pre-allocate slice with estimated capacity for better performance
	allFields := make([]Field, len(sourceFields), estimatedCapacity)
	copy(allFields, sourceFields)

	// Process embedded fields for each struct
	structNames := UniqueStructNames(embeddedFields)
	for _, structName := range structNames {
		generatedFields := processEmbeddedFieldsForStruct(embeddedFields, sourceFields, structName)
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
	generatedFields = processEmbeddedInlineModeRecursive(
		generatedFields,
		embedded,
		allFields,
		allEmbeddedFields,
		structName,
		make(map[string]bool),
	)

	return generatedFields
}

// processEmbeddedInlineModeRecursive recursively processes embedded fields in inline mode.
// This handles nested embedded structs by recursively expanding embedded fields within embedded types.
func processEmbeddedInlineModeRecursive(
	generatedFields []Field,
	embedded EmbeddedField,
	allFields []Field,
	allEmbeddedFields []EmbeddedField,
	structName string,
	activeTypes map[string]bool,
) []Field {
	if embedded.EmbeddedTypeName == "" || activeTypes[embedded.EmbeddedTypeName] {
		return generatedFields
	}
	activeTypes[embedded.EmbeddedTypeName] = true
	defer delete(activeTypes, embedded.EmbeddedTypeName)

	// Step 1: Add direct fields from the embedded type
	for _, field := range allFields {
		if field.StructName != embedded.EmbeddedTypeName {
			continue
		}
		// Clone the field and reassign to target struct
		newField := field
		newField.StructName = structName
		newField.Overrides = mergePlatformOverrides(field.Overrides, embedded.Overrides)
		newField.GeneratedFromEmbedded = true

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

		recursiveEmbedded := nestedEmbedded
		recursiveEmbedded.StructName = structName
		recursiveEmbedded.Overrides = mergePlatformOverrides(
			nestedEmbedded.Overrides,
			embedded.Overrides,
		)
		recursiveEmbedded.Prefix = embedded.Prefix + nestedEmbedded.Prefix

		switch nestedEmbedded.Mode {
		case "json":
			generatedFields = processEmbeddedJSONMode(generatedFields, recursiveEmbedded, structName)
		case "relation":
			generatedFields = processEmbeddedRelationMode(generatedFields, recursiveEmbedded, structName)
		case "skip":
			continue
		default:
			generatedFields = processEmbeddedInlineModeRecursive(
				generatedFields,
				recursiveEmbedded,
				allFields,
				allEmbeddedFields,
				structName,
				activeTypes,
			)
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
	columnName = embedded.Prefix + columnName

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
		Overrides:  mergePlatformOverrides(nil, embedded.Overrides),

		GeneratedFromEmbedded: true,
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

	// An explicit relation type is authoritative. When it is omitted, use the
	// conservative numeric or string heuristic documented for relation fields.
	refType := strings.TrimSpace(embedded.Type)
	if refType == "" {
		refType = "INTEGER"
		if strings.Contains(embedded.Ref, "VARCHAR") || strings.Contains(embedded.Ref, "TEXT") ||
			strings.Contains(strings.ToLower(embedded.Ref), "uuid") {
			refType = "VARCHAR(36)"
		}
	}

	fieldName := embedded.Prefix + embedded.Field

	// Generate automatic foreign key constraint name following convention
	foreignKeyName := generateForeignKeyName(structName, fieldName)

	// Create platform-specific overrides for MySQL/MariaDB compatibility
	// MySQL/MariaDB use INT for SERIAL types, so foreign keys should also use INT
	overrides := make(map[string]map[string]string)
	if refType == "INTEGER" {
		overrides["mysql"] = map[string]string{"type": "INT"}
		overrides["mariadb"] = map[string]string{"type": "INT"}
	}
	overrides = mergePlatformOverrides(overrides, embedded.Overrides)

	// Create the foreign key field
	generatedFields = append(generatedFields, Field{
		StructName:     structName,
		FieldName:      embedded.EmbeddedTypeName,
		Name:           fieldName,         // e.g., "user_id"
		Type:           refType,           // INTEGER or VARCHAR(36)
		Nullable:       embedded.Nullable, // Can the relationship be optional?
		Foreign:        embedded.Ref,      // e.g., "users(id)"
		ForeignKeyName: foreignKeyName,    // e.g., "fk_posts_user_id"
		OnDelete:       embedded.OnDelete, // ON DELETE action (CASCADE, SET NULL, etc.) — keeps the walker/planner path in sync with fromschema (#117).
		OnUpdate:       embedded.OnUpdate,
		Comment:        embedded.Comment, // Documentation for the relationship
		Overrides:      overrides,        // Platform-specific type overrides

		GeneratedFromEmbedded: true,
	})

	return generatedFields
}

func mergePlatformOverrides(
	base map[string]map[string]string,
	explicit map[string]map[string]string,
) map[string]map[string]string {
	if len(base) == 0 && len(explicit) == 0 {
		return nil
	}
	result := make(map[string]map[string]string, len(base)+len(explicit))
	for dialect, values := range base {
		result[dialect] = maps.Clone(values)
	}
	for dialect, values := range explicit {
		if result[dialect] == nil {
			result[dialect] = make(map[string]string)
		}
		maps.Copy(result[dialect], values)
	}
	return result
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
//   - Indexes: Deduplicated by resolved table + index name combination
//   - Enums: Deduplicated by schema-qualified enum name
//   - Embedded Fields: Deduplicated by struct name + embedded type name combination
//   - Views and materialized views: Deduplicated by name
//   - Triggers: Deduplicated by table name + trigger name combination
//   - Constraints: Deduplicated by explicit table + name, or declaring struct + name when table is omitted
//   - Grants: Deduplicated by role + privileges + grant option + (table or schema) target
//   - Roles: Deduplicated by role name
//   - Schemas: Deduplicated by schema name
//   - RLS policies: Deduplicated by resolved table + policy name combination
//
// Composite finalization additionally deduplicates sequences, domains,
// composite types, ranges, and exact managed-data declarations. Grants retain
// their full role/privilege/target identity, while distinct managed-data files
// targeting the same table remain separate. This prevents duplicate emits when
// objects are declared across files or composite sources.
//
// This method modifies the Database in-place, replacing the original
// slices with deduplicated versions. The order of entities may change during
// this process, but dependency ordering is handled separately.
func Deduplicate(r *Database) {
	deduplicateDatabase(r, structDeduplicationScope)
}

func deduplicateComposite(r *Database) {
	deduplicateDatabase(r, compositeDeduplicationScope)
	r.Sequences = deduplicateNamedDefinitions(r.Sequences, func(sequence Sequence) string {
		return sequence.QualifiedName()
	})
	r.Domains = deduplicateNamedDefinitions(r.Domains, func(domain Domain) string {
		return domain.QualifiedName()
	})
	r.CompositeTypes = deduplicateNamedDefinitions(r.CompositeTypes, func(composite CompositeType) string {
		return composite.QualifiedName()
	})
	r.Ranges = deduplicateNamedDefinitions(r.Ranges, func(rangeType Range) string {
		return rangeType.QualifiedName()
	})
	r.ManagedData = deduplicateNamedDefinitions(r.ManagedData, managedDataDefinitionIdentity)
}

type deduplicationScope func(tableScopeResolver, string, string) string

func structDeduplicationScope(_ tableScopeResolver, structName, _ string) string {
	return structName
}

func compositeDeduplicationScope(resolver tableScopeResolver, structName, tableName string) string {
	return resolver.resolve(structName, tableName)
}

func deduplicateDatabase(
	r *Database,
	resolveScope deduplicationScope,
) {
	resolver := newTableScopeResolver(r.Tables)
	r.Schemas = deduplicateSchemas(r.Schemas)
	r.Tables = deduplicateNamedDefinitions(r.Tables, func(table Table) string {
		return table.QualifiedName()
	})
	r.Fields = deduplicateFields(r.Fields, resolver, resolveScope)
	r.Indexes = deduplicateIndexes(r.Indexes, r.Tables)
	// Qualified, not bare. A PostgreSQL enum lives in a schema, and public.mood
	// and other.mood are two types: the bare key folded them into one and the
	// second disappeared with no diagnostic, so `schema inspect` of a realm
	// holding both described a database that does not exist (stokaro/ptah#1360).
	// An enum with no schema still keys on its bare name, which is every enum
	// the Go-annotation path declares, so nothing that path folds today stops
	// folding.
	r.Enums = deduplicateNamedDefinitions(r.Enums, func(enum Enum) string {
		return enum.QualifiedName()
	})
	r.EmbeddedFields = deduplicateEmbeddedFields(r.EmbeddedFields, resolver, resolveScope)
	r.Extensions = deduplicateExtensions(r.Extensions)
	r.Functions = deduplicateNamedDefinitions(r.Functions, func(function Function) string {
		return function.Name
	})

	deduplicateSchemaObjects(r)
	rlsResolver := newRLSTableResolver(r.Tables, resolver)
	bindRLSTables(r, rlsResolver)
	r.RLSPolicies = deduplicateRLSPolicies(r.RLSPolicies, rlsResolver)
	r.RLSEnabledTables = deduplicateNamedDefinitions(r.RLSEnabledTables, func(table RLSEnabledTable) string {
		return table.Table
	})
}

func deduplicateFields(
	fields []Field,
	resolver tableScopeResolver,
	resolveScope deduplicationScope,
) []Field {
	return deduplicateNamedDefinitions(fields, func(field Field) string {
		scope := resolveScope(resolver, field.StructName, "")
		return scope + "." + field.Name
	})
}

func deduplicateEmbeddedFields(
	fields []EmbeddedField,
	resolver tableScopeResolver,
	resolveScope deduplicationScope,
) []EmbeddedField {
	return deduplicateNamedDefinitions(fields, func(field EmbeddedField) string {
		scope := resolveScope(resolver, field.StructName, "")
		return scope + "." + field.EmbeddedTypeName
	})
}

func deduplicateExtensions(extensions []Extension) []Extension {
	byName := make(map[string]Extension, len(extensions))
	for _, extension := range extensions {
		byName[extension.Name] = extension
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	deduplicated := make([]Extension, 0, len(names))
	for _, name := range names {
		deduplicated = append(deduplicated, byName[name])
	}
	return deduplicated
}

// rlsTableSemantics are the identifier rules the owning table of a row-level
// security declaration is folded with. Row-level security is a
// PostgreSQL-family construct, so the default schema an unqualified table
// lands in is `public`.
var rlsTableSemantics = identifier.ForDialect(platform.Postgres)

// rlsTableResolver answers one question: which declared table does this
// row-level security declaration name?
//
// One table has more than one spelling, and PostgreSQL reaches it through all
// of them. A table declared as `orders` is named by `orders`, by
// `public.orders`, and by `ORDERS`, because an unquoted identifier folds to
// lower case. Measured on PostgreSQL 17.10, `CREATE POLICY p ON orders`
// followed by either `CREATE POLICY p ON public.orders` or
// `CREATE POLICY p ON ORDERS` exits 3 with `policy "p" for table "orders"
// already exists`: one policy declared twice, not two policies.
//
// Answering that question in one place is the point. Keying deduplication by
// the string each declaration happened to use kept both, and `ptah schema
// render` then emitted a pair of CREATE POLICY statements the database rejects
// (stokaro/ptah#1276).
//
// Resolution never invents a name. It maps a reference onto a table that is
// declared, or leaves the reference alone:
//
//   - the exact spelling wins, through [tableScopeResolver.resolve];
//   - failing that, the table identity wins, which is what folds
//     `public.orders` onto a table declared without a schema.
//
// There is deliberately no case fold here, and that is the whole point of this
// comment. `ORDERS` and `"ORDERS"` are two different instructions to
// PostgreSQL -- the first names `orders`, the second names `ORDERS` -- and by
// the time a declaration reaches this resolver the two are the same string.
// Folding here therefore had to be wrong for one of two inputs it cannot tell
// apart, and being wrong for the quoted one relocated an access-control
// declaration onto a relation the author did not name (stokaro/ptah#1311).
// Measured on PostgreSQL 17.10 against a database holding only `orders`,
// `CREATE POLICY p ON "ORDERS"` exits 1 with `relation "ORDERS" does not
// exist`, while the folded render `CREATE POLICY "p" ON "orders"` exits 0 and
// leaves a pg_policy row on `public.orders`.
//
// Quoting is resolved where it still exists instead. The SQL frontend folds
// each component of a row-level security table reference as PostgreSQL does --
// unquoted components lose their case, quoted components keep it -- so a
// reference arrives here already naming its relation. Sources with no quoting
// syntax at all (Go annotations, YAML, HCL, a hand-built [Database]) are
// case-preserving by construction, because Ptah quotes every identifier it
// renders; for them the declaration is already the relation and there is
// nothing to fold. A reference that matches no declared table keeps its own
// spelling, so the render reproduces the database's own answer rather than
// quietly succeeding somewhere else.
type rlsTableResolver struct {
	scopes     tableScopeResolver
	byIdentity map[string]string
}

func newRLSTableResolver(tables []Table, scopes tableScopeResolver) rlsTableResolver {
	resolver := rlsTableResolver{
		scopes:     scopes,
		byIdentity: make(map[string]string, len(tables)),
	}
	for _, table := range tables {
		qualifiedName := table.QualifiedName()
		identity := rlsTableSemantics.QualifiedTableIdentityKey(qualifiedName)
		addTableScope(resolver.byIdentity, identity, qualifiedName)
	}
	return resolver
}

func (r rlsTableResolver) resolve(structName, table string) string {
	scoped := r.scopes.resolve(structName, table)
	identity := rlsTableSemantics.QualifiedTableIdentityKey(scoped)
	if declared := resolvedTableScope(r.byIdentity, identity, ""); declared != "" {
		return declared
	}
	return scoped
}

// bindRLSTables rewrites the table every row-level security declaration names
// onto the declared table it reaches, so the rest of the pipeline sees one
// spelling.
//
// Deduplicating on the resolved table is not enough on its own, because
// deduplication keeps the first declaration and the first one may be the
// variant spelling. On a table declared as `orders`, a schema whose first
// policy says `ON ORDERS` rendered `CREATE POLICY "p" ON "ORDERS"` -- a table
// nothing declared -- and PostgreSQL answered `relation "ORDERS" does not
// exist`. That predates the table-scoped key and it is what "make every
// spelling reach one answer" has to mean for a renderer that quotes what it is
// given.
//
// A declaration whose table matches nothing declared keeps its spelling: it is
// not this function's business to guess at a table it cannot see. "Matches" is
// [rlsTableResolver.resolve]'s answer, and the case fold inside it only runs
// downwards, so a file declaring `"ORDERS"` and naming `orders` does not match:
// the render keeps `orders`, and PostgreSQL answers `relation "orders" does not
// exist` exactly as it answers the source file.
func bindRLSTables(r *Database, resolver rlsTableResolver) {
	for index := range r.RLSPolicies {
		policy := &r.RLSPolicies[index]
		policy.Table = boundRLSTable(resolver, policy.StructName, policy.Table)
	}
	for index := range r.RLSEnabledTables {
		enabled := &r.RLSEnabledTables[index]
		enabled.Table = boundRLSTable(resolver, enabled.StructName, enabled.Table)
	}
}

// boundRLSTable leaves an omitted table alone. A declaration attached to a Go
// struct names its table through that struct, and the renderer resolves it
// there.
func boundRLSTable(resolver rlsTableResolver, structName, table string) string {
	if strings.TrimSpace(table) == "" {
		return table
	}
	return resolver.resolve(structName, table)
}

// rlsPolicyIdentity is what makes two row-level security policies the same
// policy: the table that owns it, and its own name.
//
// It is a struct rather than a joined string because both components are
// PostgreSQL identifiers and either may contain a dot when quoted, while
// Table.QualifiedName already spends the dot structurally. Any separator is
// therefore ambiguous; a struct key cannot be.
type rlsPolicyIdentity struct {
	table  string
	policy string
}

// deduplicateRLSPolicies keeps one policy per (table, policy name) pair.
//
// The table has to be part of the key. A PostgreSQL policy name is scoped to
// its table: `CREATE POLICY tenant_isolation` succeeds once on each of two
// tables in one schema and is refused only when repeated on the same table.
// The single-source path used to key on the policy name alone, which silently
// dropped the second of two identically named policies before the comparator
// ever saw it (stokaro/ptah#1276).
//
// The table component is the declared table the policy names rather than the
// string it was written with, which is [rlsTableResolver]'s subject.
func deduplicateRLSPolicies(policies []RLSPolicy, resolver rlsTableResolver) []RLSPolicy {
	return deduplicateNamedDefinitions(policies, func(policy RLSPolicy) rlsPolicyIdentity {
		return rlsPolicyIdentity{
			table:  resolver.resolve(policy.StructName, policy.Table),
			policy: policy.Name,
		}
	})
}

// deduplicateNamedDefinitions keeps the first definition per identity.
//
// The key type is a parameter rather than a string so a caller whose identity
// has SEVERAL components can use a struct instead of joining them with a
// separator. Joining is not safe here: every component is a PostgreSQL
// identifier, quoting lets any of them contain the separator, and the encoding
// then stops being injective -- table `a` with policy `"b.c"` and table `a.b`
// with policy `c` both render as `a.b.c`, and one of two distinct policies is
// dropped (stokaro/ptah#1276). Single-component callers keep passing strings.
func deduplicateNamedDefinitions[T any, K comparable](definitions []T, identity func(T) K) []T {
	seen := make(map[K]struct{}, len(definitions))
	deduplicated := make([]T, 0, len(definitions))
	for _, definition := range definitions {
		key := identity(definition)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduplicated = append(deduplicated, definition)
	}
	return deduplicated
}

func deduplicateSchemaObjects(r *Database) {
	r.Views = deduplicateViews(r.Views)
	r.MaterializedViews = deduplicateMaterializedViews(r.MaterializedViews)
	r.Triggers = deduplicateTriggers(r.Triggers)
	r.Constraints = deduplicateConstraints(r.Constraints)
	r.Grants = deduplicateGrants(r.Grants)
	r.Roles = deduplicateRoles(r.Roles)
}

func deduplicateSchemas(schemas []Schema) []Schema {
	seen := make(map[string]bool)
	deduplicated := make([]Schema, 0, len(schemas))
	for _, schema := range schemas {
		if !seen[schema.Name] {
			seen[schema.Name] = true
			deduplicated = append(deduplicated, schema)
		}
	}
	return deduplicated
}

func deduplicateIndexes(indexes []Index, tables []Table) []Index {
	type identity struct {
		tableName string
		indexName string
	}

	seen := make(map[identity]struct{}, len(indexes))
	deduplicated := make([]Index, 0, len(indexes))
	tableNames := ResolveIndexTableNames(indexes, tables)
	for position, index := range indexes {
		tableName := tableNames[position]
		if tableName == "" {
			tableName = index.StructName
		}
		key := identity{tableName: tableName, indexName: index.Name}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduplicated = append(deduplicated, index)
	}
	return deduplicated
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
	type triggerIdentity struct {
		table string
		name  string
	}
	seen := make(map[triggerIdentity]bool)
	deduplicated := make([]Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		trigger.Canonicalize()
		key := triggerIdentity{table: trigger.Table, name: trigger.Name}
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, trigger)
		}
	}
	return deduplicated
}

// deduplicateConstraints dedups table-level constraints by their declared
// table scope when table= is explicit, otherwise by the declaring Go type.
// The fallback is needed before normalizeTableScopedNames fills .Table for
// annotations that rely on struct association.
func deduplicateConstraints(constraints []Constraint) []Constraint {
	seen := make(map[string]bool)
	deduplicated := make([]Constraint, 0, len(constraints))
	for _, c := range constraints {
		key := constraintDedupKey(c)
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, c)
		}
	}
	return deduplicated
}

func constraintDedupKey(c Constraint) string {
	scope := c.StructName
	if strings.TrimSpace(c.Table) != "" {
		scope = strings.TrimSpace(c.Table)
	}
	return constraintIdentity(scope, c)
}

func constraintIdentity(scope string, constraint Constraint) string {
	if name := strings.TrimSpace(constraint.Name); name != "" {
		return scope + "\x00named\x00" + name
	}

	nullsDistinct := "unset"
	if constraint.NullsDistinct != nil {
		nullsDistinct = strconv.FormatBool(*constraint.NullsDistinct)
	}
	return strings.Join([]string{
		scope,
		"unnamed",
		strings.ToUpper(strings.TrimSpace(constraint.Type)),
		strings.Join(constraint.Columns, "\x01"),
		strings.Join(constraint.IncludeColumns, "\x01"),
		nullsDistinct,
		constraint.UsingMethod,
		constraint.ExcludeElements,
		constraint.WhereCondition,
		constraint.CheckExpression,
		constraint.ForeignTable,
		strings.Join(constraint.ForeignColumnsOrDefault(), "\x01"),
		constraint.OnDelete,
		constraint.OnUpdate,
	}, "\x00")
}

// deduplicateGrants dedups by role + privileges + target (table, schema, or
// sequence). The grant option is part of identity: a plain grant and a grant
// WITH GRANT OPTION must both survive. Privilege order is normalized only for
// the key, so logically identical grants deduplicate even when annotations list
// privileges in a different order.
func deduplicateGrants(grants []Grant) []Grant {
	seen := make(map[grantKey]bool)
	deduplicated := make([]Grant, 0, len(grants))
	for _, g := range grants {
		g.Canonicalize()
		key := newGrantKey(g)
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, g)
		}
	}
	return deduplicated
}

// grantKey is the identity deduplicateGrants compares on.
//
// It is a struct rather than a delimiter-joined string because a joined string
// makes identity depend on the delimiters being absent from every component,
// and nothing here forbids them: Grant.Canonicalize trims and upper-cases, it
// does not reject a role, table, schema or sequence name containing the
// separators. The key used to be
// `role|privs|t:table|s:schema|q:sequence|o:bool`, under which a grant on the
// table `a|s:b` with no schema and a grant on the table `a` in the schema
// `b|s:` produced the same string, and the second one was silently dropped.
//
// This is the shape stokaro/ptah#1345 names as evidence -- #1283 lost distinct
// grants exactly this way -- so the answer is the one that removes the question
// rather than a better separator.
type grantKey struct {
	role string
	// privileges is still a joined string, because a slice is not comparable.
	// The separator is NUL, which no privilege token can carry: Canonicalize
	// upper-cases and trims each one, and the set they are drawn from is SQL
	// privilege words.
	privileges string
	onTable    string
	onSchema   string
	onSequence string
	withOption bool
}

func newGrantKey(g Grant) grantKey {
	privileges := append([]string(nil), g.Privileges...)
	sort.Strings(privileges)
	return grantKey{
		role:       g.Role,
		privileges: strings.Join(privileges, "\x00"),
		onTable:    g.OnTable,
		onSchema:   g.OnSchema,
		onSequence: g.OnSequence,
		withOption: g.WithOption,
	}
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
