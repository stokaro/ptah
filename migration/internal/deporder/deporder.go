package deporder

import (
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/stokaro/ptah/core/goschema"
)

// ViewLike is a PostgreSQL view-like object that can reference other view-like
// objects in its SELECT body.
type ViewLike struct {
	Name         string
	Body         string
	Materialized bool
}

// StableTopologicalSort returns nodes ordered so dependencies come first while
// preserving caller order for otherwise independent nodes. Cycles degrade
// deterministically by appending remaining nodes in caller order.
func StableTopologicalSort(nodes []string, dependencies map[string][]string) []string {
	index := indexNodes(nodes)
	inDegree := make(map[string]int, len(index))
	dependents := make(map[string][]string, len(index))

	for node := range index {
		inDegree[node] = 0
	}
	for node, deps := range dependencies {
		if _, ok := index[node]; !ok {
			continue
		}
		seenDeps := make(map[string]struct{}, len(deps))
		for _, dep := range deps {
			if dep == node {
				continue
			}
			if _, ok := index[dep]; !ok {
				continue
			}
			if _, seen := seenDeps[dep]; seen {
				continue
			}
			seenDeps[dep] = struct{}{}
			inDegree[node]++
			dependents[dep] = append(dependents[dep], node)
		}
	}

	for node := range dependents {
		sortByIndex(dependents[node], index)
	}

	queue := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if inDegree[node] == 0 {
			queue = append(queue, node)
		}
	}

	result := make([]string, 0, len(nodes))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		for _, dependent := range dependents[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = appendStable(queue, dependent, index)
			}
		}
	}

	if len(result) == len(nodes) {
		return result
	}

	seen := make(map[string]struct{}, len(result))
	for _, node := range result {
		seen[node] = struct{}{}
	}
	for _, node := range nodes {
		if _, ok := seen[node]; !ok {
			result = append(result, node)
		}
	}
	return result
}

// StableReverseDependencySort returns nodes ordered so dependents come before
// the objects they depend on, preserving caller order for independent nodes.
func StableReverseDependencySort(nodes []string, dependencies map[string][]string) []string {
	index := indexNodes(nodes)
	dependents := make(map[string][]string, len(index))
	for _, child := range nodes {
		for _, parent := range dependencies[child] {
			if parent == child {
				continue
			}
			if _, ok := index[parent]; ok && !slices.Contains(dependents[parent], child) {
				dependents[parent] = append(dependents[parent], child)
			}
		}
	}
	for node := range dependents {
		sortByIndex(dependents[node], index)
	}

	result := make([]string, 0, len(nodes))
	state := make(map[string]int, len(nodes))
	var visit func(string)
	visit = func(node string) {
		switch state[node] {
		case 1, 2:
			return
		}
		state[node] = 1
		for _, dependent := range dependents[node] {
			visit(dependent)
		}
		state[node] = 2
		result = append(result, node)
	}

	for _, node := range nodes {
		visit(node)
	}
	return result
}

// TablesForCreate returns target tables in dependency order for CREATE TABLE
// operations. It accepts either qualified or unqualified table names.
func TablesForCreate(schema *goschema.Database, tableNames []string) []goschema.Table {
	if schema == nil || len(tableNames) == 0 {
		return nil
	}

	tablesByKey := mapTablesByQualifiedName(schema.Tables)
	keys := tableKeysInInputOrder(schema.Tables, tableNames)
	orderedKeys := StableTopologicalSort(keys, GeneratedTableDependencies(schema))

	tables := make([]goschema.Table, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		if table, ok := tablesByKey[key]; ok {
			tables = append(tables, table)
		}
	}
	return tables
}

// TableDropOrder returns table names in child-before-parent order for DROP
// TABLE operations. Output names match the caller's input spelling.
func TableDropOrder(tableNames []string, schema *goschema.Database) []string {
	ordered := append([]string(nil), tableNames...)
	if schema == nil || len(ordered) < 2 {
		return ordered
	}

	inputByKey := make(map[string]string, len(ordered))
	keys := make([]string, 0, len(ordered))
	for _, tableName := range ordered {
		key := resolveTableKey(schema.Tables, tableName)
		if _, seen := inputByKey[key]; seen {
			continue
		}
		inputByKey[key] = tableName
		keys = append(keys, key)
	}

	orderedKeys := StableReverseDependencySort(keys, GeneratedTableDependencies(schema))
	result := make([]string, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		result = append(result, inputByKey[key])
	}
	return result
}

// GeneratedTableDependencies returns table dependency edges derived from
// finalized metadata plus inline field and table-level FK definitions.
func GeneratedTableDependencies(schema *goschema.Database) map[string][]string {
	dependencies := make(map[string][]string, len(schema.Tables))
	for _, table := range schema.Tables {
		dependencies[table.QualifiedName()] = append([]string(nil), schema.Dependencies[table.QualifiedName()]...)
	}

	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil {
			continue
		}
		addGeneratedTableDependency(dependencies, schema.Tables, *table, foreignReferenceTable(field.Foreign))
	}

	for _, embedded := range schema.EmbeddedFields {
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, embedded.StructName)
		if table == nil {
			continue
		}
		addGeneratedTableDependency(dependencies, schema.Tables, *table, foreignReferenceTable(embedded.Ref))
	}

	for _, constraint := range schema.Constraints {
		if constraint.ForeignTable == "" || !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil {
			continue
		}
		addGeneratedTableDependency(dependencies, schema.Tables, *table, constraint.ForeignTable)
	}

	return dependencies
}

// FunctionsForCreate returns target functions in dependency order.
func FunctionsForCreate(schema *goschema.Database, functionNames []string) []goschema.Function {
	if schema == nil || len(functionNames) == 0 {
		return nil
	}

	functionByName := make(map[string]goschema.Function, len(schema.Functions))
	requested := make(map[string]struct{}, len(functionNames))
	for _, functionName := range functionNames {
		requested[functionName] = struct{}{}
	}
	names := make([]string, 0, len(functionNames))
	for _, fn := range schema.Functions {
		functionByName[fn.Name] = fn
		if _, ok := requested[fn.Name]; ok {
			names = append(names, fn.Name)
		}
	}

	orderedNames := StableTopologicalSort(names, schema.FunctionDependencies)

	functions := make([]goschema.Function, 0, len(orderedNames))
	for _, name := range orderedNames {
		if fn, ok := functionByName[name]; ok {
			functions = append(functions, fn)
		}
	}
	return functions
}

// ViewLikesForCreate returns views and materialized views in dependency order
// when their bodies reference other added view-like objects.
func ViewLikesForCreate(objects []ViewLike) []ViewLike {
	if len(objects) < 2 {
		return append([]ViewLike(nil), objects...)
	}

	ids := make([]string, 0, len(objects))
	byID := make(map[string]ViewLike, len(objects))
	idsByName := make(map[string][]string, len(objects))
	for i, object := range objects {
		id := viewLikeID(object, i)
		ids = append(ids, id)
		byID[id] = object
		idsByName[object.Name] = append(idsByName[object.Name], id)
	}

	dependencies := make(map[string][]string, len(objects))
	for i, object := range objects {
		id := viewLikeID(object, i)
		for candidateName, candidateIDs := range idsByName {
			if candidateName == object.Name || !referencesIdentifier(object.Body, candidateName) {
				continue
			}
			dependencies[id] = append(dependencies[id], candidateIDs...)
		}
	}

	orderedIDs := StableTopologicalSort(ids, dependencies)
	ordered := make([]ViewLike, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		ordered = append(ordered, byID[id])
	}
	return ordered
}

func indexNodes(nodes []string) map[string]int {
	index := make(map[string]int, len(nodes))
	for i, node := range nodes {
		if _, exists := index[node]; !exists {
			index[node] = i
		}
	}
	return index
}

func sortByIndex(nodes []string, index map[string]int) {
	slices.SortFunc(nodes, func(a, b string) int {
		return index[a] - index[b]
	})
}

func appendStable(queue []string, node string, index map[string]int) []string {
	insertAt := len(queue)
	for i, queued := range queue {
		if index[node] < index[queued] {
			insertAt = i
			break
		}
	}
	queue = append(queue, "")
	copy(queue[insertAt+1:], queue[insertAt:])
	queue[insertAt] = node
	return queue
}

func mapTablesByQualifiedName(tables []goschema.Table) map[string]goschema.Table {
	result := make(map[string]goschema.Table, len(tables))
	for _, table := range tables {
		result[table.QualifiedName()] = table
	}
	return result
}

func tableKeysInInputOrder(tables []goschema.Table, tableNames []string) []string {
	keys := make([]string, 0, len(tableNames))
	seen := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		key := resolveTableKey(tables, tableName)
		if _, ok := seen[key]; ok {
			continue
		}
		if generatedTableByName(tables, tableName) == nil {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func resolveTableKey(tables []goschema.Table, tableName string) string {
	if table := generatedTableByName(tables, tableName); table != nil {
		return table.QualifiedName()
	}
	return tableName
}

func generatedTableByName(tables []goschema.Table, tableName string) *goschema.Table {
	tableName = strings.TrimSpace(tableName)
	for i := range tables {
		table := &tables[i]
		if table.Name == tableName || table.QualifiedName() == tableName {
			return table
		}
	}
	return nil
}

func generatedTableByStructName(tables []goschema.Table, structName string) *goschema.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []goschema.Table, structName, tableName string) *goschema.Table {
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
	if tableName == "" {
		return nil
	}
	return generatedTableByName(tables, tableName)
}

func addGeneratedTableDependency(
	dependencies map[string][]string,
	tables []goschema.Table,
	table goschema.Table,
	refTable string,
) {
	tableName := table.QualifiedName()
	refTable = resolveGeneratedReferenceTableName(tables, table, refTable)
	if tableName == refTable || slices.Contains(dependencies[tableName], refTable) {
		return
	}
	dependencies[tableName] = append(dependencies[tableName], refTable)
}

func resolveGeneratedReferenceTableName(tables []goschema.Table, table goschema.Table, refTable string) string {
	refTable = strings.TrimSpace(refTable)
	if strings.Contains(refTable, ".") {
		return refTable
	}

	if table.Schema != "" {
		schemaQualified := table.Schema + "." + refTable
		if ref := generatedTableByName(tables, schemaQualified); ref != nil {
			return ref.QualifiedName()
		}
	}

	var match string
	for _, candidate := range tables {
		if candidate.Name != refTable {
			continue
		}
		if match != "" {
			return refTable
		}
		match = candidate.QualifiedName()
	}
	if match != "" {
		return match
	}
	return refTable
}

func foreignReferenceTable(reference string) string {
	table, _, _ := strings.Cut(strings.TrimSpace(reference), "(")
	return strings.TrimSpace(table)
}

func viewLikeID(object ViewLike, index int) string {
	kind := "view"
	if object.Materialized {
		kind = "matview"
	}
	return kind + ":" + object.Name + ":" + strconv.Itoa(index)
}

func referencesIdentifier(body, name string) bool {
	body = strings.ToLower(body)
	name = strings.ToLower(strings.TrimSpace(name))
	if body == "" || name == "" {
		return false
	}

	for start := 0; start < len(body); {
		index := strings.Index(body[start:], name)
		if index < 0 {
			return false
		}
		index += start
		end := index + len(name)
		if (isIdentifierBoundary(body, index-1) || isQualifiedIdentifierTail(body, index-1)) && isIdentifierBoundary(body, end) {
			return true
		}
		start = end
	}
	return false
}

func isIdentifierBoundary(value string, index int) bool {
	if index < 0 || index >= len(value) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(value[index:])
	return !isSQLIdentifierRune(r)
}

func isQualifiedIdentifierTail(value string, index int) bool {
	return index >= 0 && index < len(value) && value[index] == '.'
}

func isSQLIdentifierRune(r rune) bool {
	return r == '_' || r == '$' || r == '.' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
