// Package deporder provides deterministic dependency ordering for schema
// objects: stable topological sorts used to create objects such as views in
// dependency order and drop them in reverse.
package deporder

import (
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// ViewLike is a view-like object that can reference other view-like objects in
// its SELECT body.
type ViewLike struct {
	Name         string
	Body         string
	Materialized bool
	// Routine marks a function or procedure ordered among the view-likes
	// because it names a relation the same plan creates; see
	// [RoutineWithRelations]. Its Body is [RoutineOrderingBody], so a view that
	// calls it follows it and a view it reads precedes it.
	Routine bool

	// DependsOn names objects this one must be created after, beyond the ones
	// its body mentions.
	//
	// A body is the only evidence the sort has, and it is not always the whole
	// truth: a view whose SELECT reaches a table through a function, a
	// search_path, or a name this dialect's reader does not resolve depends on
	// something no scan of the text can find. A declared edge says so directly.
	//
	// A name here that matches no object in the set contributes no edge rather
	// than an error: the set is what this render carries, and an object scoped
	// to another dialect is absent from it by design (stokaro/ptah#3121).
	DependsOn []string
}

// UserType is a PostgreSQL user-defined type -- a domain, a composite type or a
// range type -- named together with the type spellings its own definition
// mentions: a domain's base type, a composite's field types, a range's subtype.
//
// The three kinds share one namespace and can name each other in either
// direction, so they have to be ordered together rather than kind by kind.
// `CREATE DOMAIN d AS addr` needs the composite `addr` first, and
// `CREATE TYPE addr AS (f d)` needs the domain `d` first.
type UserType struct {
	// Name is the qualified name the caller uses to look the type back up.
	Name string
	// References are the type spellings the definition names, in the caller's
	// own spelling. Anything outside the set is ignored, so a reference to a
	// built-in type or to a type the database already has costs nothing.
	References []string
}

// UserTypesForCreate returns user-defined type names in creation order: a type
// another one names comes first. A cycle degrades to caller order, which is
// what PostgreSQL itself refuses to create anyway.
func UserTypesForCreate(userTypes []UserType) []string {
	return StableTopologicalSort(userTypeNames(userTypes), UserTypeDependencies(userTypes))
}

// UserTypesForDrop returns user-defined type names in drop order: a type that
// names another is dropped first, so a non-CASCADE drop is not blocked by a
// dependent the same plan is about to remove.
//
// The References the caller passes must be the ones the database holds NOW.
// A DROP executes against the current schema, so passing the definitions a plan
// intends to create instead orders the statements by a graph the server is not
// consulting, and the two differ whenever the change is what moves a reference.
// UserTypesForCreate is the call that takes the desired definitions.
func UserTypesForDrop(userTypes []UserType) []string {
	return StableReverseDependencySort(userTypeNames(userTypes), UserTypeDependencies(userTypes))
}

// UserTypeDependencies resolves each type's references against the names in the
// set and returns the edges the two sorts above run on.
func UserTypeDependencies(userTypes []UserType) map[string][]string {
	dependencies := make(map[string][]string, len(userTypes))
	for _, userType := range userTypes {
		for _, reference := range userType.References {
			name, ok := resolveUserTypeReference(userTypes, reference)
			if !ok || name == userType.Name || slices.Contains(dependencies[userType.Name], name) {
				continue
			}
			dependencies[userType.Name] = append(dependencies[userType.Name], name)
		}
	}
	return dependencies
}

func userTypeNames(userTypes []UserType) []string {
	names := make([]string, 0, len(userTypes))
	for _, userType := range userTypes {
		names = append(names, userType.Name)
	}
	return names
}

// resolveUserTypeReference maps one type spelling onto a name in the set. A
// qualified spelling must match a qualified name; an unqualified one falls back
// to the single type carrying that bare name, and stays unresolved when two
// schemas offer it, because guessing there would order the plan around a type
// the column does not use.
func resolveUserTypeReference(userTypes []UserType, reference string) (string, bool) {
	key := NormalizeTypeReference(reference)
	if key == "" {
		return "", false
	}
	for _, userType := range userTypes {
		if NormalizeTypeReference(userType.Name) == key {
			return userType.Name, true
		}
	}
	keyRef, parsed := tableref.Parse(key)
	if !parsed || keyRef.Qualified {
		return "", false
	}

	var match string
	for _, userType := range userTypes {
		ref, ok := tableref.Parse(NormalizeTypeReference(userType.Name))
		if !ok || ref.Name != keyRef.Name {
			continue
		}
		if match != "" {
			return "", false
		}
		match = userType.Name
	}
	return match, match != ""
}

// NormalizeTypeReference removes array dimensions and type modifiers while
// preserving quoted identifier case and punctuation. Unquoted identifiers
// follow PostgreSQL's ASCII case folding. Quoted dots and parentheses belong
// to the name, not to its qualification or modifiers.
func NormalizeTypeReference(reference string) string {
	scan := lexer.NewLexerWithOptions(reference, dialectlexer.Options("postgres"))
	var result strings.Builder
	spaced := false
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		switch {
		case token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment:
			spaced = true
		case token.Value == "(" || token.Value == "[":
			return strings.TrimSpace(result.String())
		case token.MatchOperatorValue("."):
			result.WriteByte('.')
			spaced = false
		default:
			if spaced && result.Len() > 0 && !strings.HasSuffix(result.String(), ".") {
				result.WriteByte(' ')
			}
			if token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`) {
				result.WriteString(tableref.CanonicalExact("", sqlident.UnquoteDoubleQuoted(token.Value)))
			} else {
				result.WriteString(identifier.ComparisonASCIIInsensitive.IdentityKey(token.Value))
			}
			spaced = false
		}
	}
	return strings.TrimSpace(result.String())
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
func TablesForCreate(schema *schemamodel.Database, tableNames []string) []schemamodel.Table {
	if schema == nil || len(tableNames) == 0 {
		return nil
	}

	tablesByKey := mapTablesByQualifiedName(schema.Tables)
	keys := tableKeysInInputOrder(schema.Tables, tableNames)
	orderedKeys := StableTopologicalSort(keys, GeneratedTableDependencies(schema))

	tables := make([]schemamodel.Table, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		if table, ok := tablesByKey[key]; ok {
			tables = append(tables, table)
		}
	}
	return tables
}

// TableDropOrder returns table names in child-before-parent order for DROP
// TABLE operations. Output names match the caller's input spelling.
func TableDropOrder(tableNames []string, schema *schemamodel.Database) []string {
	if schema == nil {
		return append([]string(nil), tableNames...)
	}
	return TableDropOrderWithDependencies(tableNames, schema.Tables, GeneratedTableDependencies(schema))
}

// TableDropOrderWithDependencies is [TableDropOrder] for a caller holding the
// two inputs it reads rather than the whole schema.
//
// A planner is that caller: the diff carries the table list and the dependency
// graph, and handing the pieces over is what lets it order its drops without
// being handed the declaration they came out of (stokaro/ptah#2315).
func TableDropOrderWithDependencies(
	tableNames []string,
	tables []schemamodel.Table,
	dependencies map[string][]string,
) []string {
	ordered := append([]string(nil), tableNames...)
	if len(ordered) < 2 {
		return ordered
	}

	inputByKey := make(map[string]string, len(ordered))
	keys := make([]string, 0, len(ordered))
	for _, tableName := range ordered {
		key := resolveTableKey(tables, tableName)
		if _, seen := inputByKey[key]; seen {
			continue
		}
		inputByKey[key] = tableName
		keys = append(keys, key)
	}

	orderedKeys := StableReverseDependencySort(keys, dependencies)
	result := make([]string, 0, len(orderedKeys))
	for _, key := range orderedKeys {
		result = append(result, inputByKey[key])
	}
	return result
}

// GeneratedTableDependencies returns table dependency edges derived from
// finalized metadata plus inline field and table-level FK definitions.
func GeneratedTableDependencies(schema *schemamodel.Database) map[string][]string {
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

// GeneratedSelfReferencingForeignKeys derives the self-references a declaration
// expresses, unioned with the ones it already carries.
//
// The sibling of [GeneratedTableDependencies] and for the same reason. A table
// that references itself produces no dependency edge -- it cannot be created
// after itself -- so the constraint travels separately, and reading it out of
// `Database.SelfReferencingForeignKeys` alone made it depend on
// [schemamodel.Finalize] having run. A declaration assembled in memory has an
// empty map, and an empty map is indistinguishable from a table that has no
// self-reference: the table was created, the plan reported success, and the
// constraint was not there (stokaro/ptah#2471).
//
// It reads the same three kinds of edge the dependency derivation does -- a
// field's `foreign=`, a relation-mode embedded field, and a table-level FOREIGN
// KEY constraint -- and keeps only those whose reference resolves to the table
// itself.
func GeneratedSelfReferencingForeignKeys(
	schema *schemamodel.Database,
) map[string][]schemamodel.SelfReferencingFK {
	selfReferences := make(map[string][]schemamodel.SelfReferencingFK, len(schema.Tables))
	for _, table := range schema.Tables {
		name := table.QualifiedName()
		selfReferences[name] = append(
			[]schemamodel.SelfReferencingFK(nil), schema.SelfReferencingForeignKeys[name]...)
	}

	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil {
			continue
		}
		addGeneratedSelfReference(selfReferences, schema.Tables, *table,
			foreignReferenceTable(field.Foreign), schemamodel.SelfReferencingFK{
				FieldName: field.Name, Foreign: field.Foreign,
				ForeignKeyName: field.ForeignKeyName,
				OnDelete:       field.OnDelete, OnUpdate: field.OnUpdate,
			})
	}

	for _, embedded := range schema.EmbeddedFields {
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, embedded.StructName)
		if table == nil {
			continue
		}
		addGeneratedSelfReference(selfReferences, schema.Tables, *table,
			foreignReferenceTable(embedded.Ref), schemamodel.SelfReferencingFK{
				FieldName: embedded.Field, Foreign: embedded.Ref,
			})
	}

	// A table-level FOREIGN KEY constraint is deliberately absent from this
	// derivation, and [schemamodel.BuildDependencyGraph] says why at its own
	// constraint branch: such a constraint keeps its structured local and
	// referenced column lists, and [schemamodel.SelfReferencingFK] is
	// intentionally single-column and lossy, so there is nothing to project it
	// into. It is also already emitted -- the constraint pool renders it, as an
	// ALTER on the targets that have one and inline on SQLite, which is why
	// deriving it here produced the object twice.
	//
	// Measured before it was removed: a single-column self-reference declared
	// this way emitted two identical ALTERs under one constraint name, and a
	// composite one emitted a second statement naming the column
	// `"owner_a, owner_b"`, which no table has (stokaro/ptah#2583).

	return selfReferences
}

// addGeneratedSelfReference records one, when the reference is to the table
// itself and the declaration does not already carry it.
func addGeneratedSelfReference(
	selfReferences map[string][]schemamodel.SelfReferencingFK,
	tables []schemamodel.Table,
	table schemamodel.Table,
	refTable string,
	candidate schemamodel.SelfReferencingFK,
) {
	tableName := table.QualifiedName()
	if tableName != resolveGeneratedReferenceTableName(tables, table, refTable) {
		return
	}
	for _, held := range selfReferences[tableName] {
		// By the field and the constraint name, which is what identifies one:
		// two self-references on one column with different names are two
		// constraints, and the same one derived twice is one.
		if held.FieldName == candidate.FieldName &&
			held.ForeignKeyName == candidate.ForeignKeyName {
			return
		}
	}
	selfReferences[tableName] = append(selfReferences[tableName], candidate)
}

// FunctionsForCreate orders the routines a change adds by their dependencies.
//
// It takes the routines rather than their names, and keeps every one of them.
// Keyed by name it kept a map entry per name instead, so two overloads of one
// name collapsed to whichever the schema listed last: one was created twice and
// the other never (stokaro/ptah#2408). The dependency graph is still name-level
// -- a routine body names another routine, not one of its overloads -- so the
// ORDER comes from the names and the OUTPUT is every routine that carries one.
func FunctionsForCreate(schema *schemamodel.Database, routines []schemamodel.Function) []schemamodel.Function {
	if schema == nil {
		return nil
	}
	declared := make([]string, 0, len(schema.Functions))
	for _, function := range schema.Functions {
		declared = append(declared, function.Name)
	}
	return FunctionsForCreateWithOrdering(routines, declared, schema.FunctionDependencies)
}

// FunctionsForCreateWithOrdering is [FunctionsForCreate] for a caller holding
// the two ordering inputs rather than the whole schema.
//
// A planner is that caller: the diff carries the bodies, the declaration order
// and the call graph, and handing the pieces over is what lets it order what it
// is about to create without being handed the schema they came out of
// (stokaro/ptah#2315).
func FunctionsForCreateWithOrdering(
	routines []schemamodel.Function,
	declaredOrder []string,
	dependencies map[string][]string,
) []schemamodel.Function {
	if len(routines) == 0 {
		return nil
	}

	byName := make(map[string][]schemamodel.Function, len(routines))
	for _, routine := range routines {
		byName[routine.Name] = append(byName[routine.Name], routine)
	}

	// Declaration order, so the sort below is stable against the document
	// rather than against a map walk.
	names := make([]string, 0, len(routines))
	seen := make(map[string]struct{}, len(routines))
	for _, name := range declaredOrder {
		if _, wanted := byName[name]; !wanted {
			continue
		}
		if _, already := seen[name]; already {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	// A routine the order does not name is still created, in the order it
	// arrived. An incomplete ordering input must cost the ORDER and not the
	// statement: built the other way, a caller that filled the routines and
	// forgot the order got an empty plan and a migration reporting success
	// with no function in it (stokaro/ptah#2315).
	for _, routine := range routines {
		if _, already := seen[routine.Name]; already {
			continue
		}
		seen[routine.Name] = struct{}{}
		names = append(names, routine.Name)
	}

	ordered := make([]schemamodel.Function, 0, len(routines))
	for _, name := range StableTopologicalSort(names, dependencies) {
		ordered = append(ordered, byName[name]...)
	}
	return ordered
}

// ViewLikesForCreate returns views and materialized views in dependency order
// when their bodies reference other added view-like objects.
func ViewLikesForCreate(objects []ViewLike) []ViewLike {
	return viewLikesForCreate(objects, "")
}

// ViewLikesForCreateForDialect returns view-like objects in dependency order
// using the target dialect's identifier quoting rules. Qualified declarations
// are matched by their canonical qualified spelling, while an unqualified body
// reference resolves only when exactly one declaration has that bare name.
func ViewLikesForCreateForDialect(objects []ViewLike, dialect string) []ViewLike {
	return viewLikesForCreate(objects, dialect)
}

func viewLikesForCreate(objects []ViewLike, dialect string) []ViewLike {
	if len(objects) < 2 {
		return append([]ViewLike(nil), objects...)
	}
	graph := newViewLikeGraph(objects, dialect)
	return graph.ordered(StableTopologicalSort(graph.ids, graph.reads))
}

// ViewLikesForDropForDialect returns view-like objects in an order they can be
// dropped one at a time without CASCADE: an object comes after every object
// of the set that reads it. Objects with no such reader keep the order they
// arrived in. Dependency is read as [ViewLikesForCreateForDialect] reads it,
// so for the edges it finds this order is the mirror of that one.
//
// A dependency cycle cannot be dropped one object at a time; the objects on
// it keep their arrival order, as they do in the creation order.
func ViewLikesForDropForDialect(objects []ViewLike, dialect string) []ViewLike {
	if len(objects) < 2 {
		return append([]ViewLike(nil), objects...)
	}
	graph := newViewLikeGraph(objects, dialect)
	readers := make(map[string][]string, len(graph.reads))
	for _, reader := range graph.ids {
		for _, read := range graph.reads[reader] {
			readers[read] = append(readers[read], reader)
		}
	}
	return graph.ordered(StableTopologicalSort(graph.ids, readers))
}

// viewLikeGraph is a set of view-like objects and the edges between them:
// reads[id] holds the objects the one with that id reads.
type viewLikeGraph struct {
	ids   []string
	byID  map[string]ViewLike
	reads map[string][]string
}

func newViewLikeGraph(objects []ViewLike, dialect string) viewLikeGraph {
	graph := viewLikeGraph{
		ids:   make([]string, 0, len(objects)),
		byID:  make(map[string]ViewLike, len(objects)),
		reads: make(map[string][]string, len(objects)),
	}
	idsByName := make(map[string][]string, len(objects))
	for i, object := range objects {
		id := viewLikeID(object, i)
		graph.ids = append(graph.ids, id)
		graph.byID[id] = object
		idsByName[object.Name] = append(idsByName[object.Name], id)
	}
	bareNameCounts := viewLikeBareNameCounts(idsByName)
	for i, object := range objects {
		id := viewLikeID(object, i)
		for candidateName, candidateIDs := range idsByName {
			if candidateName == object.Name ||
				!referencesViewLikeIdentifier(object.Body, candidateName, dialect, bareNameCounts) {
				continue
			}
			graph.reads[id] = append(graph.reads[id], candidateIDs...)
		}
		graph.reads[id] = append(graph.reads[id], declaredViewLikeEdges(object, idsByName)...)
	}
	return graph
}

func (g viewLikeGraph) ordered(ids []string) []ViewLike {
	ordered := make([]ViewLike, 0, len(ids))
	for _, id := range ids {
		ordered = append(ordered, g.byID[id])
	}
	return ordered
}

// declaredViewLikeEdges resolves an object's [ViewLike.DependsOn] against the
// set being ordered.
//
// Matching folds the schema off BOTH sides, because the two spellings come from
// different places: the set holds whatever the frontend qualified the object
// with, and a declaration names its dependency the way its author wrote it.
// A declaration reading `depends_on = [view.v]` has to reach `app.v` in the
// set, and one reading `app.v` has to reach a set holding `v`.
//
// An object naming itself contributes no edge: the sort would report a cycle
// over a statement that is fine.
func declaredViewLikeEdges(object ViewLike, idsByName map[string][]string) []string {
	var edges []string
	for _, declared := range object.DependsOn {
		wanted := viewLikeBareName(declared)
		if wanted == "" || wanted == viewLikeBareName(object.Name) {
			continue
		}
		for candidateName, candidateIDs := range idsByName {
			if viewLikeBareName(candidateName) != wanted {
				continue
			}
			edges = append(edges, candidateIDs...)
		}
	}
	return edges
}

// viewLikeBareName drops a schema qualifier, so two spellings of one object
// compare equal.
func viewLikeBareName(name string) string {
	trimmed := strings.TrimSpace(name)
	if _, bare, qualified := strings.Cut(trimmed, "."); qualified {
		return bare
	}
	return trimmed
}

func viewLikeBareNameCounts(idsByName map[string][]string) map[string]int {
	counts := make(map[string]int, len(idsByName))
	for name, ids := range idsByName {
		ref, ok := tableref.Parse(name)
		if !ok {
			continue
		}
		counts[strings.ToLower(ref.Name)] += len(ids)
	}
	return counts
}

// referencesViewLikeIdentifier is the creation order's test of whether body
// reads the object called name.
//
// It reads names part by part, as [ReferencesRelation] does, so
// `analytics."zbase"` reads analytics.zbase, and it puts an unqualified name in
// the dialect's default schema, so `public.zbase` reads a zbase declared
// without one. Unlike [ReferencesRelation] it never guesses a schema it does
// not know: an edge decides which object is created first, and an edge to an
// object of another schema that has the same name reorders the creation or
// closes a cycle, while a missing one leaves the arrival order. An unqualified
// reference to a qualified object counts only where no other object of the
// set has that bare name. The dialect's quoted spellings are asked too, which
// is what reads a SQL Server name in brackets.
func referencesViewLikeIdentifier(body, name, dialect string, bareNameCounts map[string]int) bool {
	if dialect == "" {
		return ReferencesIdentifier(body, name)
	}

	ref, ok := tableref.Parse(name)
	if !ok {
		return ReferencesIdentifier(body, name)
	}
	if ref.Qualified && referencesIdentifierSpellings(body, []string{
		name,
		tableref.Canonical(ref.Schema, ref.Name),
		sqlident.Qualified(dialect, ref.Schema, ref.Name),
	}, dialect) {
		return true
	}
	if !ref.Qualified && referencesIdentifierSpellings(body, []string{
		name,
		tableref.Canonical("", ref.Name),
		sqlident.Quote(dialect, ref.Name),
	}, dialect) {
		return true
	}
	defaultSchema := identifier.ForDialect(dialect).DefaultSchema
	inDefault := func(schema string) bool {
		return defaultSchema != "" && strings.EqualFold(schema, defaultSchema)
	}
	if namesRead(codeNames(body, dialect), ref, inDefault) {
		return true
	}

	if bareNameCounts[strings.ToLower(ref.Name)] != 1 {
		return false
	}
	return referencesUnqualifiedIdentifier(body, ref.Name, dialect)
}

func referencesIdentifierSpellings(body string, spellings []string, dialect string) bool {
	for _, spelling := range spellings {
		if referencesIdentifierSpelling(body, spelling, dialect) {
			return true
		}
	}
	return false
}

func referencesUnqualifiedIdentifier(body, name, dialect string) bool {
	return referencesUnquotedIdentifierSpelling(body, name, dialect) ||
		referencesIdentifierSpelling(body, tableref.Canonical("", name), dialect) ||
		referencesIdentifierSpelling(body, sqlident.Quote(dialect, name), dialect)
}

func referencesIdentifierSpelling(body, spelling, dialect string) bool {
	return referencesIdentifierSpellingWithBoundary(body, spelling, dialect, hasStandaloneIdentifierBoundaries)
}

func referencesUnquotedIdentifierSpelling(body, spelling, dialect string) bool {
	return referencesIdentifierSpellingWithBoundary(body, spelling, dialect, hasUnquotedIdentifierBoundaries)
}

func referencesIdentifierSpellingWithBoundary(
	body, spelling, dialect string,
	hasBoundaries func(string, int, int) bool,
) bool {
	body = strings.ToLower(body)
	spelling = strings.ToLower(strings.TrimSpace(spelling))
	if body == "" || spelling == "" {
		return false
	}

	masks := sqlMasksForDialect(body, dialect)
	for start := 0; start < len(body); {
		index := strings.Index(body[start:], spelling)
		if index < 0 {
			return false
		}
		index += start
		end := index + len(spelling)
		if spanIsCode(masks.code, index, end) &&
			hasBoundaries(body, index, end) &&
			spanMatchesIdentifierQuoting(body, spelling, masks, index, end) {
			return true
		}
		start = end
	}
	return false
}

func spanMatchesIdentifierQuoting(body, spelling string, masks sqlMasks, start, end int) bool {
	if !spanHasMarkedByte(masks.quotedIdentifier, start, end) {
		return true
	}
	if spelling[0] != '"' && spelling[0] != '`' {
		return strings.ContainsAny(spelling, "\"`")
	}
	return masks.quoteOpen[start] && masks.quoteClose[end-1] && body[start] == body[end-1]
}

func spanHasMarkedByte(mask []bool, start, end int) bool {
	for i := start; i < end; i++ {
		if mask[i] {
			return true
		}
	}
	return false
}

func hasStandaloneIdentifierBoundaries(body string, start, end int) bool {
	return isIdentifierBoundary(body, start-1) && isIdentifierBoundary(body, end)
}

func hasUnquotedIdentifierBoundaries(body string, start, end int) bool {
	return hasStandaloneIdentifierBoundaries(body, start, end) && isUnquotedReferenceBoundary(body, start, end)
}

func isUnquotedReferenceBoundary(body string, start, end int) bool {
	if start > 0 && strings.ContainsRune("\"`[]", rune(body[start-1])) {
		return false
	}
	return end >= len(body) || !strings.ContainsRune("\"`[]", rune(body[end]))
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

func mapTablesByQualifiedName(tables []schemamodel.Table) map[string]schemamodel.Table {
	result := make(map[string]schemamodel.Table, len(tables))
	for _, table := range tables {
		result[table.QualifiedName()] = table
	}
	return result
}

func tableKeysInInputOrder(tables []schemamodel.Table, tableNames []string) []string {
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

func resolveTableKey(tables []schemamodel.Table, tableName string) string {
	if table := generatedTableByName(tables, tableName); table != nil {
		return table.QualifiedName()
	}
	return tableName
}

func generatedTableByName(tables []schemamodel.Table, tableName string) *schemamodel.Table {
	tableName = strings.TrimSpace(tableName)
	for i := range tables {
		if tables[i].QualifiedName() == tableName {
			return &tables[i]
		}
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return nil
	}
	var match *schemamodel.Table
	for i := range tables {
		if tables[i].Name != ref.Name {
			continue
		}
		if match != nil {
			return nil
		}
		match = &tables[i]
	}
	return match
}

func generatedTableByStructName(tables []schemamodel.Table, structName string) *schemamodel.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []schemamodel.Table, structName, tableName string) *schemamodel.Table {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return generatedTableByStructName(tables, structName)
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
	return generatedTableByName(tables, tableName)
}

func addGeneratedTableDependency(
	dependencies map[string][]string,
	tables []schemamodel.Table,
	table schemamodel.Table,
	refTable string,
) {
	tableName := table.QualifiedName()
	refTable = resolveGeneratedReferenceTableName(tables, table, refTable)
	if tableName == refTable || slices.Contains(dependencies[tableName], refTable) {
		return
	}
	dependencies[tableName] = append(dependencies[tableName], refTable)
}

func resolveGeneratedReferenceTableName(tables []schemamodel.Table, table schemamodel.Table, refTable string) string {
	refTable = strings.TrimSpace(refTable)
	ref, ok := tableref.Parse(refTable)
	if !ok {
		return refTable
	}
	if ref.Qualified {
		return schemamodel.QualifyTableName(ref.Schema, ref.Name)
	}

	if table.Schema != "" {
		schemaQualified := schemamodel.QualifyTableName(table.Schema, ref.Name)
		if ref := generatedTableByName(tables, schemaQualified); ref != nil {
			return ref.QualifiedName()
		}
	}

	var match string
	for _, candidate := range tables {
		if candidate.Name != ref.Name {
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
	switch {
	case object.Routine:
		kind = "routine"
	case object.Materialized:
		kind = "matview"
	}
	return kind + ":" + object.Name + ":" + strconv.Itoa(index)
}

// spanIsCode reports whether every byte of body[start:end] is SQL code.
func spanIsCode(code []bool, start, end int) bool {
	for i := start; i < end; i++ {
		if !code[i] {
			return false
		}
	}
	return true
}

// sqlMasksForDialect marks the byte offsets of body that are SQL code:
// everything outside string literals, dollar-quoted strings, line comments and
// block comments. Quoted identifiers are code, but their contents are copied
// through without being re-scanned, so "a--b" is one identifier rather than
// the start of a comment. The masks are computed over the same string the
// caller searches, so no offset can slide out from under them.
//
// The scanner is PostgreSQL-oriented. It adds the target's nonstandard lexical
// forms, as its descriptor states them, without changing those rules: the line
// comments it opens beside -- and /* ... */, such as the #
// of the MySQL family and ClickHouse's # and //, and the backslash escapes of
// its strings and of its quoted names. MySQL's catalog writes a view's
// strings that way, as 'it\'s'. A target whose descriptor names a lexer of
// its own is read by that lexer instead; see [lexerMasks].
func sqlMasksForDialect(body, dialect string) sqlMasks {
	described, _ := platform.Lookup(dialect)
	lexical := described.Lexical
	brackets := described.Quoting.Style == descriptor.QuoteBracket
	if lexical.Lexer != "" {
		masks := lexerMasks(body, dialectlexer.FromDescriptor(described))
		masks.bracketNames = brackets
		return masks
	}
	masks := sqlMasks{
		code:             make([]bool, len(body)),
		quotedIdentifier: make([]bool, len(body)),
		quoteOpen:        make([]bool, len(body)),
		quoteClose:       make([]bool, len(body)),
		bracketNames:     brackets,
	}
	opensLineComment := func(rest string) bool {
		return slices.ContainsFunc(lexical.LineComments, func(marker string) bool {
			return marker != "" && strings.HasPrefix(rest, marker)
		})
	}
	for i := 0; i < len(body); {
		switch {
		case opensLineComment(body[i:]):
			i = skipLineComment(body, i)
		case strings.HasPrefix(body[i:], "--"):
			i = skipLineComment(body, i)
		case strings.HasPrefix(body[i:], "/*"):
			i = skipBlockComment(body, i)
		case body[i] == '\'':
			i = skipStringLiteral(body, i, lexical.BackslashEscapes)
		case body[i] == '"' || body[i] == '`':
			end, closeAt := quotedIdentifierEnd(body, i, lexical.IdentifierBackslashEscapes)
			masks.quoteOpen[i] = true
			if closeAt >= 0 {
				masks.quoteClose[closeAt] = true
			}
			for ; i < end; i++ {
				masks.code[i] = true
				masks.quotedIdentifier[i] = true
			}
		case body[i] == '$':
			end, ok := skipDollarQuoted(body, i)
			if !ok {
				masks.code[i] = true
				i++
				continue
			}
			i = end
		default:
			masks.code[i] = true
			i++
		}
	}
	return masks
}

type sqlMasks struct {
	code             []bool
	quotedIdentifier []bool
	quoteOpen        []bool
	quoteClose       []bool
	// bracketNames reports that the target quotes a name in brackets, as SQL
	// Server writes [dbo].[orders]. The masks mark no bracket: the spellings
	// match one as written, and [dottedNames] reads it as a quoted part.
	bracketNames bool
}

// lexerMasks marks a body by the tokens of the lexer its dialect's
// descriptor names rather than by the scanner above, whose rules are
// PostgreSQL's: a lexer of an engine's own reads strings, names and comments
// by that engine's rules, as YQL reads "x" as a string and @@...@@ as one.
// Reading the body with the lexer the splitter uses keeps the two in
// agreement about where a string ends.
//
// A string, a comment and an unknown token, such as a leading translation
// setting, are not code. An identifier the lexer kept in backticks is code and
// a quoted identifier, opened at its first byte and closed at its last. An
// unterminated one runs to the end of the body, and a quoted spelling cannot
// end at a byte it does not hold, so its last byte needs no separate test.
func lexerMasks(body string, options lexer.Options) sqlMasks {
	masks := sqlMasks{
		code:             make([]bool, len(body)),
		quotedIdentifier: make([]bool, len(body)),
		quoteOpen:        make([]bool, len(body)),
		quoteClose:       make([]bool, len(body)),
	}
	l := lexer.NewLexerWithOptions(body, options)
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		switch {
		case token.Type == lexer.TokenString, token.Type == lexer.TokenComment, token.Type == lexer.TokenUnknown:
			continue
		case token.Type == lexer.TokenIdentifier && strings.HasPrefix(token.Value, "`"):
			masks.quoteOpen[token.Start] = true
			masks.quoteClose[token.End-1] = true
			for i := token.Start; i < token.End; i++ {
				masks.quotedIdentifier[i] = true
			}
		}
		for i := token.Start; i < token.End; i++ {
			masks.code[i] = true
		}
	}
	return masks
}

func skipLineComment(body string, start int) int {
	if end := strings.IndexByte(body[start:], '\n'); end >= 0 {
		return start + end + 1
	}
	return len(body)
}

// skipBlockComment consumes a /* ... */ comment. PostgreSQL nests them, so the
// scan counts depth rather than stopping at the first close.
func skipBlockComment(body string, start int) int {
	depth := 0
	for i := start; i < len(body); {
		switch {
		case strings.HasPrefix(body[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(body[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(body)
}

// skipStringLiteral consumes a '...' literal. A doubled quote continues it.
// PostgreSQL enables backslash escapes for E-prefixed literals; ClickHouse and
// the MySQL family enable them for ordinary literals, which backslashEscapes
// carries here.
func skipStringLiteral(body string, start int, backslashEscapes bool) int {
	escapes := backslashEscapes || start > 0 && body[start-1] == 'e' &&
		(start == 1 || !isSQLIdentifierRune(rune(body[start-2])))
	for i := start + 1; i < len(body); {
		switch {
		case escapes && body[i] == '\\' && i+1 < len(body):
			i += 2
		case body[i] == '\'' && i+1 < len(body) && body[i+1] == '\'':
			i += 2
		case body[i] == '\'':
			return i + 1
		default:
			i++
		}
	}
	return len(body)
}

// quotedIdentifierEnd consumes a standard-quoted or backtick-quoted identifier,
// including doubled closing quotes and, where the target's names take them,
// backslash escapes, as ClickHouse's do. A MySQL name takes none: `a\` is a
// name ending in a backslash. It returns both the exclusive end and the
// closing delimiter offset, or -1 when the identifier is unterminated.
func quotedIdentifierEnd(body string, start int, backslashEscapes bool) (end, closeAt int) {
	quote := body[start]
	for i := start + 1; i < len(body); {
		switch {
		case backslashEscapes && body[i] == '\\' && i+1 < len(body):
			i += 2
		case body[i] == quote && i+1 < len(body) && body[i+1] == quote:
			i += 2
		case body[i] == quote:
			return i + 1, i
		default:
			i++
		}
	}
	return len(body), -1
}

// skipDollarQuoted consumes a $tag$ ... $tag$ string, reporting false when the
// dollar sign does not open one -- a positional parameter such as $1, or a
// dollar sign inside an identifier.
func skipDollarQuoted(body string, start int) (int, bool) {
	end := start + 1
	for end < len(body) {
		r, size := utf8.DecodeRuneInString(body[end:])
		if r == '_' || unicode.IsLetter(r) || (end > start+1 && unicode.IsDigit(r)) {
			end += size
			continue
		}
		break
	}
	if end >= len(body) || body[end] != '$' {
		return start, false
	}

	tag := body[start : end+1]
	if closing := strings.Index(body[end+1:], tag); closing >= 0 {
		return end + 1 + closing + len(tag), true
	}
	return len(body), true
}

func isIdentifierBoundary(value string, index int) bool {
	if index < 0 || index >= len(value) {
		return true
	}
	r, _ := utf8.DecodeRuneInString(value[index:])
	return !isSQLIdentifierRune(r)
}

func isSQLIdentifierRune(r rune) bool {
	return r == '_' || r == '$' || r == '.' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
