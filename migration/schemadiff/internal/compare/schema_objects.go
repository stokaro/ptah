package compare

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/tableref"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Functions performs PostgreSQL function comparison between generated and database schemas.
//
// This function handles the comparison of PostgreSQL custom functions, which are
// PostgreSQL-specific features used for stored procedures, triggers, and custom
// business logic. Functions are compared by name and their complete definition.
//
// # Function Comparison Logic
//
// **Generated Schema Functions**:
//   - Includes all functions defined in Go struct annotations
//   - These are functions the developer intentionally created
//
// **Database Schema Functions**:
//   - Includes all user-defined functions from the database
//   - Excludes system functions and built-in PostgreSQL functions
//   - Excludes extension-owned functions (filtered by database reader)
//
// # Extension Function Filtering
//
// Extension-owned functions are automatically excluded by the database reader to prevent
// migration issues. Extension functions cannot be dropped independently and attempting
// to do so will cause migration failures. Common extensions with functions include:
//   - btree_gin: Functions like gin_btree_consistent, gin_extract_*
//   - pg_trgm: Functions like similarity, word_similarity, gin_trgm_*
//
// # Function Modification Detection
//
// Functions are considered modified if any of the following differ:
//   - Parameters (type, names, order)
//   - Return type
//   - Function body/implementation
//   - Language (plpgsql, sql, etc.)
//   - Security context (DEFINER vs INVOKER)
//   - Volatility (STABLE, IMMUTABLE, VOLATILE)
//
// # Example Scenarios
//
// **Function addition**:
//   - Generated schema defines "get_current_tenant_id()"
//   - Database doesn't have this function
//   - Result: "get_current_tenant_id" added to diff.FunctionsAdded
//
// **Function removal**:
//   - Database has "old_helper_function()"
//   - Generated schema doesn't define this function
//   - Result: "old_helper_function" added to diff.FunctionsRemoved
//
// **Function modification**:
//   - Both have "calculate_total()" function
//   - Generated: different body or parameters
//   - Result: FunctionDiff added to diff.FunctionsModified
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: SchemaDiff structure to populate with discovered differences
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.FunctionsAdded: Functions that need to be created
//   - diff.FunctionsRemoved: Functions that exist in database but not in target schema
//   - diff.FunctionsModified: Functions with definition differences
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs.
func Functions(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	FunctionsWithDialect(generated, database, diff, "")
}

// routineIdentityKey returns the key under which two spellings of one stored
// routine name are the same routine.
//
// Stored-routine names are case-insensitive on MySQL and MariaDB, and that is
// independent of the table-name rules [identifier.Semantics] carries: both
// engines report TableNames as ComparisonExact, and lower_case_table_names does
// not govern routines. Measured on MySQL 26.7.0, with `foo` in the catalog,
// `SELECT Foo(1)` and `SELECT FOO(1)` both resolve to it, `DROP FUNCTION IF
// EXISTS Foo` drops it, and `CREATE FUNCTION BAR` is refused with
// Error 1304 "FUNCTION BAR already exists" while `bar` is present.
//
// Keying routines by their exact spelling therefore made live `foo` and desired
// `Foo` two objects: the diff carried an addition AND a removal, the planner
// created `Foo` and then executed `DROP FUNCTION IF EXISTS foo`, which resolves
// to the very routine it had just created, and a successful apply left the
// database with no function at all. Measured on both engines: zero rows in
// information_schema.ROUTINES afterwards.
func routineIdentityKey(name, dialect string) string {
	if isMySQLFamily(dialect) {
		return strings.ToLower(name)
	}
	return name
}

// qualifiedRoutineIdentityKey folds ONLY the routine component of a name that
// may carry a schema.
//
// The scope of the folding rule matters as much as the rule. Routine names are
// case-insensitive on these engines; SCHEMA names are not -- they follow
// lower_case_table_names, which is 0 on both pinned images, and the identifier
// semantics already describe them as ComparisonExact. Folding the whole string
// applied the routine rule to the database name too.
//
// That was not symmetrical, which is what made it a defect rather than merely
// a wide rule: the desired side folded `Sales.Foo` whole, while the database
// side folded only [types.DBFunction.Name] and left `Schema` exact. The two
// identities then disagreed on the schema component, so an unchanged function
// was reported as BOTH added and removed and the plan tried to create a
// function that was already there.
//
// Parsing is delegated to tableref so a routine whose own name contains a dot,
// quoted as `"tenant.data"`, is not mistaken for a schema-qualified one.
// The key is built through tableref.Canonical on BOTH sides so the two spell
// an unqualified name the same way by construction; findDatabaseFunction
// re-parses it and looks the qualified form up the same way.
func qualifiedRoutineIdentityKey(name, dialect string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return tableref.Canonical("", routineIdentityKey(name, dialect))
	}
	return tableref.Canonical(ref.Schema, routineIdentityKey(ref.Name, dialect))
}

// FunctionsWithDialect compares functions using the target's routine identity
// and type-spelling rules. See [routineIdentityKey] and
// [FunctionDefinitionsWithDialect] for what the dialect decides.
func FunctionsWithDialect(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	// Build lookup maps for function comparison
	generatedFunctionMap := make(map[string]goschema.Function)
	for _, fn := range generated.Functions {
		generatedFunctionMap[qualifiedRoutineIdentityKey(fn.Name, dialect)] = fn
	}

	// A generated function's name may be schema-qualified -- the HCL parser
	// writes `extra.f` from a `function` block's schema attribute, and so does
	// a database read -- while the reader reports the two parts separately. The
	// two sides are matched the way views are, by qualified name where the
	// generated side carries one and by bare name where it does not, so that a
	// document describing `extra.f` is not compared against `public.f` and a
	// round trip does not plan a redundant CREATE OR REPLACE (stokaro/ptah#1276).
	databaseFunctionsByName := make(map[string][]types.DBFunction, len(database.Functions))
	databaseFunctionsByQualifiedName := make(map[string]types.DBFunction, len(database.Functions))
	for _, fn := range database.Functions {
		key := routineIdentityKey(fn.Name, dialect)
		databaseFunctionsByName[key] = append(databaseFunctionsByName[key], fn)
		// The schema keeps its own spelling; only the routine half is folded.
		databaseFunctionsByQualifiedName[tableref.Canonical(fn.Schema, key)] = fn
	}

	matchedDatabaseFunctions := make(map[string]struct{}, len(database.Functions))
	for functionKey, generatedFunction := range generatedFunctionMap {
		databaseFunction, exists := findDatabaseFunction(
			functionKey,
			databaseFunctionsByName,
			databaseFunctionsByQualifiedName,
		)
		if !exists {
			// The desired spelling, never the folded key: the planner resolves
			// this name back to its declaration by exact match, and the
			// rendered DDL must carry what the operator wrote.
			diff.FunctionsAdded = append(diff.FunctionsAdded, generatedFunction.Name)
			continue
		}
		matchedDatabaseFunctions[databaseFunction.QualifiedName()] = struct{}{}
		functionComparison := FunctionDefinitionsWithDialect(generatedFunction, databaseFunction, dialect)
		if len(functionComparison.Changes) > 0 {
			diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
		}
	}

	for _, fn := range database.Functions {
		if _, ok := matchedDatabaseFunctions[fn.QualifiedName()]; ok {
			continue
		}
		diff.FunctionsRemoved = append(diff.FunctionsRemoved, fn.QualifiedName())
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.FunctionsAdded)
	sort.Strings(diff.FunctionsRemoved)
	sort.Slice(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	})
}

// FunctionsWithSemantics compares function identities using the target
// database's default-schema and identifier rules. The names retained in the
// diff remain the original desired/current spellings so downstream planners
// can resolve the exact source objects they received.
func FunctionsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize("")
	if semantics.DefaultSchema == "" {
		FunctionsWithDialect(generated, database, diff, dialect)
		return
	}

	generatedFunctions := make(map[tableIdentity]goschema.Function, len(generated.Functions))
	generatedNames := make(map[tableIdentity]string, len(generated.Functions))
	for _, function := range generated.Functions {
		// qualifiedRoutineIdentityKey, not routineIdentityKey: folding the whole
		// string lowercased the SCHEMA too, while the database loop below folds
		// only the routine name and leaves function.Schema to the identifier
		// semantics. The two sides then disagreed about the schema component of
		// `Sales.Foo`, so an unchanged function was reported as both added and
		// removed and the plan tried to create one that already existed.
		identity := newQualifiedTableIdentity(
			qualifiedRoutineIdentityKey(function.Name, dialect), semantics)
		generatedFunctions[identity] = function
		generatedNames[identity] = function.Name
	}
	databaseFunctions := make(map[tableIdentity]types.DBFunction, len(database.Functions))
	databaseNames := make(map[tableIdentity]string, len(database.Functions))
	for _, function := range database.Functions {
		identity := newTableIdentity(
			function.Schema, routineIdentityKey(function.Name, dialect), semantics)
		databaseFunctions[identity] = function
		databaseNames[identity] = function.QualifiedName()
	}

	for identity, generatedFunction := range generatedFunctions {
		databaseFunction, exists := databaseFunctions[identity]
		if !exists {
			diff.FunctionsAdded = append(diff.FunctionsAdded, generatedNames[identity])
			continue
		}
		functionComparison := FunctionDefinitionsWithDialect(generatedFunction, databaseFunction, dialect)
		if len(functionComparison.Changes) > 0 {
			diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
		}
	}
	for identity := range databaseFunctions {
		if _, exists := generatedFunctions[identity]; !exists {
			diff.FunctionsRemoved = append(diff.FunctionsRemoved, databaseNames[identity])
		}
	}

	sort.Strings(diff.FunctionsAdded)
	sort.Strings(diff.FunctionsRemoved)
	sort.Slice(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	})
}

// findDatabaseFunction resolves one generated function name against the read.
//
// A qualified name is matched only against the same (schema, name); a bare one
// is matched against a uniquely named function, and against nothing when two
// schemas hold that name -- guessing there is what would attribute a function to
// a schema it does not belong to. It is the shape
// [findDatabaseViewForGeneratedView] uses, and deliberately so: both answer the
// same question about an object whose generated name may or may not carry its
// schema.
func findDatabaseFunction(
	name string,
	byName map[string][]types.DBFunction,
	byQualifiedName map[string]types.DBFunction,
) (types.DBFunction, bool) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return types.DBFunction{}, false
	}
	if ref.Qualified {
		fn, ok := byQualifiedName[tableref.Canonical(ref.Schema, ref.Name)]
		return fn, ok
	}
	candidates := byName[ref.Name]
	if len(candidates) != 1 {
		return types.DBFunction{}, false
	}
	return candidates[0], true
}

// Views compares view definitions between generated and database schemas.
func Views(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	ViewsWithDialect(generated, database, diff, "")
}

// ViewsWithDialect compares view definitions with dialect-aware normalization
// for catalog readback forms that are semantically equivalent to Ptah-rendered
// view SQL.
func ViewsWithDialect(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff, dialect string) {
	generatedViews := make(map[string]goschema.View, len(generated.Views))
	for _, view := range generated.Views {
		generatedViews[view.Name] = view
	}

	databaseViewsByName := make(map[string][]types.DBView, len(database.Views))
	databaseViewsByQualifiedName := make(map[string]types.DBView, len(database.Views))
	for _, view := range database.Views {
		databaseViewsByName[view.Name] = append(databaseViewsByName[view.Name], view)
		databaseViewsByQualifiedName[view.QualifiedName()] = view
	}

	matchedDatabaseViews := make(map[string]struct{}, len(database.Views))
	for viewName, generatedView := range generatedViews {
		databaseView, exists := findDatabaseViewForGeneratedView(
			generatedView,
			databaseViewsByName,
			databaseViewsByQualifiedName,
		)
		if !exists {
			diff.ViewsAdded = append(diff.ViewsAdded, viewName)
			continue
		}

		matchedDatabaseViews[databaseView.QualifiedName()] = struct{}{}
		viewDiff := ViewDefinitionsWithDialect(generatedView, databaseView, dialect)
		if len(viewDiff.Changes) > 0 {
			diff.ViewsModified = append(diff.ViewsModified, viewDiff)
		}
	}

	for _, view := range database.Views {
		if _, ok := matchedDatabaseViews[view.QualifiedName()]; ok {
			continue
		}
		diff.ViewsRemoved = append(diff.ViewsRemoved, viewNameForDiff(view))
	}

	sort.Strings(diff.ViewsAdded)
	sort.Strings(diff.ViewsRemoved)
	sort.Slice(diff.ViewsModified, func(i, j int) bool {
		return diff.ViewsModified[i].ViewName < diff.ViewsModified[j].ViewName
	})
}

// ViewsWithSemantics compares view identity with the live database's resolved
// default schema while retaining dialect-aware SQL-body normalization.
func ViewsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize(dialect)
	if semantics.DefaultSchema == "" {
		ViewsWithDialect(generated, database, diff, dialect)
		return
	}

	generatedViews := make(map[tableIdentity]goschema.View, len(generated.Views))
	generatedNames := make(map[tableIdentity]string, len(generated.Views))
	for _, view := range generated.Views {
		identity := newQualifiedTableIdentity(view.Name, semantics)
		generatedViews[identity] = view
		generatedNames[identity] = view.Name
	}
	databaseViews := make(map[tableIdentity]types.DBView, len(database.Views))
	databaseNames := make(map[tableIdentity]string, len(database.Views))
	for _, view := range database.Views {
		identity := newTableIdentity(view.Schema, view.Name, semantics)
		databaseViews[identity] = view
		databaseNames[identity] = viewNameForDiff(view)
	}

	for identity, generatedView := range generatedViews {
		databaseView, exists := databaseViews[identity]
		if !exists {
			diff.ViewsAdded = append(diff.ViewsAdded, generatedNames[identity])
			continue
		}
		viewDiff := ViewDefinitionsWithDialect(generatedView, databaseView, dialect)
		if len(viewDiff.Changes) > 0 {
			diff.ViewsModified = append(diff.ViewsModified, viewDiff)
		}
	}
	for identity := range databaseViews {
		if _, exists := generatedViews[identity]; !exists {
			diff.ViewsRemoved = append(diff.ViewsRemoved, databaseNames[identity])
		}
	}

	sort.Strings(diff.ViewsAdded)
	sort.Strings(diff.ViewsRemoved)
	sort.Slice(diff.ViewsModified, func(i, j int) bool {
		return diff.ViewsModified[i].ViewName < diff.ViewsModified[j].ViewName
	})
}

func findDatabaseViewForGeneratedView(
	generatedView goschema.View,
	databaseViewsByName map[string][]types.DBView,
	databaseViewsByQualifiedName map[string]types.DBView,
) (types.DBView, bool) {
	ref, ok := tableref.Parse(generatedView.Name)
	if !ok {
		return types.DBView{}, false
	}
	if ref.Qualified {
		view, ok := databaseViewsByQualifiedName[tableref.Canonical(ref.Schema, ref.Name)]
		return view, ok
	}
	candidates := databaseViewsByName[ref.Name]
	if len(candidates) != 1 {
		return types.DBView{}, false
	}
	return candidates[0], true
}

func viewNameForDiff(view types.DBView) string {
	if view.Schema == "" {
		return view.Name
	}
	return view.QualifiedName()
}

// MaterializedViews compares materialized view definitions between generated
// and database schemas.
func MaterializedViews(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedViews := make(map[string]goschema.MaterializedView)
	for _, view := range generated.MaterializedViews {
		view.Canonicalize()
		generatedViews[view.Name] = view
	}

	databaseViews := make(map[string]types.DBMatView)
	for _, view := range database.MatViews {
		databaseViews[view.QualifiedName()] = view
	}

	addedViews, removedViews := compareNamedItems(generatedViews, databaseViews)
	diff.MaterializedViewsAdded = append(diff.MaterializedViewsAdded, addedViews...)
	diff.MaterializedViewsRemoved = append(diff.MaterializedViewsRemoved, removedViews...)

	for viewName, generatedView := range generatedViews {
		if databaseView, exists := databaseViews[viewName]; exists {
			viewDiff := MaterializedViewDefinitions(generatedView, databaseView)
			if len(viewDiff.Changes) > 0 {
				diff.MaterializedViewsModified = append(diff.MaterializedViewsModified, viewDiff)
			}
		}
	}

	sort.Strings(diff.MaterializedViewsAdded)
	sort.Strings(diff.MaterializedViewsRemoved)
	sort.Slice(diff.MaterializedViewsModified, func(i, j int) bool {
		return diff.MaterializedViewsModified[i].ViewName < diff.MaterializedViewsModified[j].ViewName
	})
}

// MaterializedViewsWithSemantics compares materialized-view identities using
// the same default-schema semantics as tables and ordinary views.
func MaterializedViewsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize("")
	generatedViews := make(map[tableIdentity]goschema.MaterializedView, len(generated.MaterializedViews))
	generatedNames := make(map[tableIdentity]string, len(generated.MaterializedViews))
	for _, view := range generated.MaterializedViews {
		view.Canonicalize()
		identity := newQualifiedTableIdentity(view.Name, semantics)
		generatedViews[identity] = view
		generatedNames[identity] = view.Name
	}
	databaseViews := make(map[tableIdentity]types.DBMatView, len(database.MatViews))
	databaseNames := make(map[tableIdentity]string, len(database.MatViews))
	for _, view := range database.MatViews {
		identity := newTableIdentity(view.Schema, view.Name, semantics)
		databaseViews[identity] = view
		databaseNames[identity] = view.QualifiedName()
	}

	for identity, generatedView := range generatedViews {
		databaseView, exists := databaseViews[identity]
		if !exists {
			diff.MaterializedViewsAdded = append(diff.MaterializedViewsAdded, generatedNames[identity])
			continue
		}
		viewDiff := MaterializedViewDefinitions(generatedView, databaseView)
		if len(viewDiff.Changes) > 0 {
			diff.MaterializedViewsModified = append(diff.MaterializedViewsModified, viewDiff)
		}
	}
	for identity := range databaseViews {
		if _, exists := generatedViews[identity]; !exists {
			diff.MaterializedViewsRemoved = append(diff.MaterializedViewsRemoved, databaseNames[identity])
		}
	}

	sort.Strings(diff.MaterializedViewsAdded)
	sort.Strings(diff.MaterializedViewsRemoved)
	sort.Slice(diff.MaterializedViewsModified, func(i, j int) bool {
		return diff.MaterializedViewsModified[i].ViewName < diff.MaterializedViewsModified[j].ViewName
	})
}

// FunctionDefinitions performs detailed comparison between generated and database function definitions.
//
// This function compares all aspects of a PostgreSQL function definition to determine
// if the function needs to be recreated due to changes in its definition. PostgreSQL
// functions typically require dropping and recreating when modified.
//
// # Function Properties Compared
//
// The function compares the following properties:
//   - **Parameters**: Function parameter list and types
//   - **Returns**: Return type specification
//   - **Language**: Function language (plpgsql, sql, etc.)
//   - **Security**: Security context (DEFINER vs INVOKER)
//   - **Volatility**: Function volatility (STABLE, IMMUTABLE, VOLATILE)
//   - **Body**: Function implementation code
//
// # Example Scenarios
//
// **Parameter change**:
//   - Generated: "get_user_count(tenant_id TEXT)"
//   - Database: "get_user_count()"
//   - Result: Changes["parameters"] = "() -> (tenant_id TEXT)"
//
// **Body modification**:
//   - Generated: "SELECT COUNT(*) FROM users WHERE tenant_id = $1"
//   - Database: "SELECT COUNT(*) FROM users"
//   - Result: Changes["body"] = "old_body -> new_body"
//
// **Volatility change**:
//   - Generated: STABLE
//   - Database: VOLATILE
//   - Result: Changes["volatility"] = "VOLATILE -> STABLE"
//
// # Parameters
//
//   - genFunction: Generated function definition from Go struct annotations
//   - dbFunction: Current database function from introspection
//
// # Return Value
//
// Returns a FunctionDiff containing:
//   - FunctionName: Name of the function being compared
//   - Changes: Map of property changes in "old -> new" format
//
// # Migration Implications
//
// Function changes typically require:
//  1. DROP FUNCTION (with CASCADE if dependencies exist)
//  2. CREATE OR REPLACE FUNCTION with new definition
func FunctionDefinitions(genFunction goschema.Function, dbFunction types.DBFunction) difftypes.FunctionDiff {
	return FunctionDefinitionsWithDialect(genFunction, dbFunction, "")
}

// FunctionDefinitionsWithDialect compares two function definitions using the
// target's own routine spelling rules.
//
// The dialect is needed because a routine type has two spellings and only the
// target knows they are one type. On the MySQL family the operator writes
// `returns="INTEGER"`, [goschema.Function.Canonicalize] lowercases it to
// `integer`, and information_schema answers `int` -- both engines resolve the
// synonym themselves before recording it. Comparing those exactly reported
// `returns: int -> integer` on a function that already matched, and the planner
// answered with another destructive drop and create, on every inspection.
// Measured on MySQL 26.7.0 and MariaDB 12.3.2, the same declaration also
// produced `parameters: a int -> a integer`.
//
// The normalization runs on BOTH sides through the one function the reader also
// uses, [mysqlroutine.NormalizeType]. Normalizing only the catalog was the
// original defect: it made the two engines agree with each other while leaving
// the desired side speaking a third spelling.
func FunctionDefinitionsWithDialect(
	genFunction goschema.Function,
	dbFunction types.DBFunction,
	dialect string,
) difftypes.FunctionDiff {
	functionDiff := difftypes.FunctionDiff{
		FunctionName: genFunction.Name,
		Changes:      make(map[string]string),
	}

	// Defense-in-depth: canonicalize a local copy. The annotation parser at
	// core/goschema/parser.go already calls Canonicalize, but test code (this
	// package, integration tests) constructs goschema.Function directly with
	// non-canonical case, and a future programmatic API consumer might too.
	// The DB-side read path already returns canonical case by construction,
	// so we only normalize the gen side.
	genFunction.Canonicalize()

	// After Canonicalize, not before: it lowercases Returns and Parameters, and
	// the synonym table this resolves is keyed on the lowercase spelling.
	if isMySQLFamily(dialect) {
		genFunction.Returns = mysqlroutine.NormalizeType(genFunction.Returns)
		genFunction.Parameters = mysqlroutine.NormalizeParameterList(genFunction.Parameters)
		dbFunction.Returns = mysqlroutine.NormalizeType(dbFunction.Returns)
		dbFunction.Parameters = mysqlroutine.NormalizeParameterList(dbFunction.Parameters)
	}

	// Compare parameters
	if genFunction.Parameters != dbFunction.Parameters {
		functionDiff.Changes["parameters"] = fmt.Sprintf("%s -> %s", dbFunction.Parameters, genFunction.Parameters)
	}

	// Compare return type
	if genFunction.Returns != dbFunction.Returns {
		functionDiff.Changes["returns"] = fmt.Sprintf("%s -> %s", dbFunction.Returns, genFunction.Returns)
	}

	// Compare language
	if genFunction.Language != dbFunction.Language {
		functionDiff.Changes["language"] = fmt.Sprintf("%s -> %s", dbFunction.Language, genFunction.Language)
	}

	// Compare security context (DEFINER vs INVOKER)
	if genFunction.Security != dbFunction.Security {
		functionDiff.Changes["security"] = fmt.Sprintf("%s -> %s", dbFunction.Security, genFunction.Security)
	}

	// Compare volatility (VOLATILE/STABLE/IMMUTABLE)
	if genFunction.Volatility != dbFunction.Volatility {
		functionDiff.Changes["volatility"] = fmt.Sprintf("%s -> %s", dbFunction.Volatility, genFunction.Volatility)
	}

	// Compare function body (normalize whitespace for comparison)
	genBody := strings.TrimSpace(genFunction.Body)
	dbBody := strings.TrimSpace(dbFunction.Body)
	if genBody != dbBody {
		functionDiff.Changes["body"] = fmt.Sprintf("%s -> %s", dbBody, genBody)
	}

	return functionDiff
}

// ViewDefinitions performs detailed comparison between generated and database view definitions.
func ViewDefinitions(genView goschema.View, dbView types.DBView) difftypes.ViewDiff {
	return ViewDefinitionsWithDialect(genView, dbView, "")
}

// ViewDefinitionsWithDialect performs detailed comparison between generated and
// database view definitions with dialect-aware catalog readback normalization.
func ViewDefinitionsWithDialect(genView goschema.View, dbView types.DBView, dialect string) difftypes.ViewDiff {
	viewDiff := difftypes.ViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
		// The database body is what the view has before this diff is applied.
		// The planner needs it to decide whether CREATE OR REPLACE VIEW is legal
		// for the change, which is not derivable from the target body alone.
		PreviousBody: strings.TrimSpace(dbView.Body),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body, dialect, dbView.Schema) {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}

	dbWithCheck := !strings.EqualFold(dbView.CheckOption, "") && !strings.EqualFold(dbView.CheckOption, "NONE")
	if genView.WithCheck != dbWithCheck {
		viewDiff.Changes["with_check"] = fmt.Sprintf("%t -> %t", dbWithCheck, genView.WithCheck)
	}

	return viewDiff
}

// MaterializedViewDefinitions performs detailed comparison between generated
// and database materialized view definitions.
func MaterializedViewDefinitions(genView goschema.MaterializedView, dbView types.DBMatView) difftypes.MaterializedViewDiff {
	viewDiff := difftypes.MaterializedViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body, "", "") {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}

	return viewDiff
}

func schemaObjectBodiesEqual(generatedBody, databaseBody, dialect, databaseSchema string) bool {
	if normalizeSQLBodyPreservingQualifiers(generatedBody, dialect) == normalizeSQLBodyPreservingQualifiers(databaseBody, dialect) {
		return true
	}

	if schemaQualifierPattern.MatchString(strings.ToLower(generatedBody)) {
		return false
	}
	return normalizeSQLBodyPreservingQualifiers(generatedBody, dialect) ==
		normalizeSQLBodyStrippingQualifiers(databaseBody, dialect, databaseSchema)
}

func normalizeSQLBodyPreservingQualifiers(body, dialect string) string {
	return canonicalizeNormalizedSQLBody(normalizeSQLBody(body, dialect), dialect)
}

func normalizeSQLBodyStrippingQualifiers(body, dialect, schema string) string {
	return canonicalizeNormalizedSQLBody(stripSQLQualifiers(normalizeSQLBody(body, dialect), schema), dialect)
}

func normalizeSQLBody(body, dialect string) string {
	body = strings.TrimSpace(body)
	body = strings.TrimSuffix(body, ";")
	body = strings.TrimSpace(body)
	body = normalizeSQLCaseAndIdentifierQuotes(body, dialect)
	body = stripDefaultAggregateAliases(body)
	body = collapseWhitespaceOutsideQuotedSQL(body)
	body = normalizeCommaSpacingOutsideQuotedSQL(body)
	return strings.TrimSpace(body)
}

func canonicalizeNormalizedSQLBody(body, dialect string) string {
	body = stripDefaultColumnAliases(body)
	body = stripSimpleComparisonParentheses(body)
	body = regexp.MustCompile(`\s+`).ReplaceAllString(body, " ")
	if isMySQLFamilyDialect(dialect) {
		body = normalizeMySQLBooleanViewPredicates(body)
	}
	return strings.TrimSpace(body)
}

func normalizeMySQLBooleanViewPredicates(body string) string {
	body = replaceSQLLiteralOutsideSingleQuotedSQL(body, " = false", " = 0")
	body = replaceSQLLiteralOutsideSingleQuotedSQL(body, "= false", "= 0")
	body = replaceSQLLiteralOutsideSingleQuotedSQL(body, " = true", " = 1")
	return replaceSQLLiteralOutsideSingleQuotedSQL(body, "= true", "= 1")
}

func stripDefaultAggregateAliases(body string) string {
	return defaultAggregateAliasPattern.ReplaceAllStringFunc(body, func(match string) string {
		parts := defaultAggregateAliasPattern.FindStringSubmatch(match)
		if len(parts) != 4 || parts[1] != parts[3] {
			return match
		}
		return parts[1] + "(" + parts[2] + ")"
	})
}

func stripDefaultColumnAliases(body string) string {
	return defaultColumnAliasPattern.ReplaceAllStringFunc(body, func(match string) string {
		parts := defaultColumnAliasPattern.FindStringSubmatch(match)
		if len(parts) != 3 || parts[1] != parts[2] {
			return match
		}
		return parts[1]
	})
}

func stripSimpleComparisonParentheses(body string) string {
	return simpleComparisonParenthesesPattern.ReplaceAllString(body, "$1")
}
