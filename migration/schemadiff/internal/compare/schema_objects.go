package compare

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/matviewrefresh"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/objectidentity"
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
		// The rule itself is mysqlroutine.IdentityKey, not a ToLower written
		// here, because the declaration validator in core/renderer has to reach
		// the same answer: a pair this folds together is a pair that target
		// cannot host, and it must be refused rather than silently reduced to
		// one by this map.
		return mysqlroutine.IdentityKey(name)
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

	generatedFunctions := make(map[objectIdentity]goschema.Function, len(generated.Functions))
	generatedNames := make(map[objectIdentity]string, len(generated.Functions))
	for _, function := range generated.Functions {
		// qualifiedRoutineIdentityKey, not routineIdentityKey: folding the whole
		// string lowercased the SCHEMA too, while the database loop below folds
		// only the routine name and leaves function.Schema to the identifier
		// semantics. The two sides then disagreed about the schema component of
		// `Sales.Foo`, so an unchanged function was reported as both added and
		// removed and the plan tried to create one that already existed.
		identity := newQualifiedObjectIdentity(objectidentity.KindFunction,
			qualifiedRoutineIdentityKey(function.Name, dialect), semantics)
		generatedFunctions[identity] = function
		generatedNames[identity] = function.Name
	}
	databaseFunctions := make(map[objectIdentity]types.DBFunction, len(database.Functions))
	databaseNames := make(map[objectIdentity]string, len(database.Functions))
	for _, function := range database.Functions {
		identity := newObjectIdentity(objectidentity.KindFunction,
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

	generatedViews := make(map[objectIdentity]goschema.View, len(generated.Views))
	generatedNames := make(map[objectIdentity]string, len(generated.Views))
	for _, view := range generated.Views {
		identity := newQualifiedObjectIdentity(objectidentity.KindView, view.Name, semantics)
		generatedViews[identity] = view
		generatedNames[identity] = view.Name
	}
	databaseViews := make(map[objectIdentity]types.DBView, len(database.Views))
	databaseNames := make(map[objectIdentity]string, len(database.Views))
	for _, view := range database.Views {
		identity := newObjectIdentity(objectidentity.KindView, view.Schema, view.Name, semantics)
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
	MaterializedViewsWithDialect(generated, database, diff, "")
}

// MaterializedViewsWithDialect compares materialized-view definitions with
// dialect-aware body normalization, matching identity the way
// [ViewsWithDialect] does.
//
// The two kinds have to agree about what a name means. A declaration that names
// its object without a schema is the ordinary spelling, and a catalog reports
// every object with one, so matching only on the qualified form makes an
// unchanged object BOTH added and removed. Measured through
// MaterializedViewsWithSemantics with a ClickHouse read, a declaration of
// "user_stats" against a database holding "ptah_test.user_stats":
//
//	MaterializedViewsAdded   = [user_stats]
//	MaterializedViewsRemoved = [ptah_test.user_stats]
//	MaterializedViewsModified = []
//
// The planner answers that with a CREATE before the removal, and ClickHouse
// refuses it -- "Table ... already exists. (TABLE_ALREADY_EXISTS)" -- while the
// plain view beside it, which has matched bare names against a uniquely-named
// database view since #1276, reported nothing at all.
func MaterializedViewsWithDialect(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	generatedViews := make(map[string]goschema.MaterializedView, len(generated.MaterializedViews))
	for _, view := range generated.MaterializedViews {
		view.Canonicalize()
		generatedViews[view.Name] = view
	}

	databaseViewsByName := make(map[string][]types.DBMatView, len(database.MatViews))
	databaseViewsByQualifiedName := make(map[string]types.DBMatView, len(database.MatViews))
	for _, view := range database.MatViews {
		databaseViewsByName[view.Name] = append(databaseViewsByName[view.Name], view)
		databaseViewsByQualifiedName[view.QualifiedName()] = view
	}

	matchedDatabaseViews := make(map[string]struct{}, len(database.MatViews))
	for viewName, generatedView := range generatedViews {
		databaseView, exists := findDatabaseMatViewForGeneratedView(
			generatedView,
			databaseViewsByName,
			databaseViewsByQualifiedName,
		)
		if !exists {
			diff.MaterializedViewsAdded = append(diff.MaterializedViewsAdded, viewName)
			continue
		}

		matchedDatabaseViews[databaseView.QualifiedName()] = struct{}{}
		viewDiff := MaterializedViewDefinitionsWithDialect(generatedView, databaseView, dialect)
		if len(viewDiff.Changes) > 0 {
			diff.MaterializedViewsModified = append(diff.MaterializedViewsModified, viewDiff)
		}
	}

	for _, view := range database.MatViews {
		if _, ok := matchedDatabaseViews[view.QualifiedName()]; ok {
			continue
		}
		diff.MaterializedViewsRemoved = append(diff.MaterializedViewsRemoved, view.QualifiedName())
	}

	sort.Strings(diff.MaterializedViewsAdded)
	sort.Strings(diff.MaterializedViewsRemoved)
	sort.Slice(diff.MaterializedViewsModified, func(i, j int) bool {
		return diff.MaterializedViewsModified[i].ViewName < diff.MaterializedViewsModified[j].ViewName
	})
}

// findDatabaseMatViewForGeneratedView is findDatabaseViewForGeneratedView for
// the other view kind, and deliberately the same rule: a qualified declaration
// matches only the object it names, and a bare one matches a database object of
// that name only when exactly one schema has it. Two schemas holding the same
// name leave the declaration unmatched rather than guessing between them.
func findDatabaseMatViewForGeneratedView(
	generatedView goschema.MaterializedView,
	databaseViewsByName map[string][]types.DBMatView,
	databaseViewsByQualifiedName map[string]types.DBMatView,
) (types.DBMatView, bool) {
	ref, ok := tableref.Parse(generatedView.Name)
	if !ok {
		return types.DBMatView{}, false
	}
	if ref.Qualified {
		view, ok := databaseViewsByQualifiedName[tableref.Canonical(ref.Schema, ref.Name)]
		return view, ok
	}
	candidates := databaseViewsByName[ref.Name]
	if len(candidates) != 1 {
		return types.DBMatView{}, false
	}
	return candidates[0], true
}

// MaterializedViewsWithSemantics compares materialized-view identities using
// the same default-schema semantics as tables and ordinary views.
func MaterializedViewsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize(dialect)
	if semantics.DefaultSchema == "" {
		// No default schema means no rule for which schema owns an unqualified
		// name, so identity falls back to the name-matching the plain views use.
		// ClickHouse is the dialect that reaches this: its connection reports the
		// current database as the schema on every object it reads and leaves
		// DefaultSchema empty, so a declaration written "user_stats" and a
		// readback of "<database>.user_stats" are the same object.
		MaterializedViewsWithDialect(generated, database, diff, dialect)
		return
	}

	generatedViews := make(map[objectIdentity]goschema.MaterializedView, len(generated.MaterializedViews))
	generatedNames := make(map[objectIdentity]string, len(generated.MaterializedViews))
	for _, view := range generated.MaterializedViews {
		view.Canonicalize()
		identity := newQualifiedObjectIdentity(objectidentity.KindMatView, view.Name, semantics)
		generatedViews[identity] = view
		generatedNames[identity] = view.Name
	}
	databaseViews := make(map[objectIdentity]types.DBMatView, len(database.MatViews))
	databaseNames := make(map[objectIdentity]string, len(database.MatViews))
	for _, view := range database.MatViews {
		identity := newObjectIdentity(objectidentity.KindMatView, view.Schema, view.Name, semantics)
		databaseViews[identity] = view
		databaseNames[identity] = view.QualifiedName()
	}

	for identity, generatedView := range generatedViews {
		databaseView, exists := databaseViews[identity]
		if !exists {
			diff.MaterializedViewsAdded = append(diff.MaterializedViewsAdded, generatedNames[identity])
			continue
		}
		viewDiff := MaterializedViewDefinitionsWithDialect(generatedView, databaseView, dialect)
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
	return MaterializedViewDefinitionsWithDialect(genView, dbView, "")
}

// MaterializedViewDefinitionsWithDialect performs detailed comparison between
// generated and database materialized view definitions with dialect-aware
// catalog readback normalization.
//
// The body normalization is the same one ordinary views get, and it is the same
// one for the same reason: a server records the definition it resolved, not the
// text the author wrote. Measured on PostgreSQL 18.4, `pg_get_viewdef` reports a
// body authored as `FROM users` as `FROM analytics.users` for a materialized
// view exactly as it does for a plain one; measured on ClickHouse 26.7.3.19,
// `system.tables.as_select` reports the same body as `FROM mvqual.users`. Both
// spellings mean the object the declaration named, so the schema the catalog
// added is stripped before the two are compared. Without it a no-op comparison
// reported a body change and the planner answered with a drop and a create,
// which on ClickHouse destroys the accumulated rows of a view nobody changed.
//
// RefreshStrategy is not catalog state. The error-returning comparison entry
// point validates the desired strategy before calling this comparator. The
// low-level, no-error comparator still records a mismatch as drift, so an
// unsupported declaration cannot be reported as synchronized merely because a
// reader defaults the field to manual.
func MaterializedViewDefinitionsWithDialect(
	genView goschema.MaterializedView,
	dbView types.DBMatView,
	dialect string,
) difftypes.MaterializedViewDiff {
	viewDiff := difftypes.MaterializedViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body, dialect, dbView.Schema) {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}
	generatedStrategy := matviewrefresh.Canonical(genView.RefreshStrategy)
	databaseStrategy := matviewrefresh.Canonical(dbView.RefreshStrategy)
	if generatedStrategy != databaseStrategy {
		viewDiff.Changes["refresh_strategy"] = fmt.Sprintf("%s -> %s", databaseStrategy, generatedStrategy)
	}

	return viewDiff
}

// schemaObjectBodiesEqual reports whether a declared view or materialized view
// body and the one a catalog read back mean the same thing.
//
// The first comparison is the strict one. The second removes the qualifiers a
// server adds on its own: the object's own schema in front of a relation, and
// the table prefix MySQL puts in front of every column. It is refused outright
// when the declaration itself qualifies a relation, because then the qualifier
// is part of what was declared and a readback spelling it differently is a real
// difference.
//
// "The declaration qualifies a relation" is asked of relation positions only.
// A column prefix -- `u.id` for an alias, `users.id` for a table -- is not a
// schema, and reading it as one made an unchanged declaration report drift.
func schemaObjectBodiesEqual(generatedBody, databaseBody, dialect, databaseSchema string) bool {
	generated := normalizeSQLBody(generatedBody, dialect)
	if canonicalizeNormalizedSQLBody(generated, dialect) ==
		normalizeSQLBodyPreservingQualifiers(databaseBody, dialect) {
		return true
	}

	if bodyQualifiesRelation(generated) {
		return false
	}
	return canonicalizeNormalizedSQLBody(generated, dialect) ==
		normalizeSQLBodyStrippingQualifiers(
			databaseBody,
			dialect,
			databaseSchema,
			singlePartQualifierNames(generated),
		)
}

func normalizeSQLBodyPreservingQualifiers(body, dialect string) string {
	return canonicalizeNormalizedSQLBody(normalizeSQLBody(body, dialect), dialect)
}

func normalizeSQLBodyStrippingQualifiers(
	body, dialect, schema string,
	authored map[string]struct{},
) string {
	return canonicalizeNormalizedSQLBody(
		stripSQLQualifiers(normalizeSQLBody(body, dialect), schema, authored),
		dialect,
	)
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
