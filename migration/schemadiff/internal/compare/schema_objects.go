package compare

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
	// Build lookup maps for function comparison
	generatedFunctionMap := make(map[string]goschema.Function)
	for _, fn := range generated.Functions {
		generatedFunctionMap[fn.Name] = fn
	}

	databaseFunctionMap := make(map[string]types.DBFunction)
	for _, fn := range database.Functions {
		databaseFunctionMap[fn.Name] = fn
	}

	// Use generic comparison helper for add/remove detection
	addedFunctions, removedFunctions := compareNamedItems(generatedFunctionMap, databaseFunctionMap)
	diff.FunctionsAdded = append(diff.FunctionsAdded, addedFunctions...)
	diff.FunctionsRemoved = append(diff.FunctionsRemoved, removedFunctions...)

	// Detect function definition modifications
	for functionName, generatedFunction := range generatedFunctionMap {
		if databaseFunction, functionExists := databaseFunctionMap[functionName]; functionExists {
			functionComparison := FunctionDefinitions(generatedFunction, databaseFunction)
			if len(functionComparison.Changes) > 0 {
				diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
			}
		}
	}

	// Ensure consistent ordering of results
	sort.Strings(diff.FunctionsAdded)
	sort.Strings(diff.FunctionsRemoved)
	sort.Slice(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	})
}

// Views compares view definitions between generated and database schemas.
func Views(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedViews := make(map[string]goschema.View)
	for _, view := range generated.Views {
		generatedViews[view.Name] = view
	}

	databaseViews := make(map[string]types.DBView)
	for _, view := range database.Views {
		databaseViews[view.QualifiedName()] = view
	}

	addedViews, removedViews := compareNamedItems(generatedViews, databaseViews)
	diff.ViewsAdded = append(diff.ViewsAdded, addedViews...)
	diff.ViewsRemoved = append(diff.ViewsRemoved, removedViews...)

	for viewName, generatedView := range generatedViews {
		if databaseView, exists := databaseViews[viewName]; exists {
			viewDiff := ViewDefinitions(generatedView, databaseView)
			if len(viewDiff.Changes) > 0 {
				diff.ViewsModified = append(diff.ViewsModified, viewDiff)
			}
		}
	}

	sort.Strings(diff.ViewsAdded)
	sort.Strings(diff.ViewsRemoved)
	sort.Slice(diff.ViewsModified, func(i, j int) bool {
		return diff.ViewsModified[i].ViewName < diff.ViewsModified[j].ViewName
	})
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
	viewDiff := difftypes.ViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body) {
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

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body) {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}

	return viewDiff
}

func schemaObjectBodiesEqual(generatedBody, databaseBody string) bool {
	if normalizeSQLBodyPreservingQualifiers(generatedBody) == normalizeSQLBodyPreservingQualifiers(databaseBody) {
		return true
	}

	if schemaQualifierPattern.MatchString(strings.ToLower(generatedBody)) {
		return false
	}
	return normalizeSQLBodyPreservingQualifiers(generatedBody) == normalizeSQLBodyStrippingQualifiers(databaseBody)
}

func normalizeSQLBodyPreservingQualifiers(body string) string {
	return normalizeSQLBody(body)
}

func normalizeSQLBodyStrippingQualifiers(body string) string {
	return schemaQualifierPattern.ReplaceAllString(normalizeSQLBody(body), "")
}

func normalizeSQLBody(body string) string {
	body = strings.TrimSpace(body)
	body = strings.TrimSuffix(body, ";")
	body = strings.TrimSpace(body)
	body = strings.ReplaceAll(body, "\"", "")
	body = strings.ReplaceAll(body, "`", "")
	body = strings.ToLower(body)
	body = stripDefaultAggregateAliases(body)
	body = regexp.MustCompile(`\s+`).ReplaceAllString(body, " ")
	return strings.TrimSpace(body)
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
