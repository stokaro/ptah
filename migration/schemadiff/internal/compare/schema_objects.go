package compare

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/chrefresh"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/oracleroutine"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
func Functions(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff) {
	FunctionsWithDialect(desired, current, diff, "")
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
//
// Oracle folds them too, and there the consequence of not folding is worse than
// a wrong plan. Ptah writes Oracle names WITHOUT quotes, so the server folds
// every one of them to upper case: measured on 23.26.2.0.0, declaring
// `zz_case` and `ZZ_CASE` created ONE function, the second silently replacing
// the first, with USER_OBJECTS holding a single ZZ_CASE row. A comparator that
// kept the two spellings apart would then report the live routine as both added
// and removed, on every run.
func routineIdentityKey(name, dialect string) string {
	if isMySQLFamily(dialect) || isOracle(dialect) {
		// The rule itself is mysqlroutine.IdentityKey, not a ToLower written
		// here, because the declaration validator in core/renderer has to reach
		// the same answer: a pair this folds together is a pair that target
		// cannot host, and it must be refused rather than silently reduced to
		// one by this map.
		return mysqlroutine.IdentityKey(name)
	}
	return name
}

// isOracle reports whether the dialect is the one whose routine rules
// [oracleroutine] describes.
func isOracle(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.Oracle
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
// side folded only [catalog.Function.Name] and left `Schema` exact. The two
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
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
) {
	// Build lookup maps for function comparison
	generatedFunctionMap := make(map[string]schemamodel.Function)
	for _, fn := range desired.Functions {
		generatedFunctionMap[qualifiedRoutineIdentityKey(fn.Name, dialect)] = fn
	}

	// A generated function's name may be schema-qualified -- the HCL parser
	// writes `extra.f` from a `function` block's schema attribute, and so does
	// a database read -- while the reader reports the two parts separately. The
	// two sides are matched the way views are, by qualified name where the
	// generated side carries one and by bare name where it does not, so that a
	// document describing `extra.f` is not compared against `public.f` and a
	// round trip does not plan a redundant CREATE OR REPLACE (stokaro/ptah#1276).
	databaseFunctionsByName := make(map[string][]catalog.Function, len(database.Functions))
	databaseFunctionsByQualifiedName := make(map[string]catalog.Function, len(database.Functions))
	for _, fn := range database.Functions {
		// The kind is part of the key. One schema can hold a procedure and a
		// function of the same name, and folding them together would compare a
		// declared function against a stored procedure (stokaro/ptah#1722).
		key := routineIdentityKey(fn.Name, dialect)
		kinded := routineKeyWithKind(fn.Kind, key)
		databaseFunctionsByName[kinded] = append(databaseFunctionsByName[kinded], fn)
		// The schema keeps its own spelling; only the routine half is folded.
		databaseFunctionsByQualifiedName[routineKeyWithKind(fn.Kind, tableref.Canonical(fn.Schema, key))] = fn
	}

	matchedDatabaseFunctions := make(map[string]struct{}, len(database.Functions))
	for functionKey, generatedFunction := range generatedFunctionMap {
		databaseFunction, exists := findDatabaseFunction(
			generatedFunction.Kind,
			functionKey,
			databaseFunctionsByName,
			databaseFunctionsByQualifiedName,
		)
		if !exists {
			// The desired spelling, never the folded key: the planner resolves
			// this name back to its declaration by exact match, and the
			// rendered DDL must carry what the operator wrote.
			diff.FunctionsAdded = append(diff.FunctionsAdded, declaredRoutine(generatedFunction))
			continue
		}
		// Keyed by kind as well: a procedure and a function can share a
		// qualified name, and a match on one would otherwise mark the other as
		// matched and silently keep it (stokaro/ptah#1722).
		matchedDatabaseFunctions[routineKeyWithKind(databaseFunction.Kind, databaseFunction.QualifiedName())] = struct{}{}
		functionComparison := FunctionDefinitionsWithDialect(generatedFunction, databaseFunction, dialect)
		if len(functionComparison.Changes) > 0 {
			diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
		}
	}

	for _, fn := range database.Functions {
		if _, ok := matchedDatabaseFunctions[routineKeyWithKind(fn.Kind, fn.QualifiedName())]; ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(fn.Kind), "procedure") {
			diff.ProceduresRemoved = append(diff.ProceduresRemoved, reportedRoutine(fn))
			continue
		}
		diff.FunctionsRemoved = append(diff.FunctionsRemoved, reportedRoutine(fn))
	}

	// Ensure consistent ordering of results
	sortRoutines(diff.FunctionsAdded)
	sortRoutines(diff.FunctionsRemoved)
	sortRoutines(diff.ProceduresRemoved)
	sort.Slice(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	})
}

// FunctionsWithSemantics compares function identities using the target
// database's default-schema and identifier rules. The names retained in the
// diff remain the original desired/current spellings so downstream planners
// can resolve the exact source objects they received.
func FunctionsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize("")
	if semantics.DefaultSchema == "" {
		FunctionsWithDialect(desired, database, diff, dialect)
		return
	}

	generatedFunctions := make(map[objectIdentity][]schemamodel.Function, len(desired.Functions))
	generatedNames := make(map[objectIdentity]string, len(desired.Functions))
	for _, function := range desired.Functions {
		// qualifiedRoutineIdentityKey, not routineIdentityKey: folding the whole
		// string lowercased the SCHEMA too, while the database loop below folds
		// only the routine name and leaves function.Schema to the identifier
		// semantics. The two sides then disagreed about the schema component of
		// `Sales.Foo`, so an unchanged function was reported as both added and
		// removed and the plan tried to create one that already existed.
		identity := newQualifiedObjectIdentity(routineIdentityKind(function.Kind),
			qualifiedRoutineIdentityKey(function.Name, dialect), semantics)
		generatedFunctions[identity] = append(generatedFunctions[identity], function)
		generatedNames[identity] = function.Name
	}
	databaseFunctions := make(map[objectIdentity][]catalog.Function, len(database.Functions))
	databaseNames := make(map[objectIdentity]string, len(database.Functions))
	for _, function := range database.Functions {
		identity := newObjectIdentity(routineIdentityKind(function.Kind),
			function.Schema, routineIdentityKey(function.Name, dialect), semantics)
		databaseFunctions[identity] = append(databaseFunctions[identity], function)
		databaseNames[identity] = function.QualifiedName()
	}

	for identity, declared := range generatedFunctions {
		recorded := databaseFunctions[identity]
		pairs, added, _ := pairRoutineOverloads(declared, recorded)
		// Each added overload carries ITS OWN declaration. Appending the shared
		// name once per unmatched overload made the planner resolve the same
		// declaration every time, so one overload was created twice and the
		// other never (stokaro/ptah#2408).
		for _, declaration := range added {
			diff.FunctionsAdded = append(diff.FunctionsAdded,
				declaredRoutineNamed(declaration, generatedNames[identity]))
		}
		for _, pair := range pairs {
			functionComparison := FunctionDefinitionsWithDialect(pair.declared, pair.recorded, dialect)
			if len(functionComparison.Changes) > 0 {
				diff.FunctionsModified = append(diff.FunctionsModified, functionComparison)
			}
		}
	}
	for identity, recorded := range databaseFunctions {
		_, _, removed := pairRoutineOverloads(generatedFunctions[identity], recorded)
		for _, routine := range removed {
			// The kind travels with the removal because it cannot be recovered
			// later: there is no declaration left to read it off, and the DROP
			// verb has to match the object (stokaro/ptah#1722).
			//
			// So does the signature. A name alone does not select an overload:
			// `DROP FUNCTION IF EXISTS f` is refused with
			// `function name "f" is not unique` whenever the schema holds more
			// than one, and IF EXISTS does not help because the refusal is
			// about ambiguity rather than existence (stokaro/ptah#2296).
			removal := difftypes.RoutineRemoval{
				Name:      databaseNames[identity],
				Signature: recordedRoutineSignature(routine),
			}
			if identity.Kind() == objectidentity.KindProcedure {
				diff.ProceduresRemoved = append(diff.ProceduresRemoved, routineFromRemoval(removal))
				continue
			}
			diff.FunctionsRemoved = append(diff.FunctionsRemoved, routineFromRemoval(removal))
		}
	}

	sortRoutines(diff.FunctionsAdded)
	sortRoutines(diff.FunctionsRemoved)
	sortRoutines(diff.ProceduresRemoved)
	sort.Slice(diff.FunctionsModified, func(i, j int) bool {
		return diff.FunctionsModified[i].FunctionName < diff.FunctionsModified[j].FunctionName
	})
}

// canonicalizePostgresArguments maps each argument's type onto the spelling
// format_type emits, leaving the argument's NAME alone.
//
// The rule is narrow on purpose: it rewrites the last word before any
// parenthesized modifier, and nothing else. In PostgreSQL's parameter syntax an
// argument is `type`, `name type` or `mode name type`, so that word is always
// part of the type and never the name — a parameter called `float8` with type
// `integer` reads as `float8 integer` and is left untouched, which a rule that
// mapped any alias-looking token would have renamed.
//
//	float8                 -> double precision
//	a float8               -> a double precision
//	a decimal(10, 2)       -> a numeric(10,2)
//	a double precision     -> a double precision
//	float8 integer         -> float8 integer
func canonicalizePostgresArguments(parameters string) string {
	if parameters == "" {
		return parameters
	}
	arguments := splitTopLevelArguments(parameters)
	for i, argument := range arguments {
		arguments[i] = canonicalizePostgresArgument(argument)
	}
	return strings.Join(arguments, ", ")
}

// canonicalizePostgresArgument canonicalizes one argument's type.
func canonicalizePostgresArgument(argument string) string {
	trimmed := strings.TrimSpace(argument)
	head, modifier := trimmed, ""
	if i := strings.IndexByte(trimmed, '('); i >= 0 {
		head = strings.TrimSpace(trimmed[:i])
		modifier = strings.ReplaceAll(trimmed[i:], " ", "")
	}
	fields := strings.Fields(head)
	if len(fields) == 0 {
		return trimmed
	}
	last := len(fields) - 1
	if canonical, isAlias := pgTypeAliases[strings.ToLower(fields[last])]; isAlias {
		fields[last] = canonical
	}
	return strings.Join(fields, " ") + modifier
}

// foldArgumentMode reduces each argument's mode to a single spelling.
//
// IN is removed outright: it is the default the grammar supplies when no mode
// is written, so dropping it compares two spellings of one argument. OUT, INOUT
// and VARIADIC change what the argument is and are kept (stokaro/ptah#1722) --
// but kept in ONE case, because the two sides of this comparison do not agree
// on it and never did.
//
// The generated side has been through [schemamodel.Function.Canonicalize], which
// lower-cases the whole parameter list. The database side is whatever the
// catalog printed, and the catalogs print upper case. Measured on PostgreSQL 17
// and MySQL 9.7.2:
//
//	pg_get_function_arguments  a integer, OUT b integer
//	                           IN a integer, INOUT c integer
//	information_schema         OUT b int, INOUT c int
//
// Comparing those verbatim reported "parameters" as a difference between a
// database and its OWN description, so `ptah schema apply` planned to replace a
// routine with an identical one on every run -- for a procedure, and equally
// for the plain function with an OUT argument that has nothing to do with
// procedures (stokaro/ptah#2209).
//
// The fold stays in the comparison. Rendering the folded form instead would
// write Ptah's normalization into the operator's own DDL.
func foldArgumentMode(parameters string) string {
	if parameters == "" {
		return parameters
	}
	arguments := splitTopLevelArguments(parameters)
	for i, argument := range arguments {
		trimmed := strings.TrimSpace(argument)
		// Cut on the first space: a mode is one leading word, and an argument
		// with no mode at all leaves its first word unmatched below.
		first, rest, _ := strings.Cut(trimmed, " ")
		switch strings.ToLower(first) {
		case "in":
			trimmed = strings.TrimSpace(rest)
		case "out", "inout", "variadic":
			trimmed = strings.ToLower(first) + " " + strings.TrimSpace(rest)
		}
		arguments[i] = trimmed
	}
	return strings.Join(arguments, ", ")
}

// splitTopLevelArguments splits an argument list on the commas that separate
// arguments, leaving the ones inside a type's own parentheses alone --
// `numeric(10, 2)` is one argument, not two.
func splitTopLevelArguments(parameters string) []string {
	var arguments []string
	depth, start := 0, 0
	for i, r := range parameters {
		switch r {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				arguments = append(arguments, parameters[start:i])
				start = i + 1
			}
		}
	}
	return append(arguments, parameters[start:])
}

// routineKeyWithKind prefixes a routine key with its kind, so a procedure and a
// function of the same name are two entries rather than one.
func routineKeyWithKind(kind, key string) string {
	if strings.EqualFold(strings.TrimSpace(kind), "procedure") {
		return "procedure\x00" + key
	}
	return "function\x00" + key
}

// routineIdentityKind separates a procedure from a function of the same name.
//
// Both engines that model procedures let one schema hold both -- MySQL keys
// information_schema.PARAMETERS by SPECIFIC_NAME for exactly that reason -- so
// folding them onto one identity would pair a declared function against a
// stored procedure and report the difference between two different objects as a
// change to one (stokaro/ptah#1722).
func routineIdentityKind(kind string) objectidentity.Kind {
	if strings.EqualFold(strings.TrimSpace(kind), "procedure") {
		return objectidentity.KindProcedure
	}
	return objectidentity.KindFunction
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
	kind, name string,
	byName map[string][]catalog.Function,
	byQualifiedName map[string]catalog.Function,
) (catalog.Function, bool) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return catalog.Function{}, false
	}
	// The kind joins the key after parsing, never before: a kind-prefixed name
	// is not a name tableref can read (stokaro/ptah#1722).
	if ref.Qualified {
		fn, ok := byQualifiedName[routineKeyWithKind(kind, tableref.Canonical(ref.Schema, ref.Name))]
		return fn, ok
	}
	candidates := byName[routineKeyWithKind(kind, ref.Name)]
	if len(candidates) != 1 {
		return catalog.Function{}, false
	}
	return candidates[0], true
}

// Synonyms compares declared synonyms against the ones the database reports.
//
// The comparison is on the qualified name, and a changed target is reported as
// a modification rather than as a removal plus an addition. T-SQL has no ALTER
// SYNONYM, so both shapes end up as a drop and a create -- but only the
// modification says the two belong together, which is what lets a plan order
// them as one operation and a reader tell a retarget from a coincidence.
func Synonyms(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	generatedSynonyms := make(map[string]schemamodel.Synonym, len(desired.Synonyms))
	for _, synonym := range desired.Synonyms {
		generatedSynonyms[synonym.QualifiedName()] = synonym
	}
	databaseSynonyms := make(map[string]catalog.Synonym, len(database.Synonyms))
	for _, synonym := range database.Synonyms {
		databaseSynonyms[synonym.QualifiedName()] = synonym
	}

	for name, generatedSynonym := range generatedSynonyms {
		databaseSynonym, exists := databaseSynonyms[name]
		if !exists {
			diff.SynonymsAdded = append(diff.SynonymsAdded, generatedSynonym)
			continue
		}
		if !sameSynonymTarget(generatedSynonym.Target, databaseSynonym) {
			diff.SynonymsModified = append(diff.SynonymsModified, difftypes.SynonymDiff{
				SynonymName: name,
				OldTarget:   databaseSynonym.Target,
				NewTarget:   generatedSynonym.Target,
			})
		}
	}

	for name, databaseSynonym := range databaseSynonyms {
		if _, ok := generatedSynonyms[name]; ok {
			continue
		}
		// A desired state that could not have named this synonym has not
		// withheld it, and no document format Ptah reads can name one: HCL has
		// no block, YAML has no key, and the SQL parser's conversion produces
		// none even for a `CREATE SYNONYM` it parses. So `schema inspect` into
		// a file and `schema apply --to` that file planned DROP SYNONYM for
		// every synonym on the server, through Ptah's own output
		// (stokaro/ptah#1031). A Go schema CAN declare one, records nothing
		// here, and still removes.
		if !cov.PlansRemoval(coverage.Synonym, databaseSynonym.Schema, databaseSynonym.Name, name) {
			continue
		}
		diff.SynonymsRemoved = append(diff.SynonymsRemoved, synonymFromCatalog(databaseSynonym))
	}

	sortSynonyms(diff.SynonymsAdded)
	sortSynonyms(diff.SynonymsRemoved)
	sort.Slice(diff.SynonymsModified, func(i, j int) bool {
		return diff.SynonymsModified[i].SynonymName < diff.SynonymsModified[j].SynonymName
	})
}

// sameSynonymTarget compares a declared target against the catalog's own
// spelling of one.
//
// SQL Server records base_object_name with its own bracket quoting, so the
// declared `dbo.orders` and the stored `[dbo].[orders]` are the same target
// written two ways. Comparing the raw strings would report a modification on
// every run and make the plan churn forever, which is the false-convergence
// failure this object was added to remove rather than to introduce.
func sameSynonymTarget(declared string, stored catalog.Synonym) bool {
	return synonymTargetParts(declared) == synonymTargetParts(stored.Target)
}

// synonymTargetParts normalizes a one-to-four part name for comparison by
// stripping bracket quoting and folding case, which is what SQL Server's
// default collation does for identifiers.
func synonymTargetParts(name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "[")
		part = strings.TrimSuffix(part, "]")
		parts[i] = strings.ToLower(part)
	}
	return strings.Join(parts, ".")
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
func FunctionDefinitions(genFunction schemamodel.Function, dbFunction catalog.Function) difftypes.FunctionDiff {
	return FunctionDefinitionsWithDialect(genFunction, dbFunction, "")
}

// FunctionDefinitionsWithDialect compares two function definitions using the
// target's own routine spelling rules.
//
// The dialect is needed because a routine type has two spellings and only the
// target knows they are one type. On the MySQL family the operator writes
// `returns="INTEGER"`, [schemamodel.Function.Canonicalize] lowercases it to
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
	genFunction schemamodel.Function,
	dbFunction catalog.Function,
	dialect string,
) difftypes.FunctionDiff {
	functionDiff := difftypes.FunctionDiff{
		FunctionName: genFunction.Name,
		Changes:      make(map[string]string),
	}

	// Defense-in-depth: canonicalize a local copy. The annotation parser at
	// core/goschema/parser.go already calls Canonicalize, but test code (this
	// package, integration tests) constructs schemamodel.Function directly with
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

	// The argument mode is folded on both sides, and only here. IN is the
	// default, so `n integer` and `IN n integer` are the same argument -- but
	// PostgreSQL prints the mode for a procedure's arguments and omits it for a
	// function's, measured on 18.4:
	//
	//	pg_get_function_arguments(procedure) -> IN n integer
	//	pg_get_function_arguments(function)  -> n integer
	//
	// so a declaration written the ordinary way never converged against a
	// procedure. The fold also settles the CASE of the modes it keeps, which
	// the two sides spell differently; see [foldArgumentMode].
	genFunction.Parameters = foldArgumentMode(genFunction.Parameters)
	dbFunction.Parameters = foldArgumentMode(dbFunction.Parameters)

	// PL/SQL writes the mode AFTER the name -- `p IN NUMBER` -- so the fold
	// above, which cuts a leading `in `, never reaches it. The default is the
	// same default, and both spellings are ordinary in a declaration, so the
	// same reasoning applies in the other word order and the same rule is
	// applied to both sides.
	if isOracle(dialect) {
		genFunction.Parameters = oracleroutine.FoldDefaultArgumentMode(genFunction.Parameters)
		dbFunction.Parameters = oracleroutine.FoldDefaultArgumentMode(dbFunction.Parameters)
	}

	// A PostgreSQL type has aliases, and the two sides do not use the same
	// ones: `float8` is what a server accepts and `double precision` is what
	// it reports back. Compared as text, such a declaration plans
	// CREATE OR REPLACE on every run, applies it, changes nothing -- the
	// function is already what the statement says -- and plans it again. The
	// range subtype and the domain base type already go through this
	// canonicalization; parameters did not (stokaro/ptah#2273).
	if platform.IsPostgresFamily(dialect) {
		genFunction.Parameters = canonicalizePostgresArguments(genFunction.Parameters)
		dbFunction.Parameters = canonicalizePostgresArguments(dbFunction.Parameters)
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
func ViewDefinitions(genView schemamodel.View, dbView catalog.View) difftypes.ViewDiff {
	return ViewDefinitionsWithDialect(genView, dbView, "")
}

// ViewDefinitionsWithDialect performs detailed comparison between generated and
// database view definitions with dialect-aware catalog readback normalization.
func ViewDefinitionsWithDialect(genView schemamodel.View, dbView catalog.View, dialect string) difftypes.ViewDiff {
	viewDiff := difftypes.ViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
		// The declaration this change leaves behind. The reversal replaces it
		// with the database's own view, because a rollback restores that one
		// (stokaro/ptah#2315).
		Desired: genView,
		// The database body is what the view has before this diff is applied.
		// The planner needs it to decide whether CREATE OR REPLACE VIEW is legal
		// for the change, which is not derivable from the target body alone.
		PreviousBody: strings.TrimSpace(dbView.Body),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body, dialect, dbView.Schema) {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}

	// The third place this rule was written, and the one #2315's view
	// conversion missed: the same words decide whether a view asks for
	// WITH CHECK OPTION, and a copy that answers differently is what
	// sqlutil.CheckOptionRequestsCheck exists to prevent.
	dbWithCheck := sqlutil.CheckOptionRequestsCheck(dbView.CheckOption)
	if genView.WithCheck != dbWithCheck {
		viewDiff.Changes["with_check"] = fmt.Sprintf("%t -> %t", dbWithCheck, genView.WithCheck)
	}

	// The view's own WITH clause. A view that gains or loses SCHEMABINDING is a
	// different view -- it is what binds it to the tables it names, and an
	// indexed view requires it -- so a change here has to be planned rather
	// than tolerated (stokaro/ptah#2125).
	if !sameViewAttributes(genView.Attributes, dbView.Attributes) {
		viewDiff.Changes["attributes"] = fmt.Sprintf("%s -> %s",
			renderViewAttributes(dbView.Attributes), renderViewAttributes(genView.Attributes))
	}

	return viewDiff
}

// sameViewAttributes compares two WITH clauses as SETS.
//
// `WITH SCHEMABINDING, VIEW_METADATA` and `WITH VIEW_METADATA, SCHEMABINDING`
// are one view, and the server does not promise the order it reports. Comparing
// the lists as written would plan a change for a document that says the same
// thing in the other order, on every run.
//
// Case is folded for the same reason: the catalog echoes the text the author
// wrote, so a lowercase declaration and an uppercase readback are the same
// clause.
func sameViewAttributes(declared, actual []string) bool {
	return slices.Equal(normalizedViewAttributes(declared), normalizedViewAttributes(actual))
}

// normalizedViewAttributes upper-cases, trims, drops empties and sorts.
func normalizedViewAttributes(attributes []string) []string {
	normalized := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		if trimmed := strings.ToUpper(strings.TrimSpace(attribute)); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	slices.Sort(normalized)
	return normalized
}

// renderViewAttributes spells a clause for a change message, with a word for
// the empty case so a failure reads as a sentence rather than as an arrow with
// nothing on one side.
func renderViewAttributes(attributes []string) string {
	normalized := normalizedViewAttributes(attributes)
	if len(normalized) == 0 {
		return "(none)"
	}
	return strings.Join(normalized, ", ")
}

// MaterializedViewDefinitions performs detailed comparison between generated
// and database materialized view definitions.
func MaterializedViewDefinitions(genView schemamodel.MaterializedView, dbView catalog.MaterializedView) difftypes.MaterializedViewDiff {
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
	genView schemamodel.MaterializedView,
	dbView catalog.MaterializedView,
	dialect string,
) difftypes.MaterializedViewDiff {
	viewDiff := difftypes.MaterializedViewDiff{
		ViewName: genView.Name,
		Changes:  make(map[string]string),
	}

	if !schemaObjectBodiesEqual(genView.Body, dbView.Body, dialect, dbView.Schema) {
		viewDiff.Changes["body"] = fmt.Sprintf("%s -> %s", strings.TrimSpace(dbView.Body), strings.TrimSpace(genView.Body))
	}
	if desired, current, changed := refreshChange(genView, dbView); changed {
		viewDiff.Changes["refresh"] = fmt.Sprintf("%s -> %s", refreshText(current), refreshText(desired))
		viewDiff.RefreshChange = &difftypes.MatViewRefreshChange{
			Desired: desired.Clone(),
			Current: current.Clone(),
		}
	}

	return viewDiff
}

// refreshChange compares the declared ClickHouse refresh schedule with the one
// read back, and returns the change to report or "" when they agree.
//
// The declaration is canonicalized first, and that is the whole reason
// [go.5x5.cz/ptah/internal/chrefresh] exists: the server rewrites what it
// stores, so `EVERY 60 MINUTE` reads back as `EVERY 1 HOUR`. Comparing the two
// as written would report a change on every run and plan a drop and a create
// for it -- on an object whose drop takes every row it accumulated
// (stokaro/ptah#1802).
//
// A declaration the canonicalizer refuses reports no change rather than a
// wrong one. Refusing a declaration is the renderer's job and it does it with
// the reason; a comparison that invented a difference here would plan work for
// a schedule that is never going to be sent.
func refreshChange(
	genView schemamodel.MaterializedView,
	dbView catalog.MaterializedView,
) (desired, current *ast.MatViewRefreshSpec, changed bool) {
	desired, err := chrefresh.Canonical(genView.Refresh, dbView.Schema)
	if err != nil {
		return nil, nil, false
	}
	if chrefresh.Equal(desired, dbView.Refresh) {
		return nil, nil, false
	}
	return desired, dbView.Refresh, true
}

// refreshText names a schedule for a diff entry, and names its absence too: a
// view gaining or losing one is a change, and "" on one side would read as a
// missing value rather than as a plain view.
func refreshText(spec *ast.MatViewRefreshSpec) string {
	if spec == nil {
		return "(none)"
	}
	return chrefresh.Clause(spec)
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
	desired := normalizeSQLBody(generatedBody, dialect)
	if canonicalizeNormalizedSQLBody(desired, dialect) ==
		normalizeSQLBodyPreservingQualifiers(databaseBody, dialect) {
		return true
	}

	if bodyQualifiesRelation(desired) {
		return false
	}
	return canonicalizeNormalizedSQLBody(desired, dialect) ==
		normalizeSQLBodyStrippingQualifiers(
			databaseBody,
			dialect,
			databaseSchema,
			singlePartQualifierNames(desired),
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

// synonymFromCatalog carries a synonym the database reported into the shape the
// diff holds.
//
// Only the target needs a rule, and it is catalog.Synonym.DeclaredTarget --
// shared with the conversion path rather than spelled a second time here,
// because the two are answering the same question about the same row.
func synonymFromCatalog(reported catalog.Synonym) schemamodel.Synonym {
	return schemamodel.Synonym{
		Name:    reported.Name,
		Schema:  reported.Schema,
		Target:  reported.DeclaredTarget(),
		Comment: reported.Comment,
	}
}

// sortSynonyms orders by the key the name list was sorted on.
func sortSynonyms(synonyms difftypes.SynonymChanges) {
	sort.Slice(synonyms, func(i, j int) bool {
		return synonyms[i].QualifiedName() < synonyms[j].QualifiedName()
	})
}

// declaredRoutine carries a routine the desired schema declares.
//
// A declaration has no drop identity: nothing is being dropped, and the
// signature a DROP would need is the SERVER's canonical argument list, which
// only a read supplies. See [difftypes.RoutineChange].
func declaredRoutine(declared schemamodel.Function) difftypes.RoutineChange {
	return difftypes.RoutineChange{Function: declared}
}

// declaredRoutineNamed is declaredRoutine for the overload-aware path, where
// the name a diff carries is the desired spelling rather than the declaration's
// own -- the planner resolves it back by exact match.
func declaredRoutineNamed(declared schemamodel.Function, name string) difftypes.RoutineChange {
	declared.Name = name
	return difftypes.RoutineChange{Function: declared}
}

// reportedRoutine carries a routine the database reported, with the argument
// list a DROP addresses it by.
//
// recordedRoutineSignature prefers the catalog's own identity arguments and
// falls back to the declaration parameters, which is the same answer the
// overload-aware path records.
func reportedRoutine(reported catalog.Function) difftypes.RoutineChange {
	return difftypes.RoutineChange{
		Function: schemamodel.Function{
			Name:       reported.QualifiedName(),
			Kind:       reported.Kind,
			Parameters: reported.Parameters,
			Returns:    reported.Returns,
			Language:   reported.Language,
			Security:   reported.Security,
			Volatility: reported.Volatility,
			Body:       reported.Body,
			Comment:    reported.Comment,
		},
		Signature: recordedRoutineSignature(reported),
	}
}

// routineFromRemoval carries what the overload pairing already resolved.
func routineFromRemoval(removal difftypes.RoutineRemoval) difftypes.RoutineChange {
	return difftypes.RoutineChange{
		Function:  schemamodel.Function{Name: removal.Name},
		Signature: removal.Signature,
	}
}

// sortRoutines orders by the key the name list was sorted on.
func sortRoutines(routines difftypes.FunctionChanges) {
	sort.Slice(routines, func(i, j int) bool { return routines[i].Name < routines[j].Name })
}
