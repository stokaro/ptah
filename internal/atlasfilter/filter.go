// Package atlasfilter applies Atlas-style resource filters to introspected schemas.
package atlasfilter

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/featureselect"
	"ptah.run/internal/indexbacking"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// ExcludeDatabase returns a shallow copy of schema with resources matching
// Atlas-style exclude globs removed. Objects that carry no schema are treated
// as unqualified; use [ExcludeDatabaseWithDefaultSchema] when the connection
// names the schema those objects live in.
func ExcludeDatabase(selections selectext.Set, schema *catalog.Database, patterns []string) (*catalog.Database, error) {
	return ExcludeDatabaseWithDefaultSchema(selections, schema, patterns, "")
}

// ExcludeDatabaseWithDefaultSchema is [ExcludeDatabase] told which schema owns
// the objects introspection left unqualified.
//
// Every reader blanks the schema of objects in the connection's own schema, so
// without the default a table in it exposes only its bare name as a match
// candidate and the schema-qualified spelling of the very same object matches
// nothing. That miss is silent — exclude patterns have no match requirement —
// and on `schema apply` and `schema diff` it reaches a DROP: the user writes
// the qualified spelling to protect an object and the plan destroys it.
func ExcludeDatabaseWithDefaultSchema(
	selections selectext.Set,
	schema *catalog.Database,
	patterns []string,
	defaultSchema string,
) (*catalog.Database, error) {
	filtered, _, err := ExcludeDatabaseReport(selections, schema, patterns, defaultSchema)
	return filtered, err
}

// ExcludeReport records what an exclude run observed beyond the filtering
// itself.
//
// The one thing worth observing is emptiness. An exclude pattern has no match
// requirement, so a selector that names nothing subtracts nothing and the
// command succeeds with the object still in its output — the failure mode this
// report exists to make visible. A scoping flag that fails open is worse than
// one that refuses: someone excluding an object is excluding it for a reason,
// and silently keeping it defeats that reason with no diagnostic.
type ExcludeReport struct {
	// Unmatched lists the exclude selectors, as written, that named no object
	// and excluded no whole readable kind in the state they were applied to.
	// The selectors here are well formed, but had no effect.
	Unmatched []string
	// Stored maps a selector of Unmatched to the spelling that names an
	// object in this state, on a target whose descriptor states an
	// UnquotedFold: the selector with its letters, outside its brackets, in
	// the case the catalog stores a bare name in. A selector meets the stored
	// name as written, so on Oracle items names nothing where ITEMS names the
	// table a bare items declares. Nil where no such spelling names anything.
	Stored map[string]string
}

// ExcludeDatabaseReport is [ExcludeDatabaseWithDefaultSchema] plus the report.
// The filtering is identical; only the extra return value is new, so callers
// that do not care about emptiness keep the two-value form.
func ExcludeDatabaseReport(
	selections selectext.Set,
	schema *catalog.Database,
	patterns []string,
	defaultSchema string,
) (*catalog.Database, ExcludeReport, error) {
	return excludeDatabase(selections, schema, patterns, defaultSchema, defaultSchema, databaseTarget{}, featureselect.Bindings{})
}

// ExcludeDatabaseScopeReport is [ExcludeDatabaseReport] for a caller holding
// the whole scope.
//
// The positional form counts a pattern's parts against the same schema that
// owns unqualified objects, because a string is all it has. That is right for a
// run limited to one schema and wrong for one describing the realm, where the
// pattern's leading segment is a schema name -- see
// [Scope.RealmRelativePatterns].
func ExcludeDatabaseScopeReport(
	schema *catalog.Database,
	scope Scope,
) (*catalog.Database, ExcludeReport, error) {
	return excludeDatabase(scope.Selections, schema, scope.Exclude, scope.patternScopeSchema(), scope.DefaultSchema,
		databaseTarget{dialect: scope.Dialect, foreignKeyIndexes: scope.ForeignKeyIndexes}, scope.Bindings)
}

// databaseTarget is what the database side of an exclusion reads about the
// target its state was read from. The zero value is a target nothing describes,
// whose server builds no index for a foreign key.
type databaseTarget struct {
	dialect           string
	foreignKeyIndexes schemacomparison.ForeignKeyIndexes
}

func excludeDatabase(
	selections selectext.Set,
	schema *catalog.Database,
	patterns []string,
	patternScope, defaultSchema string,
	target databaseTarget,
	bindings featureselect.Bindings,
) (*catalog.Database, ExcludeReport, error) {
	if !selections.Selected() {
		return nil, ExcludeReport{}, selectext.ErrUnselected
	}
	words := vocabulary{selections: selections}
	filters, err := parsePatterns(patterns, resolvedDepthScope(patternScope), words)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if schema == nil || len(filters) == 0 {
		return schema, unfilteredReport(filters), nil
	}

	state := newExclusionState(filters, defaultSchema, words)
	state.dialect = target.dialect
	state.foldSelectors(unquotedFold(target.dialect))
	filtered := cloneDatabase(schema)
	// Schemas go first: every filter below asks whether its object's schema
	// left, and that answer has to exist before the first object is judged.
	filtered.Schemas = state.filterSchemas(filtered.Schemas)
	filtered.Tables = state.filterTables(filtered.Tables)
	state.excludeObservedRowSecurity(filtered.Tables)
	state.noteOwnedObjectSelectors(filtered.FeatureObjects)
	filtered.FeatureObjects, filtered.FeatureCoverage, err = bindings.Select(filtered.FeatureObjects, filtered.FeatureCoverage, func(schema, table string) bool {
		return !state.tableExcluded(schema, table)
	}, state.vocabulary.independentKinds()...)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	filtered.FeatureObjects, filtered.FeatureCoverage, err = state.filterOwnerFeatures(filtered.FeatureObjects, filtered.FeatureCoverage)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if filtered.FeatureObjects, err = state.clearOwnerFields(filtered.FeatureObjects); err != nil {
		return nil, ExcludeReport{}, err
	}
	state.excludeColumnSequences(schema.Tables)
	filtered.Enums = state.filterEnums(filtered.Enums)
	filtered.Sequences = state.filterSequences(filtered.Sequences)
	// Constraints go before indexes: an index leaves with the constraint it
	// enforces, so the constraints that left have to be known first.
	var removedConstraints []catalog.Constraint
	filtered.Constraints, removedConstraints = state.filterConstraints(filtered.Constraints)
	removedConstraints = append(removedConstraints, state.removedHiddenKeys(schema)...)
	filtered.Indexes = state.filterIndexes(filtered.Indexes, removedConstraints, excludedObjects(schema.FeatureObjects, filtered.FeatureObjects))
	filtered.Indexes = state.withoutForeignKeyBackingIndexes(filtered.Indexes, schema, removedConstraints, target)
	clearRemovedKeyColumns(filtered.Tables, filtered.Constraints, removedConstraints)
	filtered.FeatureObjects, filtered.FeatureCoverage, err = state.filterExtensionFeatures(filtered.FeatureObjects, filtered.FeatureCoverage)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	filtered.Functions = state.filterFunctions(filtered.Functions)
	filtered.Views = state.filterViews(filtered.Views)
	filtered.MatViews = state.filterMatViews(filtered.MatViews)
	filtered.Triggers = state.filterTriggers(filtered.Triggers)
	filtered.RLSPolicies = state.filterRLSPolicies(filtered.RLSPolicies)
	filtered.Roles = state.filterRoles(filtered.Roles)
	filtered.RoleMemberships = state.filterRoleMemberships(filtered.RoleMemberships)
	filtered.WideAccess = state.filterWideAccess(filtered.WideAccess)
	filtered.WideGrants = state.filterWideGrants(filtered.WideGrants)
	filtered.ObjectOwners = state.filterObjectOwners(filtered.ObjectOwners)
	filtered.Grants = state.filterGrants(filtered.Grants, excludedObjects(schema.FeatureObjects, filtered.FeatureObjects))
	state.noteReservedNames(filtered.ReservedNames)
	state.noteUnreadTables(filtered.UnreadTables)
	filtered.UnmodeledObjects = state.filterUnmodeledObjects(filtered.UnmodeledObjects)
	state.noteRemovalBlockers(filtered.RemovalBlockers)
	state.noteRolesOutOfScope(filtered.RolesOutOfScope)
	state.noteUnregisteredVirtualTables(filtered.UnregisteredVirtualTables)
	filtered.OutOfScope = outOfScopeParts(schema, filtered)
	return filtered, state.report(), nil
}

// outOfScopeParts returns the parts of the tables filtered keeps that read
// reported and filtered left out, after the parts read already held out of
// scope. The columns, indexes and constraints of a table filtered does not
// keep are not among them: a table out of scope is not changed, so nothing has
// to carry its parts. Every trigger and view filtered left out is, whatever
// table it belongs to or reads: a plan that writes a kept table anew must not
// break one that names it.
//
// It compares the two descriptions rather than asking each filter what it
// removed, so a part every rule below takes out is carried alike: a column a
// selector names, an index that left with the constraint it enforces, a
// trigger of a kept table.
func outOfScopeParts(read, filtered *catalog.Database) catalog.TableParts {
	kept := make(map[partKey]catalog.Table, len(filtered.Tables))
	for _, table := range filtered.Tables {
		kept[partKey{schema: table.Schema, table: table.Name}] = table
	}
	keptTable := func(schema, table string) bool {
		_, ok := kept[partKey{schema: schema, table: table}]
		return ok
	}
	var parts catalog.TableParts
	for _, table := range read.OutOfScope.Tables {
		if keptTable(table.Schema, table.Name) {
			parts.Tables = append(parts.Tables, table.Clone())
		}
	}
	for _, table := range read.Tables {
		held, ok := kept[partKey{schema: table.Schema, table: table.Name}]
		if !ok {
			continue
		}
		keptColumns := make(map[string]bool, len(held.Columns))
		for _, column := range held.Columns {
			keptColumns[column.Name] = true
		}
		left := table.Clone()
		left.Columns = slices.DeleteFunc(left.Columns, func(column catalog.Column) bool { return keptColumns[column.Name] })
		if len(left.Columns) > 0 {
			parts.Tables = append(parts.Tables, left)
		}
	}
	parts.Indexes = leftOutParts(read.OutOfScope.Indexes, read.Indexes, filtered.Indexes, keptTable, indexPartKey, catalog.Index.Clone)
	parts.Constraints = leftOutParts(read.OutOfScope.Constraints, read.Constraints, filtered.Constraints, keptTable, constraintPartKey, catalog.Constraint.Clone)
	anyTable := func(string, string) bool { return true }
	parts.Triggers = leftOutParts(read.OutOfScope.Triggers, read.Triggers, filtered.Triggers, anyTable, triggerPartKey, catalog.Trigger.Clone)
	parts.Views = leftOutParts(read.OutOfScope.Views, read.Views, filtered.Views, anyTable, viewPartKey, cloneViewPart)
	return parts
}

// cloneViewPart copies a view with independent attribute and option lists,
// so the out-of-scope copy shares no backing array with the read.
func cloneViewPart(view catalog.View) catalog.View {
	view.Attributes = slices.Clone(view.Attributes)
	view.Options = slices.Clone(view.Options)
	return view
}

// partKey names one part of a table, or the table itself with an empty kind
// and name.
type partKey struct {
	schema, table, kind, name string
}

// leftOutParts returns the parts of read that filtered does not hold, after
// those carried already, keeping only the parts of a table filtered keeps.
func leftOutParts[T any](
	carried, read, filtered []T,
	keptTable func(schema, table string) bool,
	key func(T) partKey,
	clone func(T) T,
) []T {
	held := make(map[partKey]bool, len(filtered))
	for _, part := range filtered {
		held[key(part)] = true
	}
	var left []T
	for _, part := range carried {
		if k := key(part); keptTable(k.schema, k.table) {
			left = append(left, clone(part))
		}
	}
	for _, part := range read {
		if k := key(part); keptTable(k.schema, k.table) && !held[k] {
			left = append(left, clone(part))
		}
	}
	return left
}

func indexPartKey(index catalog.Index) partKey {
	return partKey{schema: index.Schema, table: index.TableName, kind: "index", name: index.Name}
}

// constraintPartKey names a constraint by its type and columns as well as its
// name, because a reader may leave a constraint unnamed.
func constraintPartKey(constraint catalog.Constraint) partKey {
	columns := constraint.ColumnNames
	if len(columns) == 0 && constraint.ColumnName != "" {
		columns = []string{constraint.ColumnName}
	}
	check := ""
	if constraint.CheckClause != nil {
		check = *constraint.CheckClause
	}
	return partKey{
		schema: constraint.Schema,
		table:  constraint.TableName,
		kind:   constraint.Type,
		name:   constraint.Name + "\x00" + strings.Join(columns, "\x00") + "\x00" + check,
	}
}

func triggerPartKey(trigger catalog.Trigger) partKey {
	return partKey{schema: trigger.Schema, table: trigger.Table, kind: "trigger", name: trigger.Name}
}

func viewPartKey(view catalog.View) partKey {
	return partKey{schema: view.Schema, kind: "view", name: view.Name}
}

// unfilteredReport is the report for a state no pattern was ever tested
// against: nothing matched, because nothing was asked.
func unfilteredReport(filters []resourcePattern) ExcludeReport {
	var unmatched []string
	for _, filter := range filters {
		unmatched = append(unmatched, filter.raw)
	}
	return ExcludeReport{Unmatched: unmatched}
}

// ExcludeGenerated returns a shallow copy of schema with generated-schema IR
// resources matching Atlas-style exclude globs removed.
func ExcludeGenerated(selections selectext.Set, schema *schemamodel.Database, patterns []string) (*schemamodel.Database, error) {
	return ExcludeGeneratedWithDefaultSchema(selections, schema, patterns, "")
}

// ExcludeGeneratedWithDefaultSchema is [ExcludeGenerated] told which schema
// owns the objects the generated IR left unqualified.
//
// Both sides of a comparison must subtract the same objects: a pattern that
// removed a table from the introspected side but not from the desired side
// would turn a filtered-out object into a CREATE.
func ExcludeGeneratedWithDefaultSchema(
	selections selectext.Set,
	schema *schemamodel.Database,
	patterns []string,
	defaultSchema string,
) (*schemamodel.Database, error) {
	filtered, _, err := ExcludeGeneratedReport(selections, schema, patterns, defaultSchema)
	return filtered, err
}

// ExcludeGeneratedReport is [ExcludeGeneratedWithDefaultSchema] plus the
// [ExcludeReport] for the generated side.
func ExcludeGeneratedReport(
	selections selectext.Set,
	schema *schemamodel.Database,
	patterns []string,
	defaultSchema string,
) (*schemamodel.Database, ExcludeReport, error) {
	return excludeGenerated(selections, schema, patterns, defaultSchema, defaultSchema, "", LiveKeys{}, featureselect.Bindings{})
}

// ExcludeGeneratedScopeReport is [ExcludeDatabaseScopeReport] for the generated
// side, so both sides of a comparison count a pattern the same way.
func ExcludeGeneratedScopeReport(
	schema *schemamodel.Database,
	scope Scope,
) (*schemamodel.Database, ExcludeReport, error) {
	return excludeGenerated(scope.Selections, schema, scope.Exclude, scope.patternScopeSchema(), scope.DefaultSchema, scope.Dialect, scope.LiveKeys, scope.Bindings)
}

func excludeGenerated(
	selections selectext.Set,
	schema *schemamodel.Database,
	patterns []string,
	patternScope, defaultSchema, dialect string,
	liveKeys LiveKeys,
	bindings featureselect.Bindings,
) (*schemamodel.Database, ExcludeReport, error) {
	if !selections.Selected() {
		return nil, ExcludeReport{}, selectext.ErrUnselected
	}
	words := vocabulary{selections: selections}
	filters, err := parsePatterns(patterns, resolvedDepthScope(patternScope), words)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if schema == nil || len(filters) == 0 {
		return schema, unfilteredReport(filters), nil
	}

	state := newExclusionState(filters, defaultSchema, words)
	state.dialect = dialect
	state.fold = unquotedFold(dialect)
	state.foldSelectors(state.fold)
	state.liveKeys = liveKeys
	filtered := cloneGenerated(schema)
	// Same ordering rule as the database side, and the same reason both sides
	// have this pass at all: a schema removed from one side only would come
	// back as a CREATE SCHEMA for the object the selector was written to
	// protect.
	filtered.Schemas = state.filterGeneratedSchemas(filtered.Schemas)
	filtered.Tables = state.filterGeneratedTables(filtered.Tables)
	filtered.ManagedData = state.filterGeneratedManagedData(filtered.ManagedData)
	var removedKeys map[string]map[string]bool
	filtered.Tables, removedKeys = state.excludeGeneratedPrimaryKeys(filtered.Tables, filtered.Fields)
	tableByStruct := generatedTableByStruct(filtered.Tables)
	filtered.Fields = state.filterGeneratedFields(tableByStruct, filtered.Fields, removedKeys)
	state.excludeGeneratedColumnSequences(schema.Tables, schema.Fields)
	filtered.Tables = state.stripGeneratedTableColumnReferences(filtered.Tables)
	state.noteOwnedObjectSelectors(filtered.FeatureObjects)
	filtered.FeatureObjects, filtered.FeatureCoverage, err = bindings.Select(filtered.FeatureObjects, filtered.FeatureCoverage, func(schema, table string) bool {
		return !state.tableExcluded(schema, table)
	}, state.vocabulary.independentKinds()...)
	if err != nil {
		return nil, ExcludeReport{}, err
	}

	filtered.FeatureObjects, filtered.FeatureCoverage, err = state.filterOwnerFeatures(filtered.FeatureObjects, filtered.FeatureCoverage)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if filtered.FeatureObjects, err = state.clearOwnerFields(filtered.FeatureObjects); err != nil {
		return nil, ExcludeReport{}, err
	}
	// The same order as the database side, for the same reason.
	var removedConstraints []schemamodel.Constraint
	filtered.Constraints, removedConstraints = state.filterGeneratedConstraints(tableByStruct, filtered.Constraints)
	filtered.Indexes = state.filterGeneratedIndexes(tableByStruct, filtered.Indexes, removedConstraints)
	filtered.EmbeddedFields = state.filterGeneratedEmbeddedFields(tableByStruct, filtered.EmbeddedFields)
	filtered.Enums = state.filterGeneratedEnums(filtered.Enums)
	filtered.Sequences = state.filterGeneratedSequences(filtered.Sequences)
	filtered.FeatureObjects, filtered.FeatureCoverage, err = state.filterExtensionFeatures(filtered.FeatureObjects, filtered.FeatureCoverage)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	filtered.Functions = state.filterGeneratedFunctions(filtered.Functions)
	filtered.Views = state.filterGeneratedViews(filtered.Views)
	filtered.MaterializedViews = state.filterGeneratedMaterializedViews(filtered.MaterializedViews)
	filtered.Triggers = state.filterGeneratedTriggers(tableByStruct, filtered.Triggers)
	filtered.RLSPolicies = state.filterGeneratedRLSPolicies(tableByStruct, filtered.RLSPolicies)
	filtered.RLSEnabledTables = state.filterGeneratedRLSEnabledTables(tableByStruct, filtered.RLSEnabledTables)
	state.excludeDesiredRowSecurity(filtered)
	filtered.Roles = state.filterGeneratedRoles(filtered.Roles)
	excluded := excludedObjects(schema.FeatureObjects, filtered.FeatureObjects)
	filtered.Grants = state.filterGeneratedGrants(filtered.Grants, excluded)
	filtered.RevokedGrants = state.filterGeneratedGrants(filtered.RevokedGrants, excluded)
	filtered.Dependencies = nil
	filtered.FunctionDependencies = nil
	filtered.SelfReferencingForeignKeys = nil
	schemamodel.Finalize(filtered)
	return filtered, state.report(), nil
}

type resourcePattern struct {
	// raw is the selector exactly as the user wrote it, trimmed and already
	// split on commas. Diagnostics quote it, so a report names the spelling
	// the user typed rather than the glob left after the selector was cut off.
	raw   string
	glob  string
	types map[string]struct{}
	// fields names the object fields this pattern subtracts. Empty means the
	// pattern names the object itself.
	fields map[string]struct{}
	// schemaSegment records that the pattern spelled its schema part as an
	// explicit `<glob>[type=schema].` segment. Such a pattern is realm-relative
	// by construction, so [checkPatternDepth] must not prefix the connection's
	// schema onto it a second time.
	schemaSegment bool
}

type typeSelector struct {
	glob   string
	types  map[string]struct{}
	fields map[string]struct{}
}

// filterKindExclude and filterKindInclude label pattern-parse errors with the
// flag family the pattern came from.
const (
	filterKindExclude = "exclude"
	filterKindInclude = "include"
)

// ValidateExcludeSelectors parses Atlas-style --exclude selectors and rejects
// forms Ptah cannot honor: malformed globs, unsupported selector kinds, field
// selectors beyond the documented extension version form, and type selectors
// on non-final pattern segments. Commands run it before any database work.
// The depth rule needs the schema the patterns are relative to, which commands
// only learn once a connection is open, so this pre-connect pass applies the
// scope-independent half of it: a pattern too deep for any scope is rejected
// before a database is contacted, and the rest is caught by the filter.
func ValidateExcludeSelectors(values []string, selections selectext.Set) error {
	if !selections.Selected() {
		return selectext.ErrUnselected
	}
	_, err := parsePatterns(values, depthScope{}, vocabulary{selections: selections})
	return err
}

// depthScope is what a pattern's depth is counted against.
//
// The empty string cannot carry this on its own, because it already means
// "realm-scoped: no schema is prefixed". The pre-connect pass in
// [ValidateExcludeSelectors] passes the same empty string for a different
// reason -- no connection has said which scope applies yet -- and a diagnostic
// that confused the two told a schema-bound run its pattern was realm-scoped
// (stokaro/ptah#1703).
type depthScope struct {
	// schema is the connection's schema on a schema-bound run, and empty on a
	// realm-scoped one.
	schema string
	// resolved is false only in the pre-connect pass.
	resolved bool
}

func resolvedDepthScope(schema string) depthScope {
	return depthScope{schema: strings.TrimSpace(schema), resolved: true}
}

func parsePatterns(values []string, scope depthScope, words vocabulary) ([]resourcePattern, error) {
	var patterns []resourcePattern
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			pattern, err := parsePattern(part, filterKindExclude, words)
			if err != nil {
				return nil, err
			}
			if pattern.glob == "" {
				continue
			}
			counted := scope
			if pattern.schemaSegment {
				// The pattern named its schema slot itself, so it is already
				// realm-relative and the connection's schema must not be
				// counted a second time.
				counted.schema = ""
			}
			// Counted on the pattern as written, selector text and field suffix
			// included. That is the pinned community binary's own arithmetic:
			// on a schema-bound URL it refuses `public.*[type=table].comment` as
			// "public.public.*[type=table].comment". Counting the resource glob
			// instead would accept that spelling, and Ptah applies one depth
			// rule to every scope, so it would exit 0 on the schema-bound URL
			// where that binary exits 1.
			if err := checkPatternDepth(strings.TrimSpace(part), counted); err != nil {
				return nil, err
			}
			patterns = append(patterns, pattern)
		}
	}
	return patterns, nil
}

// maxPatternParts is the deepest object an Atlas exclude pattern can name:
// schema.object.child. The pinned community binary splits a pattern on "." and
// refuses anything deeper.
const maxPatternParts = 3

// checkPatternDepth refuses an exclude pattern that names more parts than its
// scope can address.
//
// A pattern is relative to the scope the URL names, and there are two. On a URL
// that names a schema the binary prefixes it before counting, so a pattern
// names `object` or `object.child` and a third part has nowhere left to go:
// `--exclude public.users.name` is refused, and the binary reports it as
// "public.public.users.name" while Ptah quotes what was typed and names the
// spelling that works. On a URL that names no
// schema the pattern is realm-relative and the full schema.object.child depth
// is addressable.
//
// The column part itself is never the error. `users.name` names a column in the
// first scope and `public.users.name` names one in the second; what has no
// meaning is a pattern whose schema slot is already filled by the connection.
//
// Which scope applies is the caller's answer to give, and it has to be asked: a
// defaultSchema that arrives non-empty on every run counts a realm-scoped
// pattern against a schema it is not relative to, and refuses the qualified
// column form everywhere. See [Scope.RealmRelativePatterns].
//
// The refusal is Ptah's own text, and that is a retained divergence rather than
// a gap. The binary quotes the prefixed pattern -- "public.public.users.name"
// for a pattern written as "public.users.name" -- which names a string nobody
// typed and says nothing about what to write instead. Same refusal, better
// diagnostic: rule 1 in docs/conformance.md is about never being looser, and
// this is not looser. See the retained-divergence entry there
// (stokaro/ptah#1703).
func checkPatternDepth(raw string, scope depthScope) error {
	effective := raw
	if scope.schema != "" {
		effective = scope.schema + "." + raw
	}
	if strings.Count(effective, ".")+1 <= maxPatternParts {
		return nil
	}
	return fmt.Errorf("too many parts in pattern %q: %s", raw, patternDepthGuidance(raw, scope))
}

// patternDepthGuidance says what the scope can address, and names the pattern
// that reaches the same object where one exists.
//
// A refusal that only reports the depth leaves the user to discover the scope
// rule by experiment. In the case this was reported for the answer is one
// segment away: on a connection bound to "public", "public.users.name" is
// refused and "users.name" drops exactly the column it was reaching for.
//
// The suggestion is offered only when it is true. A leading segment that is not
// the bound schema is a table name in this scope, not a schema, so dropping it
// would suggest a pattern that reaches something else.
func patternDepthGuidance(raw string, scope depthScope) string {
	if !scope.resolved {
		return "a pattern names at most schema.object.child"
	}
	if scope.schema == "" {
		return "this run covers every schema, so a pattern names schema, schema.object or schema.object.child"
	}
	guidance := fmt.Sprintf(
		"this connection is bound to schema %q, so a pattern names object or object.child", scope.schema)
	prefix := scope.schema + "."
	if remainder, bound := strings.CutPrefix(raw, prefix); bound &&
		remainder != "" && strings.Count(remainder, ".")+1 <= maxPatternParts-1 {
		return guidance + fmt.Sprintf("; write %q", remainder)
	}
	return guidance
}

// envReferenceScheme is the prefix of a reference into the selected project
// environment. It is resolved by the desired-state classifier, and by nothing
// else.
const envReferenceScheme = "env://"

// refuseEnvReference refuses a selector carrying an env:// reference, naming
// the flag rather than letting the value through as a literal glob.
//
// Only --to, --from and --url run through the classifier that expands these
// references; a selector never does. Passed through, `--exclude env://exclude`
// is an exclusion pattern spelled `env://exclude`, which matches no object,
// and the run then either refuses for the wrong reason -- the unmatched
// selection guard blames the glob rather than the scheme -- or, where that
// guard warns instead of failing, proceeds with NOTHING excluded and exits 0.
// On `schema apply` and `schema clean` that difference is destructive
// (stokaro/ptah#1697).
//
// The remedy is not to resolve it. The selected environment's own `exclude`
// list is already applied without any flag, so the reference asks for
// something the run already does; what the flag needs is the object pattern
// itself.
func refuseEnvReference(raw, kind string) error {
	if !strings.HasPrefix(strings.ToLower(raw), envReferenceScheme) {
		return nil
	}
	return fmt.Errorf(
		"--%s does not resolve %s references: %q would be used as a literal pattern and match nothing; %s",
		kind, envReferenceScheme, raw, envReferenceRemedy(kind))
}

// envReferenceRemedy names the way out, which differs by flag: the selected
// environment has an exclude list and Ptah already applies it, so an
// --exclude reference asks for something the run already does. There is no
// include list to point at.
func envReferenceRemedy(kind string) string {
	if kind == filterKindExclude {
		return "the selected env's own exclude list is already applied without this flag, " +
			"so write the object pattern itself"
	}
	return "write the object pattern itself"
}

func parsePattern(value, kind string, words vocabulary) (resourcePattern, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return resourcePattern{}, nil
	}
	if err := refuseEnvReference(raw, kind); err != nil {
		return resourcePattern{}, err
	}
	body, schemaSegment, err := stripSchemaTypeSegment(raw, kind)
	if err != nil {
		return resourcePattern{}, err
	}
	// Past the schema segment, Ptah evaluates one selector per pattern, on the
	// final segment — the only other placement the pinned community binary was
	// measured to honor. A further selector would be swallowed into the glob and
	// silently match nothing, so reject it loudly.
	if strings.Count(body, "[type=") > 1 {
		return resourcePattern{}, fmt.Errorf(
			"unsupported Atlas %s selector %q: type selectors are supported on the final pattern segment only", kind, raw)
	}
	glob := body
	types := make(map[string]struct{})
	var fields map[string]struct{}
	if open := strings.LastIndex(body, "[type="); open >= 0 {
		selector, err := parseTypeSelector(body, open, kind, words)
		if err != nil {
			return resourcePattern{}, err
		}
		glob = selector.glob
		types = selector.types
		fields = selector.fields
	} else if selector, ok := selectorLikeSuffix(body); ok {
		return resourcePattern{}, fmt.Errorf("unsupported Atlas %s selector %q", kind, selector)
	}
	if _, err := path.Match(glob, "ptah_match_probe"); err != nil {
		return resourcePattern{}, fmt.Errorf("invalid Atlas %s glob %q: %w", kind, raw, err)
	}
	return resourcePattern{raw: raw, glob: glob, types: types, fields: fields, schemaSegment: schemaSegment}, nil
}

// stripSchemaTypeSegment rewrites a leading `<glob>[type=schema].` segment into
// the plain `<glob>.` schema qualification the rest of the parser already
// understands, and reports whether it did.
//
// `*[type=schema].*[type=table]` is the one multi-segment spelling the pinned
// community binary v1.3.0 was measured to implement rather than merely accept:
// on a PostgreSQL database holding two tables and two enums across two schemas
// it removed both tables and kept both enums, exit 0, and narrowing the leading
// glob to `app[type=schema]` removed only that schema's table. Ptah refused the
// spelling outright, so a command line that ran against that binary aborted
// here — the stricter direction, and a drop-in break (stokaro/ptah#933).
//
// Only `schema` is honored in a non-final segment, because a schema is the only
// thing a leading segment can name. Any other type there keeps the refusal.
func stripSchemaTypeSegment(raw, kind string) (body string, found bool, err error) {
	open := strings.Index(raw, "[type=")
	if open < 0 {
		return raw, false, nil
	}
	closeIdx := strings.Index(raw[open:], "]")
	if closeIdx < 0 {
		return raw, false, nil
	}
	closeIdx += open
	rest := raw[closeIdx+1:]
	// A "." behind the selector opens another segment only when a further
	// selector follows it. Otherwise it introduces a field selector, which
	// [parseFieldSelector] owns.
	if !strings.HasPrefix(rest, ".") || !strings.Contains(rest, "[type=") {
		return raw, false, nil
	}
	selectorName, selectorValue, ok := strings.Cut(raw[open+1:closeIdx], "=")
	if !ok || strings.TrimSpace(selectorName) != "type" ||
		strings.ToLower(strings.TrimSpace(selectorValue)) != "schema" {
		return "", false, fmt.Errorf(
			"unsupported Atlas %s selector %q: type selectors are supported on the final pattern segment only",
			kind, raw)
	}
	glob := strings.TrimSpace(raw[:open])
	if glob == "" {
		glob = "*"
	}
	return glob + rest, true, nil
}

func parseTypeSelector(raw string, open int, kind string, words vocabulary) (typeSelector, error) {
	closeIdx := strings.Index(raw[open:], "]")
	if closeIdx < 0 {
		return typeSelector{}, fmt.Errorf("unsupported Atlas %s selector %q", kind, raw)
	}
	closeIdx += open
	selector := raw[open+1 : closeIdx]
	glob := strings.TrimSpace(raw[:open])
	if glob == "" {
		glob = "*"
	}
	selectorName, selectorValue, ok := strings.Cut(selector, "=")
	if !ok || strings.TrimSpace(selectorName) != "type" {
		return typeSelector{}, fmt.Errorf("unsupported Atlas %s selector %q", kind, selector)
	}
	types := make(map[string]struct{})
	for item := range strings.SplitSeq(selectorValue, "|") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			types[item] = struct{}{}
		}
	}
	if len(types) == 0 {
		return typeSelector{}, fmt.Errorf("empty Atlas %s type selector %q", kind, selector)
	}
	fields, err := parseFieldSelector(raw[closeIdx+1:], types, kind, words)
	if err != nil {
		return typeSelector{}, err
	}
	return typeSelector{glob: glob, types: types, fields: fields}, nil
}

// excludeFieldSelectors names, per resource kind, the object fields an
// --exclude field selector can subtract while leaving the object itself.
//
// The pinned community binary v1.3.0 accepts every field selector and honors
// none of them: `--exclude '*[type=table].comment'` there is exit 0 with output
// byte-identical to the same command without the flag, comments and all.
// Reproducing that would mean accepting a scoping instruction and silently not
// carrying it out, which is the failure this issue exists to remove, so Ptah
// honors the fields it can subtract and refuses the rest by name
// (stokaro/ptah#933).
var excludeFieldSelectors = map[string]map[string]struct{}{
	"extension":         {"version": {}},
	"table":             {"comment": {}},
	"base_table":        {"comment": {}},
	"view":              {"comment": {}},
	"materialized_view": {"comment": {}},
}

// parseFieldSelector resolves the `.field` suffix behind a type selector into
// the set of fields the pattern subtracts. `.*` names every field Ptah can
// subtract for the selected types.
func parseFieldSelector(suffix string, types map[string]struct{}, kind string, words vocabulary) (map[string]struct{}, error) {
	if suffix == "" {
		return nil, nil
	}
	field, ok := strings.CutPrefix(suffix, ".")
	if !ok || field == "" {
		return nil, fmt.Errorf("unsupported Atlas %s field selector suffix %q", kind, suffix)
	}
	if kind != filterKindExclude {
		return nil, fmt.Errorf("unsupported Atlas %s field selector %q", kind, suffix)
	}
	fields := make(map[string]struct{})
	for resourceType := range types {
		for _, honored := range words.fields(resourceType) {
			if field == "*" || field == honored {
				fields[honored] = struct{}{}
			}
		}
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf(
			"unsupported Atlas %s field selector %q: Ptah refuses a field selector it would have to ignore; %s",
			kind, suffix, supportedFieldsMessage(types, words))
	}
	return fields, nil
}

// supportedFieldsMessage names the fields the selected types do support, so a
// refusal says what would have worked.
func supportedFieldsMessage(types map[string]struct{}, words vocabulary) string {
	supported := make(map[string]struct{})
	for resourceType := range types {
		for _, honored := range words.fields(resourceType) {
			supported[honored] = struct{}{}
		}
	}
	if len(supported) == 0 {
		return "the selected resource types have no subtractable fields"
	}
	return "supported for the selected resource types: " + strings.Join(sortedFieldNames(supported), ", ")
}

func sortedFieldNames(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}

// matches reports whether this pattern names the object itself. A pattern
// carrying a field selector subtracts a field and never the object.
func (p resourcePattern) matches(resourceType string, names ...string) bool {
	if len(p.fields) > 0 {
		return false
	}
	return p.matchesNames(resourceType, names...)
}

// matchesField reports whether this pattern subtracts the named field of the
// object.
func (p resourcePattern) matchesField(resourceType, field string, names ...string) bool {
	if _, ok := p.fields[field]; !ok {
		return false
	}
	return p.matchesNames(resourceType, names...)
}

func (p resourcePattern) matchesNames(resourceType string, names ...string) bool {
	if len(p.types) > 0 {
		if _, ok := p.types[strings.ToLower(resourceType)]; !ok {
			return false
		}
	}
	for _, name := range names {
		if globMatch(p.glob, name) {
			return true
		}
	}
	return false
}

func globMatch(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok
}

func selectorLikeSuffix(raw string) (selector string, ok bool) {
	open := strings.LastIndex(raw, "[")
	if open < 0 {
		return "", false
	}
	closeIdx := strings.Index(raw[open:], "]")
	if closeIdx < 0 {
		return "", false
	}
	closeIdx += open
	selector = raw[open+1 : closeIdx]
	return selector, strings.Contains(selector, "=")
}

type exclusionState struct {
	// vocabulary is the run's owners' selector types and fields.
	vocabulary vocabulary
	patterns   []resourcePattern
	// dialect is the target the state describes, read for the names its
	// server gives a key, for whether it changes a constraint in place, and
	// for which index enforces a constraint. Empty is a target nothing
	// describes.
	dialect string
	// liveKeys are the primary keys the database holds, for the desired side
	// to name a declared key by; see [exclusionState.primaryKeyNameCandidates].
	liveKeys LiveKeys
	// columnless holds the tables a selector left without a column, whose
	// constraints leave with their columns; see [exclusionState.keepConstraint].
	columnless map[tableIdentity]struct{}
	// defaultSchema owns objects that carry no schema of their own. It is the
	// connection's schema for introspected states and the dialect default for
	// file-backed ones; empty means "no default", which restores the
	// bare-name-only candidate set.
	defaultSchema string
	// matched[i] records whether patterns[i] ever named an object during this
	// run. Emptiness is an outcome rather than a shape -- a selector can be
	// perfectly well formed and still name nothing -- so it cannot be decided
	// by reading the selector text, only by watching the match.
	matched []bool
	// storedGlobs[i] is the glob of patterns[i] with its letters in the case
	// of the target's UnquotedFold, or empty where that is the glob itself
	// or the target states no fold. storedMatched[i] records that it named
	// an object, which [ExcludeReport.Stored] reports for a selector that
	// named nothing as written.
	storedGlobs     []string
	storedMatched   []bool
	excludedTables  map[tableIdentity]struct{}
	excludedColumns map[columnIdentity]struct{}
	// excludedSchemas holds the schemas a selector named. Every object that
	// lives in one leaves with it, the way a column leaves with its table:
	// the pinned binary drops the whole schema for `--exclude app`, so an
	// object kept behind an excluded schema would still be planned for a DROP
	// the selector was written to prevent.
	excludedSchemas map[string]struct{}
	// excludedSequences mirrors excludedTables for sequences, so a grant on a
	// sequence a selector removed leaves with it instead of naming an object
	// the filtered schema no longer holds.
	excludedSequences map[tableIdentity]struct{}
	// removedGeneratedTables keeps the generated tables an earlier selector
	// removed, keyed the way generatedTableByStruct keys the surviving ones. A
	// generated child is reached through its parent struct, so once the parent
	// leaves the map there is no name left to test a child selector against and
	// the child selector reports as naming nothing. The keep decision does not
	// use this map -- a child of a removed table leaves with it either way.
	removedGeneratedTables map[string]schemamodel.Table
	// fold is the UnquotedFold of a filtered generated state, which spells
	// its names as a statement does; see storedSpelling. Empty meets every
	// name as written.
	fold identifier.Fold
}

// tableIdentity and columnIdentity are the shared identity model's comparison
// value, under this package's own names.
//
// They were two private structs here and two more, shaped the same way, in the
// comparator. Two copies of "what makes two tables the same table" is how the
// filter and the comparator come to disagree about one schema: an object the
// filter believes it excluded stays visible to the comparator, which then plans
// against it. One model answers both (stokaro/ptah#1345).
type (
	tableIdentity  = objectidentity.Key
	columnIdentity = objectidentity.Key
)

// exactIdentities is the folding rule this package compares under, and it folds
// nothing.
//
// An exclude pattern and the schema it is matched against are both written by
// the same author in the same file, so the spelling that appears in one is the
// spelling that appears in the other. Folding here would silently widen a
// pattern past what it says: `--exclude Orders` would take `orders` with it on
// a target where the two are distinct tables, and the object would leave the
// schema without ever being named.
var exactIdentities = objectidentity.NewBuilder(identifier.Semantics{
	TableNames:  identifier.ComparisonExact,
	ColumnNames: identifier.ComparisonExact,
	IndexNames:  identifier.ComparisonExact,
})

func newExclusionState(patterns []resourcePattern, defaultSchema string, words vocabulary) *exclusionState {
	matched := make([]bool, len(patterns))
	for i, pattern := range patterns {
		// A whole-kind exclusion can prevent the catalog read that would have
		// found matching objects. It still protects every object of that kind.
		matched[i] = len(pattern.wholeKinds()) != 0
	}
	return &exclusionState{
		vocabulary:             words,
		patterns:               patterns,
		defaultSchema:          strings.TrimSpace(defaultSchema),
		matched:                matched,
		excludedTables:         make(map[tableIdentity]struct{}),
		excludedColumns:        make(map[columnIdentity]struct{}),
		excludedSchemas:        make(map[string]struct{}),
		excludedSequences:      make(map[tableIdentity]struct{}),
		removedGeneratedTables: make(map[string]schemamodel.Table),
		columnless:             make(map[tableIdentity]struct{}),
	}
}

// unmatchedSelectors returns the exclude selectors, as written, that named no
// object in the state this run filtered.
func (s *exclusionState) unmatchedSelectors() []string {
	var out []string
	for i, pattern := range s.patterns {
		if !s.matched[i] {
			out = append(out, pattern.raw)
		}
	}
	return out
}

// effectiveSchema resolves the schema that owns an object. An object with no
// schema of its own lives in the default schema, so it is the same object the
// schema-qualified spelling names.
func (s *exclusionState) effectiveSchema(schema string) string {
	if strings.TrimSpace(schema) != "" {
		return schema
	}
	return s.defaultSchema
}

func (s *exclusionState) effectiveExtensionSchema(schema string) string {
	if schema != "" {
		return schema
	}
	return s.defaultSchema
}

// nameCandidates returns the names an exclude pattern can match for a
// top-level object: the bare name and the effective-schema-qualified name.
// Both spellings name the same object, so both are offered — matching the
// bare name is looser than the community binary on a database URL, and
// looseness on an exclude subtracts rather than adds statements.
func (s *exclusionState) nameCandidates(schema, name string) []string {
	if s.fold != "" {
		schema, name = storedSpelling(schema, s.fold), storedSpelling(name, s.fold)
	}
	return qualifiedNameCandidates(s.effectiveSchema(schema), name)
}

// qualifiedNameCandidatesFor is [exclusionState.nameCandidates] for objects
// whose only schema qualification is an optional "schema." prefix on the name,
// mirroring how the include projection reads generated views.
func (s *exclusionState) qualifiedNameCandidatesFor(name string) []string {
	schema, bare := splitQualified(name)
	return s.nameCandidates(schema, bare)
}

// childNameCandidates returns the names an exclude pattern can match for a
// child of a table: "table.child" and "schema.table.child". The qualified
// spelling is reachable only in the realm-relative scope; once a default schema
// is in play, [checkPatternDepth] refuses a pattern that deep before it can be
// matched against.
func (s *exclusionState) childNameCandidates(schema, table, child string) []string {
	if s.fold != "" {
		schema, table, child = storedSpelling(schema, s.fold), storedSpelling(table, s.fold), storedSpelling(child, s.fold)
	}
	return tableChildNameCandidates(s.effectiveSchema(schema), table, child)
}

// filterSchemas subtracts the schemas an exclude selector names.
//
// Schemas were the last collection this package cloned and never offered to
// the patterns. That made two answers wrong at once. A selector naming a real
// schema was reported as having matched nothing, so `schema apply` refused a
// selection that in fact named an object the description renders -- the very
// failure the unmatched report exists to prevent. And it was destructive:
// `--exclude app` left every object in `app` in the description, so the plan
// still dropped them.
//
// Measured on the pinned community binary against a database holding
// `public.users`, `public.mood`, `app.orders` and `app.color`, with the same
// `-s public -s app` scope ptah-compat exposes:
//
//	--exclude app          removes schema "app" and both objects in it
//	--exclude public       removes schema "public" and both objects in it
//	--exclude p*           removes schema "public" the same way
//	--exclude app[type=schema]  removes schema "app" and both objects in it
//	--exclude app.*        removes app's objects and KEEPS schema "app"
//
// So a pattern is matched against the schema's own name, under resource type
// `schema`, and a schema that matches takes its contents with it. The two-part
// `app.*` spelling keeps naming the objects rather than the schema, because
// `app` is not a match for the glob `app.*`.
//
// Ptah also keeps honoring the bare spelling as an object name, which that
// binary does not do in this scope -- the divergence already recorded for
// tables and views. Both readings are offered here for the same reason both
// name candidates are offered elsewhere: on an exclude, looseness only ever
// subtracts more, and it cannot turn a protected object into a dropped one.
func (s *exclusionState) filterSchemas(schemas []catalog.Schema) []catalog.Schema {
	return keep(schemas, func(schema catalog.Schema) bool {
		excluded := s.matches("schema", schemaNameCandidates(schema.Name)...)
		if excluded {
			s.excludeSchema(schema.Name)
		}
		return !excluded
	})
}

// filterGeneratedSchemas is [exclusionState.filterSchemas] for the desired
// side, so both sides of a comparison subtract the same schemas.
func (s *exclusionState) filterGeneratedSchemas(schemas []schemamodel.Schema) []schemamodel.Schema {
	return keep(schemas, func(schema schemamodel.Schema) bool {
		excluded := s.matches("schema", schemaNameCandidates(schema.Name)...)
		if excluded {
			s.excludeSchema(schema.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterTables(tables []catalog.Table) []catalog.Table {
	result := make([]catalog.Table, 0, len(tables))
	for _, table := range tables {
		table = cloneTable(table)
		tableNames := s.nameCandidates(table.Schema, table.Name)
		if s.matchesAny(tableResourceTypes(table), tableNames...) || s.schemaExcluded(table.Schema) {
			s.excludeTable(table.Schema, table.Name)
			// The table leaves with every column it had, but the column
			// patterns still have to be ASKED. filterColumns is never reached
			// for an excluded table, so without this a selector naming a real
			// column of it -- `--exclude users --exclude users.id` -- would be
			// reported as naming nothing. Same rule as filterIndexes below,
			// applied to the one child the parent short-circuit skips entirely.
			s.noteColumnSelectors(table)
			continue
		}
		if s.matchesAnyField(tableResourceTypes(table), "comment", tableNames...) {
			table.Comment = ""
		}
		columns := len(table.Columns)
		table.Columns = s.filterColumns(table, table.Columns)
		if columns > 0 && len(table.Columns) == 0 {
			if key, ok := tableIdentityKey(s.effectiveSchema(table.Schema), table.Name); ok {
				s.columnless[key] = struct{}{}
			}
		}
		result = append(result, table)
	}
	return result
}

// noteColumnSelectors asks the patterns whether they name a column of a table
// another selector already removed. It records matches and nothing else: the
// columns are gone with their table either way, so no exclusion is recorded and
// no keep decision changes.
func (s *exclusionState) noteColumnSelectors(table catalog.Table) {
	for _, column := range table.Columns {
		s.matches("column", s.childNameCandidates(table.Schema, table.Name, column.Name)...)
	}
}

func (s *exclusionState) filterColumns(table catalog.Table, columns []catalog.Column) []catalog.Column {
	result := make([]catalog.Column, 0, len(columns))
	for _, column := range columns {
		columnNames := s.childNameCandidates(table.Schema, table.Name, column.Name)
		if s.matches("column", columnNames...) {
			s.excludeColumn(table.Schema, table.Name, column.Name)
			continue
		}
		result = append(result, column)
	}
	return result
}

func (s *exclusionState) filterEnums(enums []catalog.Enum) []catalog.Enum {
	return keep(enums, func(value catalog.Enum) bool {
		named := s.matches("enum", s.nameCandidates(value.Schema, value.Name)...)
		return !named && !s.schemaExcluded(value.Schema)
	})
}

// filterSequences subtracts the sequences an exclusion has to ask about
// rather than clone. The owners' selections subtract a domain, a composite
// type and a range in the same way; see [exclusionState.filterOwnerFeatures].
//
// The include projection already selects a sequence by name
// ([scopeSelection.projectDatabaseTopLevel] and projectDatabaseSupport), so
// an unasked `--exclude order_seq` would be a silent no-op that still plans
// DROP SEQUENCE for the object the user wrote the selector to protect -- the
// same destructive miss this file answers for enums and functions. It is also
// what makes the unmatched-selector report truthful: a selector can only be
// called empty by a filter that asked it.
func (s *exclusionState) filterSequences(sequences []catalog.Sequence) []catalog.Sequence {
	return keep(sequences, func(sequence catalog.Sequence) bool {
		excluded := s.matches("sequence", s.nameCandidates(sequence.Schema, sequence.Name)...) ||
			s.schemaExcluded(sequence.Schema) || s.ownerExcluded(sequence.Schema, sequence.OwnedBy)
		if excluded {
			s.excludeSequence(sequence.Schema, sequence.Name)
		}
		return !excluded
	})
}

// noteRolesOutOfScope asks the patterns about the cluster roles this
// description deliberately does not define.
//
// They are cloned but never filtered, and correctly so: they are not part of
// the description, so there is nothing to subtract and the plan cannot touch
// them. That makes "this selector protected nothing" false for a selector that
// names one -- it is already out -- so the selector is marked as having named
// something rather than reported empty.
func (s *exclusionState) noteRolesOutOfScope(roles []catalog.Role) {
	for _, role := range roles {
		s.matches("role", role.Name)
	}
}

// noteUnregisteredVirtualTables asks the patterns about the virtual tables the
// reading build could not classify.
//
// They are cloned but never filtered, for the reason
// [noteRolesOutOfScope] gives about its own list and one more besides. This
// list is what the comparison refusal reads, and narrowing it would delete the
// refusal on exactly the run that needs it: `--exclude docs` removes the
// virtual table from Tables while every one of the module's shadow tables stays
// in the comparison. See stokaro/ptah#1028.
//
// The selector is still ASKED, so `--exclude docs` against such a database is
// not reported as having protected nothing. It names the virtual table that is
// still in this list even after the same name left Tables, and telling an
// operator their selector matched nothing while refusing the run because of the
// object it named would be two contradictory statements about one name.
func (s *exclusionState) noteUnregisteredVirtualTables(tables []catalog.VirtualTable) {
	for _, table := range tables {
		s.matches("table", s.nameCandidates(table.Schema, table.Name)...)
	}
}

// filterIndexes asks the patterns whether they name the index BEFORE the
// parent short-circuits decide, so a selector that names an index inside an
// already-excluded table still counts as having matched something. The keep
// decision is unchanged: an index whose table or column left the schema leaves
// with it either way. Every child filter below is ordered for the same reason.
//
// An index also leaves with the constraint it enforces: a primary key's, a
// UNIQUE constraint's, or a feature object's such as a PostgreSQL EXCLUDE
// constraint, which marks its index ManagedBy. removed holds the constraints
// this exclusion took out, and excludedFeature reports a feature object it took
// out. Kept on its own, the index is planned for a DROP INDEX that PostgreSQL
// refuses with SQLSTATE 2BP01 while the constraint stands, and the selector that
// protected the constraint fails the run.
func (s *exclusionState) filterIndexes(
	indexes []catalog.Index,
	removed []catalog.Constraint,
	excludedFeature func(objectidentity.ID) bool,
) []catalog.Index {
	return keep(indexes, func(index catalog.Index) bool {
		named := s.matches("index", s.childNameCandidates(index.Schema, index.TableName, index.Name)...)
		if s.tableExcluded(index.Schema, index.TableName) || s.anyColumnExcluded(index.Schema, index.TableName, index.Columns) {
			return false
		}
		if index.ManagedBy != (objectidentity.ID{}) && excludedFeature(index.ManagedBy) {
			return false
		}
		if slices.ContainsFunc(removed, func(constraint catalog.Constraint) bool {
			return indexbacking.Enforces(s.dialect, constraint, index)
		}) {
			return false
		}
		return !named
	})
}

// withoutForeignKeyBackingIndexes returns indexes without the index the server
// built for each foreign key in removed that a selector names, by the target's
// rule. schema is the whole read, so the rule sees every index that could back
// the key, kept or not. The rule claims only the index the server built: an
// index the author declared stays, even where the key stands on it, because
// the declaration owns it and a declaration that leaves it out asks for its
// removal. The server refuses that removal while the key needs the index, and
// the refusal is the answer the author gets.
//
// A selector that leaves every constraint out, such as `*[type=constraint]`,
// takes no index with a key. It is the selector that lets a read go without
// the privilege to see constraints, and MariaDB shows a SELECT-only account no
// foreign key, so the account could not tell which index a key stands on. A
// run's answer depends on its selectors, never on what its account could
// see, so neither account takes the index.
func (s *exclusionState) withoutForeignKeyBackingIndexes(
	indexes []catalog.Index,
	schema *catalog.Database,
	removed []catalog.Constraint,
	target databaseTarget,
) []catalog.Index {
	if target.foreignKeyIndexes == nil {
		return indexes
	}
	semantics := identifier.ForDialect(target.dialect)
	type indexKey struct{ table, name string }
	backing := make(map[indexKey]bool)
	for _, constraint := range removed {
		if indexbacking.KindOf(constraint.Type) != indexbacking.ForeignKey || !s.namesConstraint(constraint) {
			continue
		}
		for _, index := range target.foreignKeyIndexes.BackingIndexes(schema, constraint, semantics) {
			backing[indexKey{index.QualifiedTableName(), index.Name}] = true
		}
	}
	if len(backing) == 0 {
		return indexes
	}
	return keep(indexes, func(index catalog.Index) bool {
		return !backing[indexKey{index.QualifiedTableName(), index.Name}]
	})
}

// namesConstraint reports whether a selector names constraint, including one
// that leaves every constraint out.
func (s *exclusionState) namesConstraint(constraint catalog.Constraint) bool {
	candidates := s.childNameCandidates(constraint.Schema, constraint.TableName, constraint.Name)
	types := constraintTypes(constraint.Type)
	return slices.ContainsFunc(s.patterns, func(pattern resourcePattern) bool {
		return slices.ContainsFunc(types, func(resourceType string) bool { return pattern.matches(resourceType, candidates...) })
	})
}

// removedHiddenKeys returns the keys a selector names among those a read
// reports through an index alone: the primary key of a table whose read holds
// no PRIMARY KEY row, and, where a UNIQUE is an index, a unique index the read
// holds no row for. MariaDB shows a SELECT-only account no constraint row and
// every index, and a reader derives each column's key flags from the indexes.
// Asked of the rows alone, a selector naming the key would leave its flags and
// index in scope for that account and take them for one that sees the rows,
// and the two runs would plan differently.
func (s *exclusionState) removedHiddenKeys(schema *catalog.Database) []catalog.Constraint {
	type key struct{ table, name, kind string }
	read := make(map[key]bool)
	for _, constraint := range schema.Constraints {
		switch indexbacking.KindOf(constraint.Type) {
		case indexbacking.PrimaryKey:
			read[key{constraint.QualifiedTableName(), "", "PRIMARY KEY"}] = true
		case indexbacking.Unique:
			read[key{constraint.QualifiedTableName(), constraint.Name, "UNIQUE"}] = true
		case indexbacking.ForeignKey, indexbacking.None:
		}
	}
	uniqueIsIndex := keyNamings.Get(s.dialect).uniqueNamedAfterColumn
	var removed []catalog.Constraint
	for _, index := range schema.Indexes {
		var hidden catalog.Constraint
		switch {
		case index.IsPrimary && !read[key{index.QualifiedTableName(), "", "PRIMARY KEY"}]:
			hidden = catalog.Constraint{Type: "PRIMARY KEY"}
		case index.IsUnique && !index.IsPrimary && uniqueIsIndex && !read[key{index.QualifiedTableName(), index.Name, "UNIQUE"}]:
			hidden = catalog.Constraint{Type: "UNIQUE"}
		default:
			continue
		}
		hidden.Schema, hidden.TableName, hidden.Name, hidden.ColumnNames = index.Schema, index.TableName, index.Name, index.Columns
		if !s.keepConstraint(hidden) {
			removed = append(removed, hidden)
		}
	}
	return removed
}

// clearRemovedKeyColumns clears the flags a reader derives from a key on each
// column whose key the exclusion removed, unless a key it kept marks the column
// too: IsUnique from a UNIQUE constraint on one column, and IsPrimaryKey from
// the primary key. tables are the exclusion's own copies. Kept beside the
// removed constraint, a flag is compared alone, and `schema compare` reports
// `unique: true -> false` or `primary_key: true -> false` on the column on
// every run, a difference no statement reconciles. The desired side leaves out
// the key the same selector names; see
// [exclusionState.columnUniqueNameCandidates] and
// [exclusionState.excludeGeneratedPrimaryKeys]. Cleared on this side alone,
// the primary key flag would plan an ADD PRIMARY KEY on a table that holds one.
func clearRemovedKeyColumns(tables []catalog.Table, kept, removed []catalog.Constraint) {
	type columnKey struct{ table, column string }
	type columnKeys struct{ unique, primary map[columnKey]bool }
	keys := func(constraints []catalog.Constraint) columnKeys {
		found := columnKeys{unique: make(map[columnKey]bool), primary: make(map[columnKey]bool)}
		for _, constraint := range constraints {
			columns := constraint.ColumnNamesOrDefault()
			switch indexbacking.KindOf(constraint.Type) {
			case indexbacking.Unique:
				if len(columns) == 1 {
					found.unique[columnKey{constraint.QualifiedTableName(), columns[0]}] = true
				}
			case indexbacking.PrimaryKey:
				for _, column := range columns {
					found.primary[columnKey{constraint.QualifiedTableName(), column}] = true
				}
			case indexbacking.ForeignKey, indexbacking.None:
			}
		}
		return found
	}
	removedKeys, keptKeys := keys(removed), keys(kept)
	if len(removedKeys.unique) == 0 && len(removedKeys.primary) == 0 {
		return
	}
	for i := range tables {
		for j := range tables[i].Columns {
			key := columnKey{tables[i].QualifiedName(), tables[i].Columns[j].Name}
			if removedKeys.unique[key] && !keptKeys.unique[key] {
				tables[i].Columns[j].IsUnique = false
			}
			if removedKeys.primary[key] && !keptKeys.primary[key] {
				tables[i].Columns[j].IsPrimaryKey = false
			}
		}
	}
}

// filterConstraints returns the constraints the exclusion keeps, and the ones
// it removed, whose indexes [exclusionState.filterIndexes] removes with them.
func (s *exclusionState) filterConstraints(constraints []catalog.Constraint) (kept, removed []catalog.Constraint) {
	kept = keep(constraints, func(constraint catalog.Constraint) bool {
		keepConstraint := s.keepConstraint(constraint)
		if !keepConstraint {
			removed = append(removed, constraint)
		}
		return keepConstraint
	})
	return kept, removed
}

func (s *exclusionState) keepConstraint(constraint catalog.Constraint) bool {
	foreignSchema := foreignSchemaOrLocal(constraint)
	candidates := s.childNameCandidates(constraint.Schema, constraint.TableName, constraint.Name)
	named := s.matchesAny(constraintTypes(constraint.Type), candidates...)
	if s.tableExcluded(constraint.Schema, constraint.TableName) ||
		s.tableExcluded(foreignSchema, derefString(constraint.ForeignTable)) ||
		s.anyColumnExcluded(constraint.Schema, constraint.TableName, constraint.ColumnNamesOrDefault()) ||
		s.leftWithoutColumns(constraint.Schema, constraint.TableName) ||
		s.anyColumnExcluded(foreignSchema, derefString(constraint.ForeignTable), constraint.ForeignColumnsOrDefault()) {
		return false
	}
	return !named
}

func (s *exclusionState) filterFunctions(functions []catalog.Function) []catalog.Function {
	return keep(functions, func(function catalog.Function) bool {
		named := s.matches("function", s.nameCandidates(function.Schema, function.Name)...)
		return !named && !s.schemaExcluded(function.Schema)
	})
}

func (s *exclusionState) filterViews(views []catalog.View) []catalog.View {
	result := make([]catalog.View, 0, len(views))
	for _, view := range views {
		names := s.nameCandidates(view.Schema, view.Name)
		if s.matches("view", names...) || s.schemaExcluded(view.Schema) {
			s.excludeTable(view.Schema, view.Name)
			continue
		}
		if s.matchesField("view", "comment", names...) {
			view.Comment = ""
		}
		result = append(result, view)
	}
	return result
}

func (s *exclusionState) filterMatViews(views []catalog.MaterializedView) []catalog.MaterializedView {
	result := make([]catalog.MaterializedView, 0, len(views))
	for _, view := range views {
		names := s.nameCandidates(view.Schema, view.Name)
		if s.matches("materialized_view", names...) || s.schemaExcluded(view.Schema) {
			s.excludeTable(view.Schema, view.Name)
			continue
		}
		if s.matchesField("materialized_view", "comment", names...) {
			view.Comment = ""
		}
		result = append(result, view)
	}
	return result
}

func (s *exclusionState) filterTriggers(triggers []catalog.Trigger) []catalog.Trigger {
	return keep(triggers, func(trigger catalog.Trigger) bool {
		named := s.matches("trigger", s.childNameCandidates(trigger.Schema, trigger.Table, trigger.Name)...)
		if s.tableExcluded(trigger.Schema, trigger.Table) {
			return false
		}
		return !named
	})
}

func (s *exclusionState) filterRLSPolicies(policies []catalog.RLSPolicy) []catalog.RLSPolicy {
	return keep(policies, func(policy catalog.RLSPolicy) bool {
		schema, table := splitQualified(policy.Table)
		named := s.matches("policy", s.childNameCandidates(schema, table, policy.Name)...)
		if s.tableExcluded(schema, table) {
			return false
		}
		return !named
	})
}

func (s *exclusionState) filterRoles(roles []catalog.Role) []catalog.Role {
	return keep(roles, func(role catalog.Role) bool {
		return !s.matches("role", role.Name)
	})
}

// filterObjectOwners drops an ownership row whose owner or whose object is
// excluded.
//
// An ownership row names both ends a selector can reach: the role that owns
// and the object owned. Keeping a row for an excluded object would say who owns
// something this description no longer contains (stokaro/ptah#1950).
func (s *exclusionState) filterObjectOwners(
	owners []catalog.ObjectOwner,
) []catalog.ObjectOwner {
	return keep(owners, func(owner catalog.ObjectOwner) bool {
		if s.matches("role", owner.Owner) {
			return false
		}
		return !s.matches(owner.Kind, qualifiedNameCandidates(owner.Schema, owner.Name)...)
	})
}

// filterRoleMemberships drops an edge whose either end is excluded.
//
// A membership names no object of its own: it is a fact about two roles, and it
// is answered by the `role` selector on both sides. Keeping an edge whose role
// left would leave the description referring to a role it no longer defines,
// which is the same defect RolesOutOfScope exists to avoid on the other side
// (stokaro/ptah#1950).
func (s *exclusionState) filterRoleMemberships(
	memberships []catalog.RoleMembership,
) []catalog.RoleMembership {
	return keep(memberships, func(membership catalog.RoleMembership) bool {
		return !s.matches("role", membership.Role) && !s.matches("role", membership.Member)
	})
}

// filterWideAccess drops the access an excluded role holds, which the `role`
// selector answers as it does for a membership. Nil stays nil: it says the
// read did not establish the access, and an empty list would say the read
// found none, which lets the security analysis measure a superuser as
// reaching nothing.
func (s *exclusionState) filterWideAccess(access []catalog.WideAccess) []catalog.WideAccess {
	if access == nil {
		return nil
	}
	return keep(access, func(held catalog.WideAccess) bool { return !s.matches("role", held.Role) })
}

// filterWideGrants drops the grants an excluded role holds, which the `role`
// selector answers as it does for wide access. Nil stays nil: it says the
// read did not establish the grants, and an empty list would say it found
// none.
func (s *exclusionState) filterWideGrants(grants []catalog.WideGrant) []catalog.WideGrant {
	if grants == nil {
		return nil
	}
	return keep(grants, func(held catalog.WideGrant) bool { return !s.matches("role", held.Role) })
}

// excludedObjects reports, by identity, the feature objects of before that an
// exclusion took out of after.
func excludedObjects(before, after schemaext.Objects) func(objectidentity.ID) bool {
	return func(ref objectidentity.ID) bool {
		_, held, err := before.Get(ref)
		if err != nil || !held {
			return false
		}
		_, kept, err := after.Get(ref)
		return err == nil && !kept
	}
}

// objectGrantExcluded reports whether the owner's object a grant names, such
// as a YDB topic, left with an exclusion: the grant rides its object. The
// owner reads the grant's spelling; see [selectext.Set.GrantObject].
func (v vocabulary) objectGrantExcluded(excluded func(objectidentity.ID) bool, objectType, objectName string) bool {
	ref, ok := v.selections.GrantObject(objectType, objectName)
	return ok && excluded(ref)
}

func (s *exclusionState) filterGrants(grants []catalog.Grant, excluded func(objectidentity.ID) bool) []catalog.Grant {
	return keep(grants, func(grant catalog.Grant) bool {
		named := s.matches("grant", grant.QualifiedTarget(), grant.Role+"."+grant.QualifiedTarget())
		if strings.EqualFold(grant.ObjectType, "TABLE") && s.tableExcluded(grant.Schema, grant.ObjectName) {
			return false
		}
		if s.vocabulary.objectGrantExcluded(excluded, grant.ObjectType, grant.QualifiedTarget()) {
			return false
		}
		if strings.EqualFold(grant.ObjectType, "SEQUENCE") && s.sequenceExcluded(grant.Schema, grant.ObjectName) {
			return false
		}
		// A grant on a routine leaves with the routine, which filterFunctions
		// excludes by name, every overload together.
		if routineGrantObjectTypes[strings.ToUpper(grant.ObjectType)] &&
			s.matches("function", s.nameCandidates(grant.Schema, grant.ObjectName)...) {
			return false
		}
		// A grant rides the object it is on, so it leaves when that object's
		// schema leaves. For a SCHEMA grant the object IS the schema, named by
		// ObjectName; reading grant.Schema there would resolve an empty field
		// to the default schema and drop a grant on an unrelated schema.
		if strings.EqualFold(grant.ObjectType, "SCHEMA") {
			return !named && !s.schemaExcluded(grant.ObjectName)
		}
		return !named && (strings.EqualFold(grant.ObjectType, "DATABASE") || !s.schemaExcluded(grant.Schema))
	})
}

func (s *exclusionState) filterGeneratedTables(tables []schemamodel.Table) []schemamodel.Table {
	result := make([]schemamodel.Table, 0, len(tables))
	for _, table := range tables {
		names := s.nameCandidates(table.Schema, table.Name)
		if s.matches("table", names...) || s.schemaExcluded(table.Schema) {
			s.excludeTable(table.Schema, table.Name)
			// Recorded whichever way the table left, so a selector naming one
			// of its children is still asked rather than reported empty.
			s.removedGeneratedTables[table.StructName] = table
			continue
		}
		// The database side subtracts the same field, so both sides of a
		// comparison see the same comment and an excluded one is neither
		// planned nor dropped.
		if s.matchesField("table", "comment", names...) {
			table.Comment = ""
		}
		result = append(result, table)
	}
	return result
}

// filterGeneratedManagedData drops the rows a desired state declares for an
// excluded table. The table has left the other side too, so a row left here
// compares with nothing: `schema drift` reports it and `schema apply` plans its
// INSERT. A declaration can name a table the desired state does not define, so it
// is asked the question its table is asked instead of looking the table up.
func (s *exclusionState) filterGeneratedManagedData(declarations []schemamodel.ManagedData) []schemamodel.ManagedData {
	return keep(declarations, func(data schemamodel.ManagedData) bool {
		return !s.matches("table", s.nameCandidates(data.Schema, data.Table)...) && !s.schemaExcluded(data.Schema)
	})
}

// noteGeneratedChild asks the patterns whether they name a child of a generated
// table an earlier selector removed.
//
// Every generated child filter resolves its parent through the map of SURVIVING
// tables, so a removed parent makes the child unreachable and its selector
// reports as naming nothing -- while the same selector on the introspected side
// matches, because an index there carries its own schema and table. The keep
// decision is untouched: the child leaves with its parent either way.
func (s *exclusionState) noteGeneratedChild(resourceType, structName, tableName, childName string) {
	table, ok := generatedObjectTable(s.removedGeneratedTables, structName, tableName)
	if !ok {
		return
	}
	s.matches(resourceType, s.childNameCandidates(table.Schema, table.Name, childName)...)
}

// filterGeneratedFields returns the declared fields the exclusion keeps.
// removedKeys holds, by struct name, the columns of each primary key a
// selector named; see [exclusionState.leaveKey].
func (s *exclusionState) filterGeneratedFields(
	tables map[string]schemamodel.Table,
	fields []schemamodel.Field,
	removedKeys map[string]map[string]bool,
) []schemamodel.Field {
	result := make([]schemamodel.Field, 0, len(fields))
	for _, field := range fields {
		table, ok := tables[field.StructName]
		if !ok {
			s.noteGeneratedChild("column", field.StructName, "", field.Name)
			continue
		}
		if s.generatedFieldExcluded(table, field.Name) {
			s.excludeColumn(table.Schema, table.Name, field.Name)
			continue
		}
		if s.generatedForeignTableExcluded(table.Schema, field.Foreign) {
			field = stripGeneratedFieldForeignKey(field)
		}
		if s.generatedForeignColumnsExcluded(table.Schema, field.Foreign, foreignReferenceColumns(field.Foreign)) {
			field = stripGeneratedFieldForeignKey(field)
		}
		if field.Unique {
			candidates := s.columnUniqueNameCandidates(table, field.Name)
			if s.matchesAny(constraintTypes("UNIQUE"), candidates...) {
				field.Unique = false
			}
		}
		if removedKeys[field.StructName][field.Name] {
			field = s.leaveKey(field)
		}
		result = append(result, field)
	}
	return result
}

// columnUniqueNameCandidates are the names a selector can name a column's own
// UNIQUE by. The declaration gives it no name, and the server does: the
// column's name where the target's descriptor says a UNIQUE is an index the
// server names after its first column, as on the MySQL family, and
// `<table>_<column>_key` everywhere else, as on PostgreSQL. A selector that
// names it leaves the declaration out, as the database side leaves out the
// constraint and its index; kept, the comparison plans an ADD CONSTRAINT for a
// constraint the server holds.
//
// Only the target's name is offered. The other spelling can belong to an
// unrelated object on the same table, such as an index `code` on PostgreSQL,
// and a selector naming that object would leave the declared UNIQUE out while
// the server's constraint stays, so the plan would drop the constraint.
func (s *exclusionState) columnUniqueNameCandidates(table schemamodel.Table, column string) []string {
	if keyNamings.Get(s.dialect).uniqueNamedAfterColumn {
		return s.childNameCandidates(table.Schema, table.Name, column)
	}
	return s.childNameCandidates(table.Schema, table.Name, table.Name+"_"+column+"_key")
}

// excludeGeneratedPrimaryKeys leaves out the primary key of each table whose
// key a selector names, as the database side leaves out the server's key and
// its index, and returns the key's columns by struct name for
// [exclusionState.filterGeneratedFields] to clear on the fields. Kept, the
// comparison plans an ADD PRIMARY KEY on a table that holds one.
//
// The columns returned are the declared key's and the database's key's alike:
// the server's key stays, and so does the NOT NULL it holds on each of its
// columns, whatever the declaration says about the key. A table declaring no
// key is asked about the database's key only, so a selector naming the key of
// a table that holds none on either side reports that it named nothing.
func (s *exclusionState) excludeGeneratedPrimaryKeys(
	tables []schemamodel.Table,
	fields []schemamodel.Field,
) (kept []schemamodel.Table, removedKeys map[string]map[string]bool) {
	keyColumns := make(map[string]map[string]bool)
	addKeyColumn := func(structName, column string) {
		if keyColumns[structName] == nil {
			keyColumns[structName] = make(map[string]bool)
		}
		keyColumns[structName][column] = true
	}
	for _, field := range fields {
		if field.Primary {
			addKeyColumn(field.StructName, field.Name)
		}
	}
	for _, table := range tables {
		for _, column := range table.PrimaryKey {
			addKeyColumn(table.StructName, column)
		}
		for _, part := range table.PrimaryKeyParts {
			addKeyColumn(table.StructName, part.Name)
		}
		for _, column := range s.liveKeys.primaryColumns(s, table) {
			addKeyColumn(table.StructName, column)
		}
	}
	kept = slices.Clone(tables)
	removedKeys = make(map[string]map[string]bool)
	for i, table := range kept {
		if len(keyColumns[table.StructName]) == 0 {
			continue
		}
		candidates := s.primaryKeyNameCandidates(table)
		if !s.matchesAny(constraintTypes("PRIMARY KEY"), candidates...) {
			continue
		}
		removedKeys[table.StructName] = keyColumns[table.StructName]
		kept[i] = withoutPrimaryKey(table)
	}
	return kept, removedKeys
}

// leaveKey returns field as a column of a primary key the exclusion left
// out: not a key column, and NOT NULL. The server keeps the key and the NOT
// NULL it holds on the column. A declaration that wrote `id int PRIMARY KEY`
// left the NOT NULL to the key, and without it the comparison plans DROP NOT
// NULL, which PostgreSQL refuses while the key stands (SQLSTATE 42P16).
//
// A target whose key column can hold NULL, as SQLite's can, keeps the
// nullability the declaration wrote: the server holds no NOT NULL the key did
// not write, and adding one would refuse every NULL the column holds.
func (s *exclusionState) leaveKey(field schemamodel.Field) schemamodel.Field {
	field.Primary = false
	if !keyNamings.Get(s.dialect).primaryKeyNullable {
		field.Nullable = false
	}
	return field
}

// keyNaming is what a target's descriptor says about its server's keys: the
// names it gives one, and whether a key column holds NOT NULL.
type keyNaming struct {
	// uniqueNamedAfterColumn is IndexNaming.UniqueIsIndex: a UNIQUE is an
	// index, which the server names after its first column.
	uniqueNamedAfterColumn bool
	// primaryKeyName is Comparison.PrimaryKeyName, the name the server gives
	// every primary key.
	primaryKeyName string
	// primaryKeySuffix is IndexNaming.PrimaryKeySuffix, which names a
	// primary key the file leaves unnamed `<table>_<suffix>`.
	primaryKeySuffix string
	// primaryKeyNullable is Comparison.PrimaryKeyNullable: a key column holds
	// no NOT NULL the declaration did not write.
	primaryKeyNullable bool
}

// keyNamings derives each dialect's key naming once per registry. An unknown
// dialect gets the zero value: a UNIQUE named `<table>_<column>_key`, and a
// primary key named as the declaration names it.
var keyNamings = descriptortable.New(func(described descriptor.Dialect) keyNaming {
	return keyNaming{
		uniqueNamedAfterColumn: described.IndexNaming.UniqueIsIndex,
		primaryKeyName:         described.Comparison.PrimaryKeyName,
		primaryKeySuffix:       described.IndexNaming.PrimaryKeySuffix,
		primaryKeyNullable:     described.Comparison.PrimaryKeyNullable,
	}
})

// primaryKeyNameCandidates are the names a selector can name a table's primary
// key by, which is the name the target's server holds it under: the one it
// gives every primary key where the descriptor states one, `PRIMARY` on the
// MySQL family; otherwise the name the declaration gave the key or, for a key
// it named none, `<table>_<suffix>` where the descriptor states the suffix,
// `<table>_pkey` on PostgreSQL. A server that names an unnamed key in a way no
// file can predict, as SQL Server and Oracle do, offers no name for it, so a
// selector cannot name a key the declaration left unnamed there.
//
// Where the database holds a primary key on the table, its name is the only
// one offered. The comparison pairs the two keys whatever they are named, so
// the selector has to leave out both or neither: a name the declaration
// predicts for a key the server holds under another name, one an author gave
// it or one a renamed table kept, names nothing.
func (s *exclusionState) primaryKeyNameCandidates(table schemamodel.Table) []string {
	if name, held := s.liveKeys.primary(s, table); held {
		if name == "" {
			return nil
		}
		return s.childNameCandidates(table.Schema, table.Name, name)
	}
	naming := keyNamings.Get(s.dialect)
	name := naming.primaryKeyName
	if name == "" {
		name = table.PrimaryKeyName
	}
	if name == "" && naming.primaryKeySuffix != "" {
		name = table.Name + "_" + naming.primaryKeySuffix
	}
	if name == "" {
		return nil
	}
	return s.childNameCandidates(table.Schema, table.Name, name)
}

// withoutPrimaryKey returns table with no primary key and none of the settings
// that describe one.
func withoutPrimaryKey(table schemamodel.Table) schemamodel.Table {
	table.PrimaryKey = nil
	table.PrimaryKeyName = ""
	table.PrimaryKeyParts = nil
	table.PrimaryKeyInclude = nil
	table.PrimaryKeyMethod = ""
	table.PrimaryKeyFacets = schemaext.Facets{}
	table.PrimaryKeyComment = ""
	table.PrimaryKeyDeferrable = false
	table.PrimaryKeyInitially = ""
	return table
}

func (s *exclusionState) stripGeneratedTableColumnReferences(tables []schemamodel.Table) []schemamodel.Table {
	out := slices.Clone(tables)
	for i := range out {
		out[i].PrimaryKey = s.filterGeneratedColumnNames(out[i], out[i].PrimaryKey)
		out[i].PrimaryKeyParts = keep(out[i].PrimaryKeyParts, func(part schemamodel.PrimaryKeyPart) bool {
			return !s.generatedColumnExcluded(out[i], part.Name)
		})
		out[i].PrimaryKeyInclude = s.filterGeneratedColumnNames(out[i], out[i].PrimaryKeyInclude)
	}
	return out
}

// filterGeneratedIndexes is [exclusionState.filterIndexes] for the desired
// side. A declared index leaves with a declared UNIQUE constraint of its name
// on its table, as the index the server builds for that constraint leaves on
// the database side: removed holds the constraints the exclusion took out.
func (s *exclusionState) filterGeneratedIndexes(
	tables map[string]schemamodel.Table,
	indexes []schemamodel.Index,
	removed []schemamodel.Constraint,
) []schemamodel.Index {
	return keep(indexes, func(index schemamodel.Index) bool {
		table, ok := generatedIndexTable(tables, index)
		if !ok {
			s.noteGeneratedChild("index", index.StructName, index.TableName, index.Name)
			return false
		}
		named := s.matches("index", s.childNameCandidates(table.Schema, table.Name, index.Name)...)
		if s.generatedAnyColumnExcluded(table, generatedIndexColumns(index)) {
			return false
		}
		if index.Unique && slices.ContainsFunc(removed, func(constraint schemamodel.Constraint) bool {
			owner, ok := generatedConstraintTable(tables, constraint)
			return ok && indexbacking.KindOf(constraint.Type) == indexbacking.Unique &&
				constraint.Name == index.Name && owner.Schema == table.Schema && owner.Name == table.Name
		}) {
			return false
		}
		return !named
	})
}

// filterGeneratedConstraints returns the declared constraints the exclusion
// keeps, and the ones it removed, whose indexes
// [exclusionState.filterGeneratedIndexes] removes with them.
func (s *exclusionState) filterGeneratedConstraints(
	tables map[string]schemamodel.Table,
	constraints []schemamodel.Constraint,
) (kept, removed []schemamodel.Constraint) {
	kept = keep(constraints, func(constraint schemamodel.Constraint) bool {
		keepConstraint := s.keepGeneratedConstraint(tables, constraint)
		if !keepConstraint {
			removed = append(removed, constraint)
		}
		return keepConstraint
	})
	return kept, removed
}

func (s *exclusionState) keepGeneratedConstraint(tables map[string]schemamodel.Table, constraint schemamodel.Constraint) bool {
	table, ok := generatedConstraintTable(tables, constraint)
	if !ok {
		for _, resourceType := range constraintTypes(constraint.Type) {
			s.noteGeneratedChild(resourceType, constraint.StructName, constraint.Table, constraint.Name)
		}
		return false
	}
	candidates := s.childNameCandidates(table.Schema, table.Name, constraint.Name)
	named := s.matchesAny(constraintTypes(constraint.Type), candidates...)
	if s.generatedAnyColumnExcluded(table, generatedConstraintColumns(constraint)) {
		return false
	}
	if s.generatedForeignTableExcluded(table.Schema, constraint.ForeignTable) ||
		s.generatedForeignColumnsExcluded(table.Schema, constraint.ForeignTable, constraint.ForeignColumnsOrDefault()) {
		return false
	}
	return !named
}

func (s *exclusionState) filterGeneratedEmbeddedFields(
	tables map[string]schemamodel.Table,
	fields []schemamodel.EmbeddedField,
) []schemamodel.EmbeddedField {
	return keep(fields, func(field schemamodel.EmbeddedField) bool {
		table, ok := tables[field.StructName]
		if !ok {
			s.noteGeneratedChild("column", field.StructName, "", field.Field)
			return false
		}
		if field.Field != "" && s.generatedFieldExcluded(table, field.Field) {
			s.excludeColumn(table.Schema, table.Name, field.Field)
			return false
		}
		if s.generatedForeignTableExcluded(table.Schema, field.Ref) {
			return false
		}
		if s.generatedForeignColumnsExcluded(table.Schema, field.Ref, foreignReferenceColumns(field.Ref)) {
			return false
		}
		return true
	})
}

// filterGeneratedEnums mirrors the database-side enum exclusion, so excluding
// an enum removes it from both sides of a comparison. Current enum models keep
// schema and name separately; SQL-source legacy values may still carry the
// qualifier in Name when Schema is empty.
func (s *exclusionState) filterGeneratedEnums(enums []schemamodel.Enum) []schemamodel.Enum {
	return keep(enums, func(enum schemamodel.Enum) bool {
		schema, name := enumIdentity(enum.Schema, enum.Name)
		named := s.matches("enum", s.nameCandidates(schema, name)...)
		return !named && !s.schemaExcluded(schema)
	})
}

// filterGeneratedSequences is the desired-side mirror of filterSequences.
// Both sides of a comparison must subtract the same objects: a selector that
// removed a sequence from the introspected side alone would turn a protected
// object into a CREATE.
//
// A sequence carries a Schema field of its own, exactly as a generated table
// does, so the candidates come from nameCandidates rather than from a
// "schema." prefix.
func (s *exclusionState) filterGeneratedSequences(sequences []schemamodel.Sequence) []schemamodel.Sequence {
	return keep(sequences, func(sequence schemamodel.Sequence) bool {
		excluded := s.matches("sequence", s.nameCandidates(sequence.Schema, sequence.Name)...) ||
			s.schemaExcluded(sequence.Schema) || s.ownerExcluded(sequence.Schema, sequence.OwnedBy)
		if excluded {
			s.excludeSequence(sequence.Schema, sequence.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterGeneratedFunctions(functions []schemamodel.Function) []schemamodel.Function {
	return keep(functions, func(function schemamodel.Function) bool {
		named := s.matches("function", s.qualifiedNameCandidatesFor(function.Name)...)
		return !named && !s.qualifiedSchemaExcluded(function.Name)
	})
}

// filterGeneratedViews mirrors the introspected view exclusion. A generated
// view carries its schema only as an optional "schema." prefix on its name, so
// the candidates are resolved from that prefix and the default schema, keeping
// both sides of a comparison subtracting the same views.
func (s *exclusionState) filterGeneratedViews(views []schemamodel.View) []schemamodel.View {
	result := make([]schemamodel.View, 0, len(views))
	for _, view := range views {
		names := s.qualifiedNameCandidatesFor(view.Name)
		if s.matches("view", names...) || s.qualifiedSchemaExcluded(view.Name) {
			s.excludeTable("", view.Name)
			continue
		}
		if s.matchesField("view", "comment", names...) {
			view.Comment = ""
		}
		result = append(result, view)
	}
	return result
}

func (s *exclusionState) filterGeneratedMaterializedViews(views []schemamodel.MaterializedView) []schemamodel.MaterializedView {
	result := make([]schemamodel.MaterializedView, 0, len(views))
	for _, view := range views {
		names := s.qualifiedNameCandidatesFor(view.Name)
		if s.matches("materialized_view", names...) || s.qualifiedSchemaExcluded(view.Name) {
			s.excludeTable("", view.Name)
			continue
		}
		if s.matchesField("materialized_view", "comment", names...) {
			view.Comment = ""
		}
		result = append(result, view)
	}
	return result
}

func (s *exclusionState) filterGeneratedTriggers(
	tables map[string]schemamodel.Table,
	triggers []schemamodel.Trigger,
) []schemamodel.Trigger {
	return keep(triggers, func(trigger schemamodel.Trigger) bool {
		table, ok := generatedObjectTable(tables, trigger.StructName, trigger.Table)
		if !ok {
			s.noteGeneratedChild("trigger", trigger.StructName, trigger.Table, trigger.Name)
			return false
		}
		return !s.matches("trigger", s.childNameCandidates(table.Schema, table.Name, trigger.Name)...)
	})
}

func (s *exclusionState) filterGeneratedRLSPolicies(
	tables map[string]schemamodel.Table,
	policies []schemamodel.RLSPolicy,
) []schemamodel.RLSPolicy {
	return keep(policies, func(policy schemamodel.RLSPolicy) bool {
		table, ok := generatedObjectTable(tables, policy.StructName, policy.Table)
		if !ok {
			s.noteGeneratedChild("policy", policy.StructName, policy.Table, policy.Name)
			return false
		}
		return !s.matches("policy", s.childNameCandidates(table.Schema, table.Name, policy.Name)...)
	})
}

func (s *exclusionState) filterGeneratedRLSEnabledTables(
	tables map[string]schemamodel.Table,
	values []schemamodel.RLSEnabledTable,
) []schemamodel.RLSEnabledTable {
	return keep(values, func(value schemamodel.RLSEnabledTable) bool {
		_, ok := generatedObjectTable(tables, value.StructName, value.Table)
		return ok
	})
}

func (s *exclusionState) filterGeneratedRoles(roles []schemamodel.Role) []schemamodel.Role {
	return keep(roles, func(role schemamodel.Role) bool {
		return !s.matches("role", role.Name)
	})
}

func (s *exclusionState) filterGeneratedGrants(grants []schemamodel.Grant, excluded func(objectidentity.ID) bool) []schemamodel.Grant {
	return keep(grants, func(grant schemamodel.Grant) bool {
		named := s.matches("grant", generatedGrantTargets(grant)...)
		if grant.OnTable != "" && generatedTableKeyExcluded(s, grant.OnTable) {
			return false
		}
		if grant.OnObject != "" && (s.vocabulary.objectGrantExcluded(excluded, grant.ObjectKind, grant.OnObject) ||
			s.qualifiedSchemaExcluded(grant.OnObject)) {
			return false
		}
		// Mirrors filterGrants: a grant leaves with the schema of the object it
		// is on, and for a schema grant that object is the schema itself.
		if grant.OnSchema != "" && s.schemaExcluded(grant.OnSchema) {
			return false
		}
		if grant.OnTable != "" && s.qualifiedSchemaExcluded(grant.OnTable) {
			return false
		}
		if grant.OnSequence != "" && (s.qualifiedSchemaExcluded(grant.OnSequence) ||
			generatedSequenceKeyExcluded(s, grant.OnSequence)) {
			return false
		}
		if grant.OnRoutine != "" && (s.qualifiedSchemaExcluded(grant.OnRoutine) ||
			s.matches("function", s.qualifiedNameCandidatesFor(grant.OnRoutine)...)) {
			return false
		}
		return !named
	})
}

// routineGrantObjectTypes are the object types a catalog read reports for a
// privilege on a function or procedure.
var routineGrantObjectTypes = map[string]bool{"FUNCTION": true, "PROCEDURE": true}

func (s *exclusionState) matches(resourceType string, names ...string) bool {
	return s.matchesAny([]string{resourceType}, names...)
}

// matchesField and matchesAny deliberately visit every pattern instead of
// returning on the first hit. The return value only needs the first, but the
// match marks do not: two selectors that name the same object are both honored,
// and stopping early would report the second as having matched nothing.
func (s *exclusionState) matchesField(resourceType, field string, names ...string) bool {
	return s.matchesAnyField([]string{resourceType}, field, names...)
}

func (s *exclusionState) matchesAnyField(resourceTypes []string, field string, names ...string) bool {
	found := false
	for i, pattern := range s.patterns {
		for _, resourceType := range resourceTypes {
			if pattern.matchesField(resourceType, field, names...) {
				s.matched[i] = true
				found = true
				break
			}
		}
		if !s.matched[i] && s.storedSpelling(i) {
			stored := pattern
			stored.glob = s.storedGlobs[i]
			s.storedMatched[i] = slices.ContainsFunc(resourceTypes, func(resourceType string) bool {
				return stored.matchesField(resourceType, field, names...)
			})
		}
	}
	return found
}

// foldSelectors records the glob of each pattern in fold's case, so that a
// selector that names nothing as written can say which spelling names an
// object. The empty fold records nothing.
func (s *exclusionState) foldSelectors(fold identifier.Fold) {
	if fold == "" {
		return
	}
	s.storedGlobs = make([]string, len(s.patterns))
	s.storedMatched = make([]bool, len(s.patterns))
	for i, pattern := range s.patterns {
		if folded := fold.Apply(pattern.glob); folded != pattern.glob {
			s.storedGlobs[i] = folded
		}
	}
}

// storedSpelling reports whether patterns[i] has a stored spelling that has
// not named an object yet.
func (s *exclusionState) storedSpelling(i int) bool {
	return s.storedGlobs != nil && s.storedGlobs[i] != "" && !s.storedMatched[i]
}

// storedSelectors maps each unmatched selector whose stored spelling named an
// object to that spelling, written as the selector with its letters outside
// brackets in fold's case.
func (s *exclusionState) storedSelectors(fold identifier.Fold) map[string]string {
	var stored map[string]string
	for i, pattern := range s.patterns {
		if s.matched[i] || s.storedMatched == nil || !s.storedMatched[i] {
			continue
		}
		if stored == nil {
			stored = make(map[string]string)
		}
		stored[pattern.raw] = foldOutsideBrackets(pattern.raw, fold)
	}
	return stored
}

// foldOutsideBrackets writes selector with its letters outside square
// brackets in fold's case, leaving a type or field modifier as written.
func foldOutsideBrackets(selector string, fold identifier.Fold) string {
	var out strings.Builder
	depth := 0
	start := 0
	for index, r := range selector {
		switch {
		case r == '[':
			if depth == 0 {
				out.WriteString(fold.Apply(selector[start:index]))
				start = index
			}
			depth++
		case r == ']' && depth > 0:
			depth--
			if depth == 0 {
				out.WriteString(selector[start : index+1])
				start = index + 1
			}
		}
	}
	if depth == 0 {
		out.WriteString(fold.Apply(selector[start:]))
	} else {
		out.WriteString(selector[start:])
	}
	return out.String()
}

func (s *exclusionState) matchesAny(resourceTypes []string, names ...string) bool {
	found := false
	for i, pattern := range s.patterns {
		for _, resourceType := range resourceTypes {
			if pattern.matches(resourceType, names...) {
				s.matched[i] = true
				found = true
				break
			}
		}
		if !s.matched[i] && s.storedSpelling(i) {
			stored := pattern
			stored.glob = s.storedGlobs[i]
			s.storedMatched[i] = slices.ContainsFunc(resourceTypes, func(resourceType string) bool {
				return stored.matches(resourceType, names...)
			})
		}
	}
	return found
}

func (s *exclusionState) excludeSchema(name string) {
	if name != "" {
		s.excludedSchemas[name] = struct{}{}
	}
}

// schemaExcluded reports whether the schema owning an object left. An object
// with no schema of its own lives in the default schema, so it is the same
// object the schema-qualified spelling names and it leaves with that schema.
func (s *exclusionState) schemaExcluded(schema string) bool {
	name := s.effectiveSchema(schema)
	if name == "" {
		return false
	}
	_, excluded := s.excludedSchemas[name]
	return excluded
}

func (s *exclusionState) extensionSchemaExcluded(schema string) bool {
	name := s.effectiveExtensionSchema(schema)
	if name == "" {
		return false
	}
	_, excluded := s.excludedSchemas[name]
	return excluded
}

// qualifiedSchemaExcluded is [exclusionState.schemaExcluded] for objects whose
// only schema qualification is an optional "schema." prefix on the name --
// generated enums, functions, extensions and views.
func (s *exclusionState) qualifiedSchemaExcluded(name string) bool {
	schema, _ := splitQualified(name)
	return s.schemaExcluded(schema)
}

// excludeTable and tableExcluded key a table in its effective schema, so a
// table written `public.audit` and an object that names it `audit` are one
// table under the default schema `public`. A feature object's owning table
// arrives with an empty schema where its source left the schema out.
func (s *exclusionState) excludeTable(schema, table string) {
	if key, ok := tableIdentityKey(s.effectiveSchema(schema), table); ok {
		s.excludedTables[key] = struct{}{}
	}
}

func (s *exclusionState) tableExcluded(schema, table string) bool {
	key, ok := tableIdentityKey(s.effectiveSchema(schema), table)
	if !ok {
		return false
	}
	_, excluded := s.excludedTables[key]
	return excluded
}

// excludeSequence and sequenceExcluded key a sequence in its effective schema:
// a grant can name the sequence with or without the schema its column's table
// is written in.
func (s *exclusionState) excludeSequence(schema, name string) {
	if key, ok := sequenceIdentityKey(s.effectiveSchema(schema), name); ok {
		s.excludedSequences[key] = struct{}{}
	}
}

func (s *exclusionState) sequenceExcluded(schema, name string) bool {
	key, ok := sequenceIdentityKey(s.effectiveSchema(schema), name)
	if !ok {
		return false
	}
	_, excluded := s.excludedSequences[key]
	return excluded
}

func (s *exclusionState) excludeColumn(schema, table, column string) {
	if key, ok := columnIdentityKey(schema, table, column); ok {
		s.excludedColumns[key] = struct{}{}
	}
}

func (s *exclusionState) anyColumnExcluded(schema, table string, columns []string) bool {
	for _, column := range columns {
		key, ok := columnIdentityKey(schema, table, column)
		if !ok {
			continue
		}
		if _, excluded := s.excludedColumns[key]; excluded {
			return true
		}
	}
	return false
}

func (s *exclusionState) generatedFieldExcluded(table schemamodel.Table, column string) bool {
	return s.matches("column", s.childNameCandidates(table.Schema, table.Name, column)...)
}

func (s *exclusionState) generatedColumnExcluded(table schemamodel.Table, column string) bool {
	return s.anyColumnExcluded(table.Schema, table.Name, []string{column}) ||
		s.generatedFieldExcluded(table, column)
}

func (s *exclusionState) generatedAnyColumnExcluded(table schemamodel.Table, columns []string) bool {
	for _, column := range columns {
		if s.generatedColumnExcluded(table, column) {
			return true
		}
	}
	return false
}

func (s *exclusionState) filterGeneratedColumnNames(table schemamodel.Table, columns []string) []string {
	return keep(columns, func(column string) bool {
		return !s.generatedColumnExcluded(table, column)
	})
}

func (s *exclusionState) generatedForeignTableExcluded(localSchema, reference string) bool {
	table, _, _ := strings.Cut(strings.TrimSpace(reference), "(")
	schema, name := splitQualified(table)
	if schema == "" {
		schema = localSchema
	}
	return s.tableExcluded(schema, name)
}

func (s *exclusionState) generatedForeignColumnsExcluded(localSchema, reference string, columns []string) bool {
	table, _, _ := strings.Cut(strings.TrimSpace(reference), "(")
	schema, name := splitQualified(table)
	if schema == "" {
		schema = localSchema
	}
	return s.anyColumnExcluded(schema, name, columns)
}

func stripGeneratedFieldForeignKey(field schemamodel.Field) schemamodel.Field {
	field.Foreign = ""
	field.ForeignKeyName = ""
	field.OnDelete = ""
	field.OnUpdate = ""
	return field
}

func cloneDatabase(schema *catalog.Database) *catalog.Database {
	return &catalog.Database{
		ReservedNames:   slices.Clone(schema.ReservedNames),
		FeatureObjects:  schema.FeatureObjects,
		FeatureCoverage: schema.FeatureCoverage,
		Schemas:         slices.Clone(schema.Schemas),
		Tables:          slices.Clone(schema.Tables),
		Enums:           slices.Clone(schema.Enums),
		Indexes:         slices.Clone(schema.Indexes),
		Constraints:     slices.Clone(schema.Constraints),
		Functions:       slices.Clone(schema.Functions),
		Sequences:       slices.Clone(schema.Sequences),
		Views:           slices.Clone(schema.Views),
		MatViews:        slices.Clone(schema.MatViews),
		Triggers:        slices.Clone(schema.Triggers),
		RLSPolicies:     slices.Clone(schema.RLSPolicies),
		Roles:           slices.Clone(schema.Roles),
		Grants:          slices.Clone(schema.Grants),
		RoleMemberships: slices.Clone(schema.RoleMemberships),
		WideAccess:      slices.Clone(schema.WideAccess),
		WideGrants:      slices.Clone(schema.WideGrants),
		ObjectOwners:    cloneObjectOwners(schema.ObjectOwners),
		// Which roles the server has is a fact about the server, not part of
		// the description a filter narrows. Dropping it here would tell the
		// comparator that every cluster role outside the description is
		// absent. See stokaro/ptah#1267.
		RolesOutOfScope:       slices.Clone(schema.RolesOutOfScope),
		RolesOutOfScopeReason: schema.RolesOutOfScopeReason,
		// Filtering narrows what a description contains; it does not widen what
		// the description claimed to cover. This is a field-by-field
		// constructor, which is exactly the shape that drops a new field in
		// silence, so the carry is asserted by
		// TestScopeDatabaseKeepsCoverage (stokaro/ptah#1276).
		NotDescribed: schema.NotDescribed,
		// Which virtual tables the reading build could not classify is a fact
		// about the read, and narrowing cannot make it untrue. Dropping it here
		// would defeat the refusal it feeds precisely on the run that needs it:
		// the tables at risk are the module's shadow tables, not the virtual
		// table an operator excludes, so an exclusion that removed this signal
		// would leave every dangerous row in the comparison and take away the
		// only thing that knew. See stokaro/ptah#1028.
		UnregisteredVirtualTables: slices.Clone(schema.UnregisteredVirtualTables),
		// What the read left out stays what it left out: a filter narrows the
		// description, and these default privileges were never in it.
		// Where the database is does not change with what a filter keeps of
		// it, and a YDB plan names a Serial column's sequence by that path.
		DatabasePath: schema.DatabasePath,
		// Who read it does not change either. Routine replacement checks its
		// definer against this account, and a saved plan's fingerprint follows
		// this role's memberships whatever the filter keeps.
		CurrentAccount:         schema.CurrentAccount,
		CurrentDatabase:        schema.CurrentDatabase,
		ConnectedRole:          schema.ConnectedRole,
		ConnectedRoleSuperuser: schema.ConnectedRoleSuperuser,
		// The names a filter keeps are spelled as the read spelled them, so
		// whether that is the stored spelling does not change either.
		StoredNames: schema.StoredNames,
		// A filter keeps a prediction a prediction.
		Predicted: schema.Predicted,
		// A table the read found and left out is still in the database, and a
		// filter cannot change that: dropping the record would let a declared
		// table at it plan a CREATE TABLE the database rejects.
		UnreadTables: slices.Clone(schema.UnreadTables),
		// The filters narrow these to the run's selection; see
		// [exclusionState.filterUnmodeledObjects].
		UnmodeledObjects: slices.Clone(schema.UnmodeledObjects),
		// An object that blocks a table's removal is still in the database
		// whatever a filter keeps, and a plan that drops a table the filter
		// keeps must still see it.
		RemovalBlockers: slices.Clone(schema.RemovalBlockers),
		// The planning context is what a filtered read established outside its
		// scope, for planning only. A filter narrows the compared description
		// and leaves the context as it is; without it a provider's plan cannot
		// prove what lies outside the scope.
		PlanningContextObjects:  schema.PlanningContextObjects,
		PlanningContextCoverage: schema.PlanningContextCoverage,
		// What an earlier selector took out of a kept table stays out of
		// scope; see [outOfScopeParts].
		OutOfScope: schema.OutOfScope,
		// The grants a filter drops from WideGrants are an excluded role's,
		// and the ones the read was refused stay unread.
		WideGrantsPartial: schema.WideGrantsPartial,
	}
}

func cloneObjectOwners(owners []catalog.ObjectOwner) []catalog.ObjectOwner {
	result := slices.Clone(owners)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
}

func cloneGenerated(schema *schemamodel.Database) *schemamodel.Database {
	filtered := *schema
	filtered.Schemas = slices.Clone(schema.Schemas)
	filtered.Tables = slices.Clone(schema.Tables)
	filtered.Fields = slices.Clone(schema.Fields)
	filtered.Indexes = slices.Clone(schema.Indexes)
	filtered.Constraints = slices.Clone(schema.Constraints)
	filtered.Enums = slices.Clone(schema.Enums)
	filtered.EmbeddedFields = slices.Clone(schema.EmbeddedFields)
	filtered.Functions = slices.Clone(schema.Functions)
	filtered.Sequences = slices.Clone(schema.Sequences)
	filtered.Views = slices.Clone(schema.Views)
	filtered.MaterializedViews = slices.Clone(schema.MaterializedViews)
	filtered.Triggers = slices.Clone(schema.Triggers)
	filtered.RLSPolicies = slices.Clone(schema.RLSPolicies)
	filtered.RLSEnabledTables = slices.Clone(schema.RLSEnabledTables)
	filtered.Roles = slices.Clone(schema.Roles)
	filtered.Grants = slices.Clone(schema.Grants)
	filtered.RevokedGrants = slices.Clone(schema.RevokedGrants)
	// A shallow struct copy carries a slice header, so a field left out here is
	// not dropped -- it is ALIASED, and the filter below then writes through the
	// caller's own state.
	return &filtered
}

func cloneTable(table catalog.Table) catalog.Table {
	table.Columns = slices.Clone(table.Columns)
	return table
}

func keep[T any](values []T, keepValue func(T) bool) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		if keepValue(value) {
			result = append(result, value)
		}
	}
	return result
}

func tableChildNameCandidates(schema, table, child string) []string {
	table = strings.TrimSpace(table)
	child = strings.TrimSpace(child)
	if table == "" || child == "" {
		return nil
	}
	qualifiedTable := catalog.QualifyTableName(schema, table)
	if qualifiedTable == table {
		return []string{table + "." + child}
	}
	return []string{table + "." + child, qualifiedTable + "." + child}
}

func qualifiedNameCandidates(schema, name string) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	qualified := catalog.QualifyTableName(schema, name)
	if qualified == name {
		return []string{name}
	}
	return []string{name, qualified}
}

// storedSpelling is the name a catalog stores for a name part a generated
// state spells as a statement does, on an engine whose UnquotedFold is fold
// (see [Scope.Dialect]). A part in double quotes is stored as written inside
// them, and a bare part in the fold's case: a state converted from a read, or
// declared in a SQL file, names the table stored as orders `"orders"`, and the
// one stored as ORDERS orders or ORDERS. Matched as spelled, the converted
// side offers `"orders"` and `APP."""orders"""`, a declared bare orders meets
// the selector orders while the read's ORDERS does not, and a plan drops or
// creates the table the selector meant to leave alone. Only the generated
// side of such an engine is respelled: a read reports stored names, and on
// another engine a stored name may hold quote characters of its own. An empty
// part, which leaves the default schema in force, is returned as it is.
func storedSpelling(part string, fold identifier.Fold) string {
	part = strings.TrimSpace(part)
	if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
		return sqlident.UnquoteDoubleQuoted(part)
	}
	return fold.Apply(part)
}

// unquotedFold is the UnquotedFold of dialect's descriptor, empty when the
// dialect states none or the caller names no dialect.
func unquotedFold(dialect string) identifier.Fold {
	if dialect == "" {
		return ""
	}
	return identifier.ForDialect(dialect).UnquotedFold
}

func extensionNameCandidates(schema, name string) []string {
	if strings.TrimSpace(name) == "" {
		return nil
	}
	qualified := tableref.CanonicalExact(schema, name)
	if qualified == name {
		return []string{name}
	}
	return []string{name, qualified}
}

func schemaNameCandidates(name string) []string {
	if name == "" {
		return nil
	}
	quoted := tableref.CanonicalExact("", name)
	if quoted == name {
		return []string{name}
	}
	// Catalogs such as SQL Server preserve the spelling of an ordinary
	// unquoted schema name. Keep that spelling addressable without losing the
	// quoted candidate required for PostgreSQL case and whitespace identities.
	return []string{name, quoted}
}

func generatedTableByStruct(tables []schemamodel.Table) map[string]schemamodel.Table {
	byStruct := make(map[string]schemamodel.Table, len(tables))
	for _, table := range tables {
		byStruct[table.StructName] = table
	}
	return byStruct
}

func generatedIndexTable(tables map[string]schemamodel.Table, index schemamodel.Index) (schemamodel.Table, bool) {
	return generatedObjectTable(tables, index.StructName, index.TableName)
}

func generatedConstraintTable(tables map[string]schemamodel.Table, constraint schemamodel.Constraint) (schemamodel.Table, bool) {
	return generatedObjectTable(tables, constraint.StructName, constraint.Table)
}

func generatedObjectTable(
	tables map[string]schemamodel.Table,
	structName,
	tableName string,
) (schemamodel.Table, bool) {
	if table, ok := tables[structName]; ok && tableMatchesName(table, tableName) {
		return table, true
	}
	if strings.TrimSpace(tableName) == "" {
		return schemamodel.Table{}, false
	}
	for _, table := range tables {
		if tableMatchesName(table, tableName) {
			return table, true
		}
	}
	return schemamodel.Table{}, false
}

func generatedIndexColumns(index schemamodel.Index) []string {
	columns := slices.Clone(index.Fields)
	for _, part := range index.Parts {
		if part.Name != "" {
			columns = append(columns, part.Name)
		}
	}
	columns = append(columns, index.IncludeColumns...)
	return columns
}

func generatedConstraintColumns(constraint schemamodel.Constraint) []string {
	columns := slices.Clone(constraint.Columns)
	columns = append(columns, constraint.IncludeColumns...)
	return columns
}

func foreignReferenceColumns(reference string) []string {
	_, after, ok := strings.Cut(strings.TrimSpace(reference), "(")
	if !ok {
		return nil
	}
	columns, _, _ := strings.Cut(after, ")")
	result := make([]string, 0)
	for column := range strings.SplitSeq(columns, ",") {
		column = strings.TrimSpace(column)
		if column != "" {
			result = append(result, column)
		}
	}
	return result
}

func tableMatchesName(table schemamodel.Table, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || name == table.QualifiedName() {
		return true
	}
	ref, ok := tableref.Parse(name)
	return ok && !ref.Qualified && ref.Name == table.Name
}

func generatedTableKeyExcluded(s *exclusionState, table string) bool {
	schema, name := splitQualified(table)
	return s.tableExcluded(schema, name)
}

func generatedGrantTargets(grant schemamodel.Grant) []string {
	switch {
	case grant.OnSchema != "":
		return []string{grant.OnSchema, grant.Role + "." + grant.OnSchema}
	case grant.OnTable != "":
		return []string{grant.OnTable, grant.Role + "." + grant.OnTable}
	case grant.OnSequence != "":
		return []string{grant.OnSequence, grant.Role + "." + grant.OnSequence}
	case grant.OnRoutine != "":
		return []string{grant.OnRoutine, grant.Role + "." + grant.OnRoutine}
	case grant.OnObject != "":
		return []string{grant.OnObject, grant.Role + "." + grant.OnObject}
	default:
		return nil
	}
}

// tableIdentityKey reports the identity of a table, or that the caller did not
// name one.
//
// An unnamed table is refused rather than keyed as the empty name, because a
// map holding one would answer "excluded" for every other object that also
// arrived without a name.
func tableIdentityKey(schema, table string) (tableIdentity, bool) {
	if strings.TrimSpace(table) == "" {
		return tableIdentity{}, false
	}
	return exactIdentities.TableParts(schema, table).Key(), true
}

// sequenceIdentityKey reports the identity of a sequence.
//
// It is separate from [tableIdentityKey] even though both are (schema, name):
// keeping the kind means a sequence and a table sharing a name are two objects
// in the model rather than two objects that happen to live in separate maps.
func sequenceIdentityKey(schema, name string) (tableIdentity, bool) {
	if strings.TrimSpace(name) == "" {
		return tableIdentity{}, false
	}
	return exactIdentities.SchemaScopedParts(objectidentity.KindSequence, schema, name).Key(), true
}

func columnIdentityKey(schema, table, column string) (columnIdentity, bool) {
	if strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return columnIdentity{}, false
	}
	return exactIdentities.ColumnParts(schema, table, column).Key(), true
}

func splitQualified(value string) (schema, name string) {
	ref, ok := tableref.Parse(value)
	if !ok || !ref.Qualified {
		return "", value
	}
	return ref.Schema, ref.Name
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func foreignSchemaOrLocal(constraint catalog.Constraint) string {
	if strings.TrimSpace(constraint.ForeignSchema) != "" {
		return constraint.ForeignSchema
	}
	return constraint.Schema
}

func tableResourceTypes(table catalog.Table) []string {
	types := []string{"table"}
	tableType := strings.ToLower(strings.TrimSpace(table.Type))
	tableType = strings.ReplaceAll(tableType, " ", "_")
	if tableType != "" && tableType != "table" {
		types = append(types, tableType)
	}
	return types
}

// report is the state's [ExcludeReport].
func (s *exclusionState) report() ExcludeReport {
	return ExcludeReport{Unmatched: s.unmatchedSelectors(), Stored: s.storedSelectors(unquotedFold(s.dialect))}
}

// leftWithoutColumns reports whether a selector left table without a column.
// A constraint of such a table leaves with its columns, as one over an
// excluded column does, including a CHECK whose columns the read could not
// tell: every column it could read is out of scope.
func (s *exclusionState) leftWithoutColumns(schema, table string) bool {
	key, ok := tableIdentityKey(s.effectiveSchema(schema), table)
	if !ok {
		return false
	}
	_, columnless := s.columnless[key]
	return columnless
}

// constraintTypes are the selector types that name a constraint of
// constraintType, on the database side and the desired side alike.
//
// Every constraint answers `constraint`. A foreign key also answers
// `foreign_key`, `foreign-key` and `fk`, the type the community binary names
// it by, and a CHECK constraint answers `check`, the type that binary names it
// by. A primary key and a UNIQUE constraint answer `index` as well: the server
// enforces each with an index that carries the constraint's name, and the
// community binary names a UNIQUE constraint as that index. Answering only
// under `constraint`, the constraint stays in scope when a selector removes its
// index, and the plan drops the constraint the selector was written to keep.
//
// The community binary keeps no constraint under `[type=constraint]`, and no
// primary key under any selector: it accepts the selector and plans the DROP.
// Ptah keeps what the selector names, deliberately, and records the
// divergence on the retained-divergences page. On SQLite, which changes a
// constraint only by writing a new table, the planner carries a constraint the
// selector keeps out of scope into the new table from its live definition.
func constraintTypes(constraintType string) []string {
	types := []string{"constraint"}
	switch indexbacking.KindOf(constraintType) {
	case indexbacking.ForeignKey:
		types = append(types, "foreign_key", "foreign-key", "fk")
	case indexbacking.PrimaryKey, indexbacking.Unique:
		types = append(types, "index")
	case indexbacking.None:
		if strings.EqualFold(strings.TrimSpace(constraintType), "CHECK") {
			types = append(types, "check")
		}
	}
	return types
}
