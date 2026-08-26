// Package atlasfilter applies Atlas-style resource filters to introspected schemas.
package atlasfilter

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/tableref"
)

// ExcludeDatabase returns a shallow copy of schema with resources matching
// Atlas-style exclude globs removed. Objects that carry no schema are treated
// as unqualified; use [ExcludeDatabaseWithDefaultSchema] when the connection
// names the schema those objects live in.
func ExcludeDatabase(schema *catalog.Database, patterns []string) (*catalog.Database, error) {
	return ExcludeDatabaseWithDefaultSchema(schema, patterns, "")
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
	schema *catalog.Database,
	patterns []string,
	defaultSchema string,
) (*catalog.Database, error) {
	filtered, _, err := ExcludeDatabaseReport(schema, patterns, defaultSchema)
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
	// in the state they were applied to. It is an outcome, not a shape: the
	// selectors here are well formed, they simply matched nothing.
	Unmatched []string
}

// ExcludeDatabaseReport is [ExcludeDatabaseWithDefaultSchema] plus the report.
// The filtering is identical; only the extra return value is new, so callers
// that do not care about emptiness keep the two-value form.
func ExcludeDatabaseReport(
	schema *catalog.Database,
	patterns []string,
	defaultSchema string,
) (*catalog.Database, ExcludeReport, error) {
	return excludeDatabase(schema, patterns, defaultSchema, defaultSchema)
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
	return excludeDatabase(schema, scope.Exclude, scope.patternScopeSchema(), scope.DefaultSchema)
}

func excludeDatabase(
	schema *catalog.Database,
	patterns []string,
	patternScope, defaultSchema string,
) (*catalog.Database, ExcludeReport, error) {
	filters, err := parsePatterns(patterns, resolvedDepthScope(patternScope))
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if schema == nil || len(filters) == 0 {
		return schema, unfilteredReport(filters), nil
	}

	state := newExclusionState(filters, defaultSchema)
	filtered := cloneDatabase(schema)
	// Schemas go first: every filter below asks whether its object's schema
	// left, and that answer has to exist before the first object is judged.
	filtered.Schemas = state.filterSchemas(filtered.Schemas)
	filtered.Tables = state.filterTables(filtered.Tables)
	filtered.Enums = state.filterEnums(filtered.Enums)
	filtered.Sequences = state.filterSequences(filtered.Sequences)
	filtered.Domains = state.filterDomains(filtered.Domains)
	filtered.Composites = state.filterComposites(filtered.Composites)
	filtered.Ranges = state.filterRanges(filtered.Ranges)
	filtered.Indexes = state.filterIndexes(filtered.Indexes)
	filtered.Constraints = state.filterConstraints(filtered.Constraints)
	filtered.Extensions = state.filterExtensions(filtered.Extensions)
	filtered.Functions = state.filterFunctions(filtered.Functions)
	filtered.Views = state.filterViews(filtered.Views)
	filtered.Synonyms = state.filterSynonyms(filtered.Synonyms)
	filtered.ExtendedProperties = state.filterExtendedProperties(filtered.ExtendedProperties)
	filtered.ContinuousAggregates = state.filterContinuousAggregates(filtered.ContinuousAggregates)
	filtered.Hypertables = state.filterHypertables(filtered.Hypertables)
	filtered.MatViews = state.filterMatViews(filtered.MatViews)
	filtered.Triggers = state.filterTriggers(filtered.Triggers)
	filtered.RLSPolicies = state.filterRLSPolicies(filtered.RLSPolicies)
	filtered.Roles = state.filterRoles(filtered.Roles)
	filtered.RoleMemberships = state.filterRoleMemberships(filtered.RoleMemberships)
	filtered.ObjectOwners = state.filterObjectOwners(filtered.ObjectOwners)
	filtered.Grants = state.filterGrants(filtered.Grants)
	state.noteRolesOutOfScope(filtered.RolesOutOfScope)
	state.noteUnregisteredVirtualTables(filtered.UnregisteredVirtualTables)
	return filtered, ExcludeReport{Unmatched: state.unmatchedSelectors()}, nil
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
func ExcludeGenerated(schema *schemamodel.Database, patterns []string) (*schemamodel.Database, error) {
	return ExcludeGeneratedWithDefaultSchema(schema, patterns, "")
}

// ExcludeGeneratedWithDefaultSchema is [ExcludeGenerated] told which schema
// owns the objects the generated IR left unqualified.
//
// Both sides of a comparison must subtract the same objects: a pattern that
// removed a table from the introspected side but not from the desired side
// would turn a filtered-out object into a CREATE.
func ExcludeGeneratedWithDefaultSchema(
	schema *schemamodel.Database,
	patterns []string,
	defaultSchema string,
) (*schemamodel.Database, error) {
	filtered, _, err := ExcludeGeneratedReport(schema, patterns, defaultSchema)
	return filtered, err
}

// ExcludeGeneratedReport is [ExcludeGeneratedWithDefaultSchema] plus the
// [ExcludeReport] for the generated side.
func ExcludeGeneratedReport(
	schema *schemamodel.Database,
	patterns []string,
	defaultSchema string,
) (*schemamodel.Database, ExcludeReport, error) {
	return excludeGenerated(schema, patterns, defaultSchema, defaultSchema)
}

// ExcludeGeneratedScopeReport is [ExcludeDatabaseScopeReport] for the generated
// side, so both sides of a comparison count a pattern the same way.
func ExcludeGeneratedScopeReport(
	schema *schemamodel.Database,
	scope Scope,
) (*schemamodel.Database, ExcludeReport, error) {
	return excludeGenerated(schema, scope.Exclude, scope.patternScopeSchema(), scope.DefaultSchema)
}

func excludeGenerated(
	schema *schemamodel.Database,
	patterns []string,
	patternScope, defaultSchema string,
) (*schemamodel.Database, ExcludeReport, error) {
	filters, err := parsePatterns(patterns, resolvedDepthScope(patternScope))
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if schema == nil || len(filters) == 0 {
		return schema, unfilteredReport(filters), nil
	}

	state := newExclusionState(filters, defaultSchema)
	filtered := cloneGenerated(schema)
	// Same ordering rule as the database side, and the same reason both sides
	// have this pass at all: a schema removed from one side only would come
	// back as a CREATE SCHEMA for the object the selector was written to
	// protect.
	filtered.Schemas = state.filterGeneratedSchemas(filtered.Schemas)
	filtered.Tables = state.filterGeneratedTables(filtered.Tables)
	tableByStruct := generatedTableByStruct(filtered.Tables)
	filtered.Fields = state.filterGeneratedFields(tableByStruct, filtered.Fields)
	filtered.Tables = state.stripGeneratedTableColumnReferences(filtered.Tables)
	filtered.Indexes = state.filterGeneratedIndexes(tableByStruct, filtered.Indexes)
	filtered.Constraints = state.filterGeneratedConstraints(tableByStruct, filtered.Constraints)
	filtered.EmbeddedFields = state.filterGeneratedEmbeddedFields(tableByStruct, filtered.EmbeddedFields)
	filtered.Enums = state.filterGeneratedEnums(filtered.Enums)
	filtered.Sequences = state.filterGeneratedSequences(filtered.Sequences)
	filtered.Domains = state.filterGeneratedDomains(filtered.Domains)
	filtered.CompositeTypes = state.filterGeneratedCompositeTypes(filtered.CompositeTypes)
	filtered.Ranges = state.filterGeneratedRanges(filtered.Ranges)
	filtered.Extensions = state.filterGeneratedExtensions(filtered.Extensions)
	filtered.Functions = state.filterGeneratedFunctions(filtered.Functions)
	filtered.Views = state.filterGeneratedViews(filtered.Views)
	filtered.MaterializedViews = state.filterGeneratedMaterializedViews(filtered.MaterializedViews)
	filtered.Triggers = state.filterGeneratedTriggers(tableByStruct, filtered.Triggers)
	filtered.RLSPolicies = state.filterGeneratedRLSPolicies(tableByStruct, filtered.RLSPolicies)
	filtered.RLSEnabledTables = state.filterGeneratedRLSEnabledTables(tableByStruct, filtered.RLSEnabledTables)
	filtered.Roles = state.filterGeneratedRoles(filtered.Roles)
	filtered.Grants = state.filterGeneratedGrants(filtered.Grants)
	filtered.Dependencies = nil
	filtered.FunctionDependencies = nil
	filtered.SelfReferencingForeignKeys = nil
	schemamodel.Finalize(filtered)
	return filtered, ExcludeReport{Unmatched: state.unmatchedSelectors()}, nil
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
func ValidateExcludeSelectors(values []string) error {
	_, err := parsePatterns(values, depthScope{})
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

func parsePatterns(values []string, scope depthScope) ([]resourcePattern, error) {
	var patterns []resourcePattern
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			pattern, err := parsePattern(part, filterKindExclude)
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
// Which scope applies is the caller's answer to give, and it used to not be
// asked: defaultSchema arrived non-empty on every run, so a realm-scoped one
// was counted against a schema it was not relative to and the qualified column
// form was refused everywhere. See [Scope.RealmRelativePatterns].
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

func parsePattern(value, kind string) (resourcePattern, error) {
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
		selector, err := parseTypeSelector(body, open, kind)
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

func parseTypeSelector(raw string, open int, kind string) (typeSelector, error) {
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
	fields, err := parseFieldSelector(raw[closeIdx+1:], types, kind)
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
	"synonym":           {"comment": {}},
}

// parseFieldSelector resolves the `.field` suffix behind a type selector into
// the set of fields the pattern subtracts. `.*` names every field Ptah can
// subtract for the selected types.
func parseFieldSelector(suffix string, types map[string]struct{}, kind string) (map[string]struct{}, error) {
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
		for honored := range excludeFieldSelectors[resourceType] {
			if field == "*" || field == honored {
				fields[honored] = struct{}{}
			}
		}
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf(
			"unsupported Atlas %s field selector %q: Ptah refuses a field selector it would have to ignore; %s",
			kind, suffix, supportedFieldsMessage(types))
	}
	return fields, nil
}

// supportedFieldsMessage names the fields the selected types do support, so a
// refusal says what would have worked.
func supportedFieldsMessage(types map[string]struct{}) string {
	supported := make(map[string]struct{})
	for resourceType := range types {
		for honored := range excludeFieldSelectors[resourceType] {
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
	patterns []resourcePattern
	// defaultSchema owns objects that carry no schema of their own. It is the
	// connection's schema for introspected states and the dialect default for
	// file-backed ones; empty means "no default", which restores the
	// bare-name-only candidate set.
	defaultSchema string
	// matched[i] records whether patterns[i] ever named an object during this
	// run. Emptiness is an outcome rather than a shape -- a selector can be
	// perfectly well formed and still name nothing -- so it cannot be decided
	// by reading the selector text, only by watching the match.
	matched         []bool
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

func newExclusionState(patterns []resourcePattern, defaultSchema string) *exclusionState {
	return &exclusionState{
		patterns:               patterns,
		defaultSchema:          strings.TrimSpace(defaultSchema),
		matched:                make([]bool, len(patterns)),
		excludedTables:         make(map[tableIdentity]struct{}),
		excludedColumns:        make(map[columnIdentity]struct{}),
		excludedSchemas:        make(map[string]struct{}),
		excludedSequences:      make(map[tableIdentity]struct{}),
		removedGeneratedTables: make(map[string]schemamodel.Table),
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
	return tableChildNameCandidates(s.effectiveSchema(schema), table, child)
}

// filterSchemas subtracts the schemas an exclude selector names.
//
// Schemas were the last collection this package cloned and never offered to
// the patterns. That made two things wrong at once. A selector naming a real
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
		table.Columns = s.filterColumns(table, table.Columns)
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

// filterSequences, filterDomains, filterComposites and filterRanges subtract
// the four top-level kinds the exclusion used to clone and never ask about.
//
// The include projection already selects all four by name
// ([scopeSelection.projectDatabaseTopLevel] and projectDatabaseSupport), so
// `--include positive_int` picked the domain while `--exclude positive_int` was
// a silent no-op that still planned DROP DOMAIN for the object the user wrote
// the selector to protect -- the same destructive miss this file fixes for
// enums and functions. It is also what makes the unmatched-selector report
// truthful: a selector can only be called empty by a filter that asked it.
//
// The resource-type names match the include side exactly: sequence, domain,
// composite_type, range.
func (s *exclusionState) filterSequences(sequences []catalog.Sequence) []catalog.Sequence {
	return keep(sequences, func(sequence catalog.Sequence) bool {
		excluded := s.matches("sequence", s.nameCandidates(sequence.Schema, sequence.Name)...) ||
			s.schemaExcluded(sequence.Schema)
		if excluded {
			s.excludeSequence(sequence.Schema, sequence.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterDomains(domains []catalog.Domain) []catalog.Domain {
	return keep(domains, func(domain catalog.Domain) bool {
		named := s.matches("domain", s.nameCandidates(domain.Schema, domain.Name)...)
		return !named && !s.schemaExcluded(domain.Schema)
	})
}

func (s *exclusionState) filterComposites(composites []catalog.CompositeType) []catalog.CompositeType {
	return keep(composites, func(composite catalog.CompositeType) bool {
		named := s.matches("composite_type", s.nameCandidates(composite.Schema, composite.Name)...)
		return !named && !s.schemaExcluded(composite.Schema)
	})
}

func (s *exclusionState) filterRanges(ranges []catalog.Range) []catalog.Range {
	return keep(ranges, func(value catalog.Range) bool {
		named := s.matches("range", s.nameCandidates(value.Schema, value.Name)...)
		return !named && !s.schemaExcluded(value.Schema)
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
func (s *exclusionState) filterIndexes(indexes []catalog.Index) []catalog.Index {
	return keep(indexes, func(index catalog.Index) bool {
		named := s.matches("index", s.childNameCandidates(index.Schema, index.TableName, index.Name)...)
		if s.tableExcluded(index.Schema, index.TableName) || s.anyColumnExcluded(index.Schema, index.TableName, index.Columns) {
			return false
		}
		return !named
	})
}

func (s *exclusionState) filterConstraints(constraints []catalog.Constraint) []catalog.Constraint {
	return keep(constraints, func(constraint catalog.Constraint) bool {
		foreignSchema := foreignSchemaOrLocal(constraint)
		named := s.matchesAny(constraintResourceTypes(constraint), s.childNameCandidates(constraint.Schema, constraint.TableName, constraint.Name)...)
		if s.tableExcluded(constraint.Schema, constraint.TableName) ||
			s.tableExcluded(foreignSchema, derefString(constraint.ForeignTable)) ||
			s.anyColumnExcluded(constraint.Schema, constraint.TableName, constraint.ColumnNamesOrDefault()) ||
			s.anyColumnExcluded(foreignSchema, derefString(constraint.ForeignTable), constraint.ForeignColumnsOrDefault()) {
			return false
		}
		return !named
	})
}

func (s *exclusionState) filterExtensions(extensions []catalog.Extension) []catalog.Extension {
	result := make([]catalog.Extension, 0, len(extensions))
	for _, extension := range extensions {
		names := extensionNameCandidates(s.effectiveExtensionSchema(extension.Schema), extension.Name)
		if s.matches("extension", names...) || s.extensionSchemaExcluded(extension.Schema) {
			continue
		}
		if s.matchesField("extension", "version", names...) {
			extension.Version = ""
		}
		result = append(result, extension)
	}
	return result
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

// filterSynonyms drops synonyms an exclusion selector names, and synonyms
// whose own schema is excluded.
//
// The selector matches the ALIAS, never the target. A synonym excluded because
// its target was excluded would be a different rule: the alias is the object
// this schema declares, and the target may not even be in this database.
func (s *exclusionState) filterSynonyms(synonyms []catalog.Synonym) []catalog.Synonym {
	result := make([]catalog.Synonym, 0, len(synonyms))
	for _, synonym := range synonyms {
		names := s.nameCandidates(synonym.Schema, synonym.Name)
		if s.matches("synonym", names...) || s.schemaExcluded(synonym.Schema) {
			s.excludeTable(synonym.Schema, synonym.Name)
			continue
		}
		if s.matchesField("synonym", "comment", names...) {
			synonym.Comment = ""
		}
		result = append(result, synonym)
	}
	return result
}

// filterExtendedProperties drops the extended properties an exclusion selector
// names, and the ones whose owner is excluded.
//
// The second rule is what makes this more than a name match. An extended
// property is not an independent object: SQL Server drops it with the table it
// hangs off, so a description that excluded `docs` and kept a property
// addressed to `docs` would carry a row naming an object the same description
// says is not there -- and a comparison would then plan
// sp_dropextendedproperty against a table that is not in the plan.
//
// The property is still offered to the patterns under its own name, so
// `--exclude ptah_flag` is reported as having matched something rather than as
// a selector that protects nothing.
func (s *exclusionState) filterExtendedProperties(
	properties []catalog.ExtendedProperty,
) []catalog.ExtendedProperty {
	result := make([]catalog.ExtendedProperty, 0, len(properties))
	for _, property := range properties {
		names := s.nameCandidates(property.Schema, property.Name)
		if s.matches("extended_property", names...) {
			continue
		}
		// A database-scoped property is in no schema, so excluding a schema
		// does not exclude it. Asking schemaExcluded("") would resolve the
		// empty name to the connection's own schema and drop the property
		// whenever THAT one is excluded, which is a different object entirely.
		if property.Schema != "" && s.schemaExcluded(property.Schema) {
			continue
		}
		if property.Table != "" && s.tableExcluded(property.Schema, property.Table) {
			continue
		}
		result = append(result, property)
	}
	return result
}

// filterContinuousAggregates drops the continuous aggregates an exclusion
// selector names, and the ones whose schema is excluded.
//
// Excluding one is meaningful even though Ptah does not manage it: the
// description carries the aggregate so a comparison can decline a declaration
// that wants its name, and an operator who excludes it is saying they want
// that refusal to go away. The object stays on the server either way.
func (s *exclusionState) filterContinuousAggregates(
	aggregates []catalog.ContinuousAggregate,
) []catalog.ContinuousAggregate {
	result := make([]catalog.ContinuousAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		names := s.nameCandidates(aggregate.Schema, aggregate.Name)
		if s.matches("continuous_aggregate", names...) || s.schemaExcluded(aggregate.Schema) {
			continue
		}
		result = append(result, aggregate)
	}
	return result
}

// filterHypertables drops the hypertable rows whose TABLE this exclusion
// removed.
//
// A hypertable has no selector of its own, and deliberately so: it is not a
// second object beside the table but a fact about it, so `--exclude conditions`
// has already said everything there is to say. Giving it one would let a
// description carry a hypertable row for a table it does not describe, and the
// note built on that row would name a statement the reader cannot see
// (stokaro/ptah#1026).
func (s *exclusionState) filterHypertables(
	hypertables []catalog.Hypertable,
) []catalog.Hypertable {
	return keep(hypertables, func(hypertable catalog.Hypertable) bool {
		return !s.tableExcluded(hypertable.Schema, hypertable.Name)
	})
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
// An ownership row names two things a selector can reach: the role that owns
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

func (s *exclusionState) filterGrants(grants []catalog.Grant) []catalog.Grant {
	return keep(grants, func(grant catalog.Grant) bool {
		named := s.matches("grant", grant.QualifiedTarget(), grant.Role+"."+grant.QualifiedTarget())
		if strings.EqualFold(grant.ObjectType, "TABLE") && s.tableExcluded(grant.Schema, grant.ObjectName) {
			return false
		}
		if strings.EqualFold(grant.ObjectType, "SEQUENCE") && s.sequenceExcluded(grant.Schema, grant.ObjectName) {
			return false
		}
		// A grant rides the object it is on, so it leaves when that object's
		// schema leaves. For a SCHEMA grant the object IS the schema, named by
		// ObjectName; reading grant.Schema there would resolve an empty field
		// to the default schema and drop a grant on an unrelated schema.
		if strings.EqualFold(grant.ObjectType, "SCHEMA") {
			return !named && !s.schemaExcluded(grant.ObjectName)
		}
		return !named && !s.schemaExcluded(grant.Schema)
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

func (s *exclusionState) filterGeneratedFields(
	tables map[string]schemamodel.Table,
	fields []schemamodel.Field,
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
		result = append(result, field)
	}
	return result
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

func (s *exclusionState) filterGeneratedIndexes(
	tables map[string]schemamodel.Table,
	indexes []schemamodel.Index,
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
		return !named
	})
}

func (s *exclusionState) filterGeneratedConstraints(
	tables map[string]schemamodel.Table,
	constraints []schemamodel.Constraint,
) []schemamodel.Constraint {
	return keep(constraints, func(constraint schemamodel.Constraint) bool {
		table, ok := generatedConstraintTable(tables, constraint)
		if !ok {
			for _, resourceType := range generatedConstraintResourceTypes(constraint) {
				s.noteGeneratedChild(resourceType, constraint.StructName, constraint.Table, constraint.Name)
			}
			return false
		}
		named := s.matchesAny(generatedConstraintResourceTypes(constraint), s.childNameCandidates(table.Schema, table.Name, constraint.Name)...)
		if s.generatedAnyColumnExcluded(table, generatedConstraintColumns(constraint)) {
			return false
		}
		if s.generatedForeignTableExcluded(table.Schema, constraint.ForeignTable) ||
			s.generatedForeignColumnsExcluded(table.Schema, constraint.ForeignTable, constraint.ForeignColumnsOrDefault()) {
			return false
		}
		return !named
	})
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

// filterGeneratedSequences, filterGeneratedDomains,
// filterGeneratedCompositeTypes and filterGeneratedRanges are the desired-side
// mirrors of the four kinds added to the database side. Both sides of a
// comparison must subtract the same objects: a selector that removed a domain
// from the introspected side alone would turn a protected object into a CREATE.
//
// These carry a Schema field of their own, exactly as generated tables do, so
// the candidates come from nameCandidates rather than from a "schema." prefix.
func (s *exclusionState) filterGeneratedSequences(sequences []schemamodel.Sequence) []schemamodel.Sequence {
	return keep(sequences, func(sequence schemamodel.Sequence) bool {
		excluded := s.matches("sequence", s.nameCandidates(sequence.Schema, sequence.Name)...) ||
			s.schemaExcluded(sequence.Schema)
		if excluded {
			s.excludeSequence(sequence.Schema, sequence.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterGeneratedDomains(domains []schemamodel.Domain) []schemamodel.Domain {
	return keep(domains, func(domain schemamodel.Domain) bool {
		named := s.matches("domain", s.nameCandidates(domain.Schema, domain.Name)...)
		return !named && !s.schemaExcluded(domain.Schema)
	})
}

func (s *exclusionState) filterGeneratedCompositeTypes(types []schemamodel.CompositeType) []schemamodel.CompositeType {
	return keep(types, func(composite schemamodel.CompositeType) bool {
		named := s.matches("composite_type", s.nameCandidates(composite.Schema, composite.Name)...)
		return !named && !s.schemaExcluded(composite.Schema)
	})
}

func (s *exclusionState) filterGeneratedRanges(ranges []schemamodel.Range) []schemamodel.Range {
	return keep(ranges, func(value schemamodel.Range) bool {
		named := s.matches("range", s.nameCandidates(value.Schema, value.Name)...)
		return !named && !s.schemaExcluded(value.Schema)
	})
}

func (s *exclusionState) filterGeneratedExtensions(extensions []schemamodel.Extension) []schemamodel.Extension {
	result := make([]schemamodel.Extension, 0, len(extensions))
	for _, extension := range extensions {
		names := extensionNameCandidates(s.effectiveExtensionSchema(extension.Schema), extension.Name)
		if s.matches("extension", names...) || s.extensionSchemaExcluded(extension.Schema) {
			continue
		}
		if s.matchesField("extension", "version", names...) {
			extension.Version = ""
		}
		result = append(result, extension)
	}
	return result
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

func (s *exclusionState) filterGeneratedGrants(grants []schemamodel.Grant) []schemamodel.Grant {
	return keep(grants, func(grant schemamodel.Grant) bool {
		named := s.matches("grant", generatedGrantTargets(grant)...)
		if grant.OnTable != "" && generatedTableKeyExcluded(s, grant.OnTable) {
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
		if grant.OnSequence != "" && s.qualifiedSchemaExcluded(grant.OnSequence) {
			return false
		}
		return !named
	})
}

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
	}
	return found
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

func (s *exclusionState) excludeTable(schema, table string) {
	if key, ok := tableIdentityKey(schema, table); ok {
		s.excludedTables[key] = struct{}{}
	}
}

func (s *exclusionState) tableExcluded(schema, table string) bool {
	key, ok := tableIdentityKey(schema, table)
	if !ok {
		return false
	}
	_, excluded := s.excludedTables[key]
	return excluded
}

func (s *exclusionState) excludeSequence(schema, name string) {
	if key, ok := sequenceIdentityKey(schema, name); ok {
		s.excludedSequences[key] = struct{}{}
	}
}

func (s *exclusionState) sequenceExcluded(schema, name string) bool {
	key, ok := sequenceIdentityKey(schema, name)
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
		Schemas:              slices.Clone(schema.Schemas),
		Tables:               slices.Clone(schema.Tables),
		Enums:                slices.Clone(schema.Enums),
		Indexes:              slices.Clone(schema.Indexes),
		Constraints:          slices.Clone(schema.Constraints),
		Extensions:           slices.Clone(schema.Extensions),
		Functions:            slices.Clone(schema.Functions),
		Sequences:            slices.Clone(schema.Sequences),
		Domains:              slices.Clone(schema.Domains),
		Composites:           slices.Clone(schema.Composites),
		Ranges:               slices.Clone(schema.Ranges),
		Views:                slices.Clone(schema.Views),
		Synonyms:             slices.Clone(schema.Synonyms),
		ExtendedProperties:   slices.Clone(schema.ExtendedProperties),
		ContinuousAggregates: slices.Clone(schema.ContinuousAggregates),
		Hypertables:          slices.Clone(schema.Hypertables),
		MatViews:             slices.Clone(schema.MatViews),
		Triggers:             slices.Clone(schema.Triggers),
		RLSPolicies:          slices.Clone(schema.RLSPolicies),
		Roles:                slices.Clone(schema.Roles),
		Grants:               slices.Clone(schema.Grants),
		RoleMemberships:      slices.Clone(schema.RoleMemberships),
		ObjectOwners:         slices.Clone(schema.ObjectOwners),
		// Which roles the server has is a fact about the server, not part of
		// the description a filter narrows. Dropping it here would tell the
		// comparator that every cluster role outside the description is
		// absent. See stokaro/ptah#1267.
		RolesOutOfScope: slices.Clone(schema.RolesOutOfScope),
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
	}
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
	filtered.Extensions = slices.Clone(schema.Extensions)
	filtered.Functions = slices.Clone(schema.Functions)
	filtered.Sequences = slices.Clone(schema.Sequences)
	filtered.Domains = slices.Clone(schema.Domains)
	filtered.CompositeTypes = slices.Clone(schema.CompositeTypes)
	filtered.Ranges = slices.Clone(schema.Ranges)
	filtered.Views = slices.Clone(schema.Views)
	filtered.Synonyms = slices.Clone(schema.Synonyms)
	filtered.ExtendedProperties = slices.Clone(schema.ExtendedProperties)
	filtered.MaterializedViews = slices.Clone(schema.MaterializedViews)
	filtered.Triggers = slices.Clone(schema.Triggers)
	filtered.RLSPolicies = slices.Clone(schema.RLSPolicies)
	filtered.RLSEnabledTables = slices.Clone(schema.RLSEnabledTables)
	filtered.Roles = slices.Clone(schema.Roles)
	filtered.Grants = slices.Clone(schema.Grants)
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

func constraintResourceTypes(constraint catalog.Constraint) []string {
	types := []string{"constraint"}
	if strings.EqualFold(constraint.Type, "FOREIGN KEY") {
		types = append(types, "foreign_key", "foreign-key")
	}
	return types
}

func generatedConstraintResourceTypes(constraint schemamodel.Constraint) []string {
	types := []string{"constraint"}
	if strings.EqualFold(constraint.Type, "FOREIGN KEY") {
		types = append(types, "foreign_key", "foreign-key")
	}
	return types
}
