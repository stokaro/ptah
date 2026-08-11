package atlasfilter

import (
	"fmt"
	"maps"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemascope"
)

// Scope is the positive selection applied to Atlas schema apply and diff.
//
// Composition order is fixed: --schema names define the schema universe,
// --include selectors pick top-level resources inside that universe, and
// exclude patterns (--exclude plus atlas.hcl env.schema.mode) subtract from
// the result. The final projection is then validated: a selected object that
// depends on an unselected or excluded object refuses the projection with a
// [*CrossScopeError] instead of producing an incomplete plan.
type Scope struct {
	// Schemas restricts the projection to the named schemas. Repeated and
	// comma-separated values union deterministically. Empty keeps all schemas.
	Schemas []string
	// Include restricts the projection to top-level resources matched by the
	// Atlas-style selectors. Repeated and comma-separated values union
	// deterministically. Empty keeps every resource in the schema universe.
	Include []string
	// Exclude subtracts Atlas-style exclude patterns from the positive
	// selection.
	Exclude []string
	// DefaultSchema owns unqualified objects ("public" for PostgreSQL, the
	// database name for MySQL-family targets, "main" for SQLite).
	DefaultSchema string
}

// Positive reports whether the scope carries a positive selection. A scope
// with only Exclude patterns is not positive: callers keep the plain
// exclusion path for it.
func (s Scope) Positive() bool {
	if len(schemascope.SplitNames(s.Schemas)) > 0 {
		return true
	}
	for _, value := range s.Include {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

// CrossScopeError reports selected objects that depend on objects outside the
// positive selection. Emitting a plan for such a projection could produce SQL
// that references missing objects or silently drops dependencies, so the
// projection refuses instead.
type CrossScopeError struct {
	// Diagnostics are sorted, deduplicated human-readable dependency
	// violations.
	Diagnostics []string
}

func (e *CrossScopeError) Error() string {
	var out strings.Builder
	out.WriteString("the --schema/--include selection drops objects that selected objects depend on:")
	for _, diagnostic := range e.Diagnostics {
		out.WriteString("\n  - ")
		out.WriteString(diagnostic)
	}
	out.WriteString("\nadd the missing objects to the selection or exclude the dependent objects")
	return out.String()
}

// includeSelectableTypes are the top-level resource kinds an --include
// selector can name with [type=...]. Child resources (columns, indexes,
// constraints, triggers, policies, grants) ride along with their parent and
// are not independently includable.
var includeSelectableTypes = map[string]struct{}{
	"table":             {},
	"view":              {},
	"materialized_view": {},
	"function":          {},
	"enum":              {},
	"extension":         {},
	"sequence":          {},
	"domain":            {},
	"composite_type":    {},
	"range":             {},
	"role":              {},
}

// Resource is one top-level identity offered to Atlas selector matching.
// Types contains every selector type that names the same resource; for
// example, a foreign table answers both table and foreign_table selectors.
type Resource struct {
	Types  []string
	Schema string
	Name   string
}

// ScopeResources applies schema/include/exclude matching to arbitrary
// top-level identities while preserving input order. It exists for catalogs
// such as schema-clean plans whose writer-owned kinds do not all have a field
// in DBSchema. The boolean result aligns one-for-one with resources.
func ScopeResources(resources []Resource, scope Scope) ([]bool, error) {
	selectors, err := parseResourceIncludeSelectors(scope.Include)
	if err != nil {
		return nil, err
	}
	patterns, err := parsePatterns(scope.Exclude, scope.DefaultSchema)
	if err != nil {
		return nil, err
	}
	selection := newScopeSelection(scope, selectors)
	selected := make([]bool, len(resources))
	for i, resource := range resources {
		keep := selection.selected(resource.Types, resource.Schema, resource.Name)
		selected[i] = keep && !resourceExcluded(patterns, resource, scope.DefaultSchema)
	}
	if err := selection.emptySelectionError(scope); err != nil {
		return nil, err
	}
	return selected, nil
}

var additionalResourceSelectableTypes = map[string]struct{}{
	"aggregate":         {},
	"collation":         {},
	"default_privilege": {},
	"event":             {},
	"foreign_table":     {},
	"procedure":         {},
}

// ValidateResourceIncludeSelectors validates include selectors for arbitrary
// writer-owned top-level resources. It is the pre-connect counterpart of
// [ScopeResources]; the ordinary schema validator intentionally remains
// narrower because a DBSchema cannot represent these additional catalog kinds.
func ValidateResourceIncludeSelectors(values []string) error {
	_, err := parseResourceIncludeSelectors(values)
	return err
}

func parseResourceIncludeSelectors(values []string) ([]resourcePattern, error) {
	allowed := maps.Clone(includeSelectableTypes)
	maps.Copy(allowed, additionalResourceSelectableTypes)
	var selectors []resourcePattern
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			raw := strings.TrimSpace(part)
			if raw == "" {
				continue
			}
			selector, err := parsePattern(raw, filterKindInclude)
			if err != nil {
				return nil, err
			}
			if err := validateIncludeSelectorTypesAgainst(raw, selector.types, allowed); err != nil {
				return nil, err
			}
			selectors = append(selectors, selector)
		}
	}
	return selectors, nil
}

func resourceExcluded(patterns []resourcePattern, resource Resource, defaultSchema string) bool {
	schema := strings.TrimSpace(resource.Schema)
	if schema == "" {
		schema = strings.TrimSpace(defaultSchema)
	}
	objectNames := qualifiedNameCandidates(schema, resource.Name)
	for _, pattern := range patterns {
		if schema != "" && pattern.matches("schema", schema) {
			return true
		}
		for _, resourceType := range resource.Types {
			if pattern.matches(resourceType, objectNames...) {
				return true
			}
		}
	}
	return false
}

// includeChildTypes are resource kinds that cannot be included on their own
// because Ptah cannot project a partial parent faithfully on both sides of a
// comparison.
var includeChildTypes = map[string]struct{}{
	"column":      {},
	"index":       {},
	"constraint":  {},
	"foreign_key": {},
	"foreign-key": {},
	"trigger":     {},
	"policy":      {},
	"grant":       {},
}

// EmptySelectionError reports that a non-empty --include selection matched no
// top-level object in the state it was applied to.
//
// A selection that picks nothing and a state that holds nothing produce the
// same projection, so without this signal the two are indistinguishable and
// callers report the second. That conflation used to swallow every --include
// spelling that reaches past a top-level resource: `path.Match` treats "." as
// an ordinary character (only "/" separates), so `main.users*email`,
// `main.users?email`, and `main.users[.]email` all matched nothing and were
// reported as synced schemas.
//
// Emptiness is an outcome, not a shape, so it cannot be decided by inspecting
// the selector text. Each verb decides its own exit from this error: apply
// refuses (nothing would be applied and nothing would say so), while diff and
// inspect keep their exit status and report the empty selection out of band.
type EmptySelectionError struct {
	// Selectors are the --include selectors as written, split and trimmed the
	// way the parser splits them.
	Selectors []string
}

func (e *EmptySelectionError) Error() string {
	return "the --include selection matched no objects: " + quoteSelectors(e.Selectors)
}

// UnmatchedExcludeError reports --exclude selectors that named no object in
// any state the command filtered.
//
// An exclude pattern carries no match requirement, so before this signal a
// selector that named nothing subtracted nothing and every verb reported
// success with the object still present. That is a scoping flag failing open,
// which is worse than one that refuses: the pinned community binary v1.3.0
// also exits 0 and says nothing for `--exclude nosuchobject`, and this is the
// one place Ptah declines to reproduce it. Matching is the floor, not the
// ceiling.
//
// Each verb decides its own exit from this error the way it does from
// [EmptySelectionError]: schema apply refuses, while schema diff and schema
// inspect keep their exit status and report it out of band.
type UnmatchedExcludeError struct {
	// Selectors are the --exclude selectors as written, split and trimmed the
	// way the parser splits them.
	Selectors []string
}

func (e *UnmatchedExcludeError) Error() string {
	return "the --exclude selection matched no objects: " + quoteSelectors(e.Selectors)
}

func quoteSelectors(selectors []string) string {
	quoted := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		quoted = append(quoted, fmt.Sprintf("%q", selector))
	}
	return strings.Join(quoted, ", ")
}

// ValidateIncludeSelectors parses Atlas-style --include selectors and rejects
// forms Ptah cannot honor: malformed globs, field selectors, child resource
// types, and unknown resource types. Commands run it before any database work,
// so a selector that can never be honored fails without contacting a database
// or resetting a dev database.
func ValidateIncludeSelectors(values []string) error {
	_, err := parseIncludeSelectors(values)
	return err
}

func parseIncludeSelectors(values []string) ([]resourcePattern, error) {
	var selectors []resourcePattern
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			selector, err := parseIncludeSelector(part)
			if err != nil {
				return nil, err
			}
			if selector.glob != "" {
				selectors = append(selectors, selector)
			}
		}
	}
	return selectors, nil
}

func parseIncludeSelector(value string) (resourcePattern, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return resourcePattern{}, nil
	}
	pattern, err := parsePattern(raw, filterKindInclude)
	if err != nil {
		return resourcePattern{}, err
	}
	if err := validateIncludeSelectorTypes(raw, pattern.types); err != nil {
		return resourcePattern{}, err
	}
	return pattern, nil
}

// includeSelectorSpellings returns the --include selectors as written, split
// and trimmed exactly the way [parseIncludeSelectors] splits them, so an
// [EmptySelectionError] names what the user typed.
func includeSelectorSpellings(values []string) []string {
	var out []string
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func validateIncludeSelectorTypes(raw string, types map[string]struct{}) error {
	return validateIncludeSelectorTypesAgainst(raw, types, includeSelectableTypes)
}

func validateIncludeSelectorTypesAgainst(
	raw string,
	types map[string]struct{},
	selectable map[string]struct{},
) error {
	for resourceType := range types {
		if _, child := includeChildTypes[resourceType]; child {
			return fmt.Errorf(
				"unsupported Atlas include selector %q: %s resources ride along with their parent and cannot be included on their own",
				raw, resourceType)
		}
		if resourceType == "schema" {
			return fmt.Errorf("unsupported Atlas include selector %q: use --schema to select schemas", raw)
		}
		if _, ok := selectable[resourceType]; !ok {
			return fmt.Errorf("unsupported Atlas include resource type %q in selector %q", resourceType, raw)
		}
	}
	return nil
}

// ScopeGenerated projects the generated-schema IR through the positive
// selection: schema universe, include selectors, exclude subtraction, and
// cross-scope dependency validation, in that order. A non-positive scope
// degrades to plain exclusion.
//
// When include selectors were given and none of them matched a top-level
// object, the projection is returned together with an [EmptySelectionError].
// The projection is valid — it is empty — so callers that tolerate an empty
// selection keep using it, and callers that refuse one have the error.
func ScopeGenerated(db *goschema.Database, scope Scope) (*goschema.Database, error) {
	filtered, _, err := ScopeGeneratedReport(db, scope)
	return filtered, err
}

// ScopeGeneratedReport is [ScopeGenerated] plus the [ExcludeReport].
//
// The report is measured against the UNPROJECTED state on purpose. An
// --include that already dropped an object is not the exclude selector naming
// nothing; asking the projection instead would report a selector as empty
// whenever a positive selection had removed its object first.
func ScopeGeneratedReport(db *goschema.Database, scope Scope) (*goschema.Database, ExcludeReport, error) {
	return scopeReport(db, scope, ExcludeGeneratedReport,
		(*scopeSelection).projectGenerated, validateGeneratedScope)
}

// scopeReport is the composition order both projections follow, with the three
// state-specific steps supplied by the caller: exclude the state, project the
// positive selection onto it, and validate the result for cross-scope
// dependencies. The two states share every decision in between, so they share
// one body.
func scopeReport[T any](
	db *T,
	scope Scope,
	exclude func(*T, []string, string) (*T, ExcludeReport, error),
	project func(*scopeSelection, *T) *T,
	validate func(*T, *T, *scopeSelection) error,
) (*T, ExcludeReport, error) {
	if !scope.Positive() {
		return exclude(db, scope.Exclude, scope.DefaultSchema)
	}
	selectors, err := parseIncludeSelectors(scope.Include)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if db == nil {
		return nil, ExcludeReport{}, nil
	}
	_, report, err := exclude(db, scope.Exclude, scope.DefaultSchema)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	selection := newScopeSelection(scope, selectors)
	final, _, err := exclude(project(selection, db), scope.Exclude, scope.DefaultSchema)
	if err != nil {
		return nil, ExcludeReport{}, err
	}
	if err := validate(db, final, selection); err != nil {
		return nil, ExcludeReport{}, err
	}
	return final, report, selection.emptySelectionError(scope)
}

// ScopeDatabase projects the introspected database schema through the same
// positive selection as [ScopeGenerated], so both sides of a comparison see
// one projection. A non-positive scope degrades to plain exclusion. It reports
// an empty include selection the same way [ScopeGenerated] does.
func ScopeDatabase(db *dbschematypes.DBSchema, scope Scope) (*dbschematypes.DBSchema, error) {
	filtered, _, err := ScopeDatabaseReport(db, scope)
	return filtered, err
}

// ScopeDatabaseReport is [ScopeDatabase] plus the [ExcludeReport], measured
// against the unprojected state for the reason [ScopeGeneratedReport] gives.
func ScopeDatabaseReport(db *dbschematypes.DBSchema, scope Scope) (*dbschematypes.DBSchema, ExcludeReport, error) {
	return scopeReport(db, scope, ExcludeDatabaseReport,
		(*scopeSelection).projectDatabase, validateDatabaseScope)
}

// scopeSelection carries the parsed positive selection during one projection.
type scopeSelection struct {
	allowed   map[string]struct{}
	selectors []resourcePattern
	def       string
	// matched records whether any include selector matched a top-level object
	// during the projection. It is the outcome the emptiness check needs:
	// counting the projected objects instead would also count the support
	// objects and grants that ride along with a selection, and would miss a
	// match the exclude subtraction later removed.
	matched bool
}

// emptySelectionError reports an include selection that matched nothing.
// Selectors that were given but never matched are the whole condition; a scope
// carrying only --schema names stays silent, because narrowing to a schema
// that holds nothing is an ordinary answer rather than a selection that failed.
func (s *scopeSelection) emptySelectionError(scope Scope) error {
	if len(s.selectors) == 0 || s.matched {
		return nil
	}
	return &EmptySelectionError{Selectors: includeSelectorSpellings(scope.Include)}
}

func newScopeSelection(scope Scope, selectors []resourcePattern) *scopeSelection {
	allowed := make(map[string]struct{})
	for _, schema := range schemascope.SplitNames(scope.Schemas) {
		allowed[schema] = struct{}{}
	}
	return &scopeSelection{
		allowed:   allowed,
		selectors: selectors,
		def:       strings.TrimSpace(scope.DefaultSchema),
	}
}

func (s *scopeSelection) effectiveSchema(schema string) string {
	schema = strings.TrimSpace(schema)
	if schema != "" {
		return schema
	}
	return s.def
}

func (s *scopeSelection) schemaAllowed(schema string) bool {
	if len(s.allowed) == 0 {
		return true
	}
	_, ok := s.allowed[s.effectiveSchema(schema)]
	return ok
}

// nameCandidates returns the names an include selector can match for an
// object: the bare name and the effective-schema-qualified name.
func (s *scopeSelection) nameCandidates(schema, name string) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	qualified := dbschematypes.QualifyTableName(s.effectiveSchema(schema), name)
	if qualified == name {
		return []string{name}
	}
	return []string{name, qualified}
}

// selectedNames reports whether the include selectors match one of the name
// candidates for any of the resource types. Empty selectors select everything
// in the schema universe.
//
// Every call is a top-level object asking whether a selector names it, so a
// match here is exactly what [scopeSelection.emptySelectionError] needs to
// know. Callers evaluate the schema universe first, so an object outside it
// never reaches this point and never counts as a match.
func (s *scopeSelection) selectedNames(resourceTypes []string, names ...string) bool {
	if len(s.selectors) == 0 {
		return true
	}
	for _, selector := range s.selectors {
		for _, resourceType := range resourceTypes {
			if selector.matches(resourceType, names...) {
				s.matched = true
				return true
			}
		}
	}
	return false
}

// selected reports whether a top-level object survives the schema universe
// and the include selectors.
func (s *scopeSelection) selected(resourceTypes []string, schema, name string) bool {
	return s.schemaAllowed(schema) && s.selectedNames(resourceTypes, s.nameCandidates(schema, name)...)
}

// selectedQualifiedName applies the schema universe and include selectors to
// an object whose only schema qualification is an optional "schema." prefix
// on its name (generated views, materialized views, functions, and enums).
func (s *scopeSelection) selectedQualifiedName(resourceTypes []string, name string) bool {
	schema, bare := splitQualified(name)
	return s.schemaAllowed(schema) && s.selectedNames(resourceTypes, s.nameCandidates(schema, bare)...)
}

func typeList(resourceType string) []string {
	return []string{resourceType}
}
