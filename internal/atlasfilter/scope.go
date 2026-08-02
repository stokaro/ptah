package atlasfilter

import (
	"fmt"
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

// includeSelectorMaxDots bounds how deep an --include selector may reach. The
// selection matches top-level resources by their bare name and by their
// effective-schema-qualified name, so a selector carries at most one "."
// *separator*. A deeper selector is the positional spelling of the
// child-resource selection [validateIncludeSelectorTypes] already rejects, and
// it is rejected the same way instead of silently projecting an empty
// selection.
//
// Depth is measured by [unquotedDots], not by counting "." characters: an
// identifier may itself contain a dot, and [dbschematypes.QualifyTableName]
// quotes any such part, so the candidate for table "my.table" in schema "main"
// is `main."my.table"` — two dot characters, one separator. Counting
// characters rejected the selector that matches it.
//
// The rule closes the literal-separator spelling only. Because path.Match
// treats "." as an ordinary character, every metacharacter that can stand for
// it escapes the check: `main.users*email`, `main.users?email`, and the
// character class `main.users[.]email` all reach past a top-level resource and
// still select nothing. See stokaro/ptah#979 for the outcome-based check that
// would cover the whole class.
const includeSelectorMaxDots = 1

// ValidateIncludeSelectors parses Atlas-style --include selectors and rejects
// forms Ptah cannot honor: malformed globs, field selectors, child resource
// types, child-depth patterns, and unknown resource types. Commands run it
// before any database work.
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
	if err := validateIncludeSelectorDepth(raw, pattern.glob); err != nil {
		return resourcePattern{}, err
	}
	return pattern, nil
}

// validateIncludeSelectorDepth rejects include selectors that reach past the
// "schema.name" qualification of a top-level resource.
func validateIncludeSelectorDepth(raw, glob string) error {
	if unquotedDots(glob) <= includeSelectorMaxDots {
		return nil
	}
	return fmt.Errorf(
		"unsupported Atlas include selector %q: selectors name top-level resources as \"name\" or \"schema.name\", and a deeper pattern names a child resource that rides along with its parent",
		raw)
}

// unquotedDots counts the "." separators of a selector glob, skipping dots
// that belong to an identifier rather than to the path.
//
// Dots are skipped inside the quoting forms the tableref parser accepts — SQL
// standard double quotes, MySQL backticks, and SQL Server brackets — because a
// qualified candidate quotes any part holding a dot, so `main."my.table"` is
// one separator, not two. Dots are also skipped after a backslash, which
// path.Match reads as an escape for the following character, so `a\.b\.c`
// matches the single bare name "a.b.c" and stays a depth-one selector.
//
// An unterminated quote leaves the remainder uncounted, which is the
// permissive direction for that input.
//
// The check is not conservative in general, though. The selection also offers
// the bare name as a candidate, and a bare name is never quoted, so a table
// literally named "a.b.c" is matched by the selector `a.b.c` — which this rule
// refuses, because that spelling is indistinguishable from the
// schema.table.column form it exists to reject. That ambiguity is why the
// refusal is deliberate rather than a bug, and why the disambiguated spellings
// `a\.b\.c` and `main."a.b.c"` are supported and documented. Resolving it
// without an escape needs the outcome-based check in stokaro/ptah#979.
func unquotedDots(glob string) int {
	dots := 0
	var closeQuote byte
	for index := 0; index < len(glob); index++ {
		character := glob[index]
		if closeQuote != 0 {
			if character == closeQuote {
				closeQuote = 0
			}
			continue
		}
		switch character {
		case '"', '`':
			closeQuote = character
		case '[':
			closeQuote = ']'
		case '\\':
			// path.Match escape: the next byte is a literal, never a separator.
			index++
		case '.':
			dots++
		}
	}
	return dots
}

func validateIncludeSelectorTypes(raw string, types map[string]struct{}) error {
	for resourceType := range types {
		if _, child := includeChildTypes[resourceType]; child {
			return fmt.Errorf(
				"unsupported Atlas include selector %q: %s resources ride along with their parent and cannot be included on their own",
				raw, resourceType)
		}
		if resourceType == "schema" {
			return fmt.Errorf("unsupported Atlas include selector %q: use --schema to select schemas", raw)
		}
		if _, ok := includeSelectableTypes[resourceType]; !ok {
			return fmt.Errorf("unsupported Atlas include resource type %q in selector %q", resourceType, raw)
		}
	}
	return nil
}

// ScopeGenerated projects the generated-schema IR through the positive
// selection: schema universe, include selectors, exclude subtraction, and
// cross-scope dependency validation, in that order. A non-positive scope
// degrades to plain exclusion.
func ScopeGenerated(db *goschema.Database, scope Scope) (*goschema.Database, error) {
	if !scope.Positive() {
		return ExcludeGenerated(db, scope.Exclude)
	}
	selectors, err := parseIncludeSelectors(scope.Include)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, nil
	}
	selection := newScopeSelection(scope, selectors)
	selected := selection.projectGenerated(db)
	final, err := ExcludeGenerated(selected, scope.Exclude)
	if err != nil {
		return nil, err
	}
	if err := validateGeneratedScope(db, final, selection); err != nil {
		return nil, err
	}
	return final, nil
}

// ScopeDatabase projects the introspected database schema through the same
// positive selection as [ScopeGenerated], so both sides of a comparison see
// one projection. A non-positive scope degrades to plain exclusion.
func ScopeDatabase(db *dbschematypes.DBSchema, scope Scope) (*dbschematypes.DBSchema, error) {
	if !scope.Positive() {
		return ExcludeDatabase(db, scope.Exclude)
	}
	selectors, err := parseIncludeSelectors(scope.Include)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, nil
	}
	selection := newScopeSelection(scope, selectors)
	selected := selection.projectDatabase(db)
	final, err := ExcludeDatabase(selected, scope.Exclude)
	if err != nil {
		return nil, err
	}
	if err := validateDatabaseScope(db, final, selection); err != nil {
		return nil, err
	}
	return final, nil
}

// scopeSelection carries the parsed positive selection during one projection.
type scopeSelection struct {
	allowed   map[string]struct{}
	selectors []resourcePattern
	def       string
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
func (s *scopeSelection) selectedNames(resourceTypes []string, names ...string) bool {
	if len(s.selectors) == 0 {
		return true
	}
	for _, selector := range s.selectors {
		for _, resourceType := range resourceTypes {
			if selector.matches(resourceType, names...) {
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
