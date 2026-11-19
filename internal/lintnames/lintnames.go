// Package lintnames declares the Ptah name of every built-in lint rule, the
// categories those names begin with, and the predicate that decides whether a
// selector written as a name or a category selects a rule.
//
// A rule keeps its code: findings, baselines and ptah-compat identify it by
// the code. The name is the native spelling a selector may use beside it, in
// `--disable`, `disabled-rules`, a `rules:` key and a `ptah:nolint` comment.
// migration/lint and internal/sqllint match selectors with this package, and
// internal/lintcatalog checks its tables against the rule registries and
// renders them, which is why the tables live below all three.
package lintnames

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// Category is the hazard class a rule name begins with: the part before the
// slash in `destructive/drop-table`.
//
// A category names what the statement risks, not the engine it runs on; the
// engine is the rule's dialect list. An engine category is the one exception:
// a rule an executable provider publishes for a hazard only its engine has
// may be named under the engine, as `ydb/add-column-form` is.
type Category struct {
	// Name is the first segment of every rule name in the category.
	Name string
	// Summary is the one-line description of the hazard the category covers.
	Summary string
	// Engine is the only dialect whose rules the category may hold, for an
	// engine category, and empty for a hazard category.
	Engine string
}

// categories declares every category a rule name may begin with.
// internal/lintcatalog refuses a rule whose category is missing here and a
// category no rule uses.
var categories = []Category{
	{Name: "capability", Summary: "the target server version lacks a capability the statement needs"},
	{Name: "correctness", Summary: "the statement is wrong as written: malformed, naming a column the schema does not declare, " +
		"or declaring a property its body breaks"},
	{Name: "coverage", Summary: "something the linter did not read, so a clean result does not mean it was checked"},
	{Name: "data-dependent", Summary: "the statement succeeds, fails or changes values depending on the rows the table already holds"},
	{Name: "dependency", Summary: "the statement drops or renames something another object still uses"},
	{Name: "design", Summary: "a schema choice that applies cleanly and costs later, in row padding, table bloat or rows with no key to address them by; " +
		"silencing it hides advice, never a statement that fails or blocks"},
	{Name: "destructive", Summary: "the statement deletes rows, values, objects or history for good"},
	{Name: "file-form", Summary: "the migration file or its directory does not have the form the run reads"},
	{Name: "integrity", Summary: "the statement removes a constraint or a NOT NULL the database enforces on the data"},
	{Name: "locking", Summary: "the statement holds a lock that blocks reads or writes while it runs"},
	{Name: "naming", Summary: "a name breaks the configured convention, or is longer than the server keeps"},
	{Name: "online", Summary: "online mode, which the user selects, could not prove the statement runs without a lock that blocks reads or writes; " +
		"its rules fire only in that mode, so `--disable locking` leaves them on"},
	{Name: "rewrite", Summary: "the statement rewrites or copies every row of the table; an info rule here reports that the rewrite was checked for and is absent"},
	{Name: "rollout", Summary: "the change breaks a client or a consumer deployed against the current schema"},
	{Name: "security", Summary: "the statement weakens access control, or writes a secret or an unquoted value into SQL"},
	{Name: "side-effect", Summary: "the statement resets, ignores or leaves behind something its text does not show; " +
		"silenced, a migration can succeed and leave the database different from what it says"},
	{Name: "transaction", Summary: "the statement cannot run, or runs differently, inside the migration's transaction or query"},
	{Name: "ydb", Summary: "a statement form only YDB refuses", Engine: "ydb"},
}

// Categories returns every declared category, ordered by name. The slice is a
// copy.
func Categories() []Category { return slices.Clone(categories) }

// CategoryFor returns the declared category called name.
func CategoryFor(name string) (Category, bool) {
	index := slices.IndexFunc(categories, func(category Category) bool { return category.Name == name })
	if index < 0 {
		return Category{}, false
	}
	return categories[index], true
}

// namePattern is the grammar of a rule name: two lowercase segments, each a
// run of letters and digits joined by single hyphens, with a slash between
// them. It cannot match a rule code or an Atlas check code, which are upper
// case.
//
// The second segment follows the SQL spelling of the statement it reports, so
// a rule about NO FORCE ROW LEVEL SECURITY is
// `security/no-force-row-level-security`, not a paraphrase of it.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*/[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// WellFormed reports whether name follows the rule-name grammar. It does not
// say the category is declared; see [CategoryFor].
func WellFormed(name string) bool { return namePattern.MatchString(name) }

// CategoryOf returns the category segment of a rule name, or the empty string
// when the name has no slash.
func CategoryOf(name string) string {
	category, _, found := strings.Cut(name, "/")
	if !found {
		return ""
	}
	return category
}

// IsSelector reports whether selector is written as a name rather than as a
// code: it holds a slash and a lower-case letter, so it names a rule
// (`locking/set-not-null`) or a category (`locking/`), or it is a declared
// category's name (`locking`).
//
// Every other selector is a code or a code prefix and is matched by the
// linter's own code rule. The split is total and the two sides cannot meet: a
// name holds a lower-case letter, and a code is upper case. Upper-case codes
// joined by a slash, `DS101/DS102`, and a slash on its own stay on the code
// side, where a nolint directive splits them at the slash. The selector is
// read as written; a caller that trims whitespace trims it first.
func IsSelector(selector string) bool {
	_, _, ok := parse(selector)
	return ok
}

// parse reads a name-space selector. It returns the rule name it names, or
// the category, and false for a selector written as a code.
func parse(selector string) (name, category string, ok bool) {
	if _, declared := CategoryFor(selector); declared {
		return "", selector, true
	}
	if !strings.Contains(selector, "/") || !strings.ContainsFunc(selector, unicode.IsLower) {
		return "", "", false
	}
	if prefix, found := strings.CutSuffix(selector, "/"); found {
		return "", prefix, true
	}
	return selector, "", true
}

// Table maps the codes of one linter's rules to their names. Its zero value
// holds no rule.
type Table struct {
	linter  string
	example string
	names   map[string]string
}

// Migration returns the names of the rules migration lint registers, the
// rules the official executable providers publish included. A rule a
// configuration declares, and one a third-party provider publishes, has a
// code and no name.
func Migration() Table {
	return Table{linter: "migration lint", example: "locking/set-not-null", names: migrationNames}
}

// SQL returns the names of the rules `ptah sql lint` can report.
func SQL() Table {
	return Table{linter: "SQL lint", example: "design/no-primary-key", names: sqlNames}
}

// Any returns the names of the rules of both linters, for a selector read
// from a file both of them read, such as the `rules:` section of
// `.ptah-lint.yaml`. A code both linters register names one rule in each.
func Any() Table {
	names := make(map[string]string, len(migrationNames)+len(sqlNames))
	maps.Copy(names, migrationNames)
	maps.Copy(names, sqlNames)
	return Table{linter: "lint", example: "locking/set-not-null", names: names}
}

// Name returns the name of the rule code, or the empty string for a code the
// table does not name.
func (t Table) Name(code string) string { return t.names[code] }

// Codes returns every code the table names, sorted.
func (t Table) Codes() []string { return slices.Sorted(maps.Keys(t.names)) }

// Selects reports whether a name-space selector selects the rule code: the
// selector is the rule's name, or the category its name begins with. A
// selector written as a code selects nothing here; see [IsSelector].
//
// There is no prefix match. `locking/set-not` selects no rule, and `lock`
// is not a category, so a name cannot reach a rule its author did not spell.
func (t Table) Selects(selector, code string) bool {
	ruleName := t.names[code]
	if ruleName == "" {
		return false
	}
	name, category, ok := parse(selector)
	switch {
	case !ok:
		return false
	case name != "":
		return name == ruleName
	default:
		return category == CategoryOf(ruleName)
	}
}

// Resolve returns the codes a name-space selector selects, sorted. It refuses
// a selector naming no rule of the table: a name it does not hold, a category
// that is not declared, and a declared category none of its rules is named
// under. A selector written as a code is refused too, since it is the code
// rule's to judge.
func (t Table) Resolve(selector string) ([]string, error) {
	name, category, ok := parse(selector)
	if !ok {
		return nil, fmt.Errorf("%q is not a rule name or category", selector)
	}
	var codes []string
	for _, code := range t.Codes() {
		if t.Selects(selector, code) {
			codes = append(codes, code)
		}
	}
	if len(codes) > 0 {
		return codes, nil
	}
	if name != "" {
		return nil, fmt.Errorf("%q is not the name of a %s rule; a name is a category and a rule joined by a slash, such as %s",
			selector, t.linter, t.example)
	}
	if _, declared := CategoryFor(category); !declared {
		return nil, fmt.Errorf("%q is not a rule category; the categories are %s", selector, categoryList())
	}
	return nil, fmt.Errorf("%q is a category that holds no %s rule", selector, t.linter)
}

// categoryList joins the declared category names for a refusal.
func categoryList() string {
	names := make([]string, 0, len(categories))
	for _, category := range categories {
		names = append(names, category.Name)
	}
	return strings.Join(names, ", ")
}
