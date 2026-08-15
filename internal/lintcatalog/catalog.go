// Package lintcatalog enumerates every lint rule Ptah's binaries can report.
//
// The enumeration is derived from the registries the linters actually consult:
// [lint.Rules] for migration lint and [sqllint.CatalogIDs] for SQL lint. What a
// registry cannot carry -- a rule's one-line meaning, and whether its
// identifier is Atlas's or Ptah's -- is declared here and joined to the
// registry by code. [Entries] fails when the two sides disagree in either
// direction, so a rule cannot exist in the code and be missing from the
// documentation, and a documented rule cannot outlive the code that emitted it.
//
// The documentation page is generated from this package rather than written by
// hand for exactly that reason: a hand-written list of rule identifiers is a
// second declaration, and a second declaration drifts.
package lintcatalog

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/internal/sqllint"
	"go.5x5.cz/ptah/migration/lint"
)

// Origin records whose rule a row describes.
//
// A rule is [OriginAtlas] when it reports a hazard the Atlas analyzer
// documentation carries a check code for, whatever identifier Ptah reports it
// under; it is [OriginPtah] otherwise. Applied to a family, it says whether
// Atlas uses the prefix.
type Origin string

const (
	// OriginAtlas marks a rule, or a prefix, that Atlas documents.
	OriginAtlas Origin = "Atlas"
	// OriginPtah marks a rule, or a prefix, that is Ptah's own.
	OriginPtah Origin = "Ptah"
)

// Kind separates the two linters, which have separate registries and separate
// commands.
type Kind string

const (
	// KindMigration is migration lint, registered in [lint.Rules].
	KindMigration Kind = "migration"
	// KindSQL is standalone SQL lint, registered in [sqllint.CatalogIDs].
	KindSQL Kind = "sql"
)

// Family is one identifier prefix and who owns it.
//
// The prefix is what carries provenance for a rule Ptah invents: inside a
// prefix Atlas never uses, every member is ours and needs no marking; inside a
// prefix Atlas does use, a member of ours is marked with a trailing P so a
// reader can tell it apart and so a code Atlas adds later cannot collide with
// it.
type Family struct {
	// Prefix is the leading letters of every code in the family.
	Prefix string
	// Origin says whether Atlas uses this prefix.
	Origin Origin
	// Summary is the one-line description of what the family covers.
	Summary string
}

// families declares every identifier prefix either tool uses. A code whose
// prefix is missing here fails [Validate] rather than being documented under no
// family, because the family is what makes the suffix convention checkable.
var families = []Family{
	{Prefix: "DS", Origin: OriginAtlas, Summary: "destructive changes: statements that delete data or drop objects"},
	{Prefix: "CD", Origin: OriginAtlas, Summary: "constraint deletions, split by the constraint type the SQL names"},
	{Prefix: "DD", Origin: OriginPtah, Summary: "changes whose outcome depends on the rows already in the table"},
	{Prefix: "MF", Origin: OriginAtlas, Summary: "Atlas: changes that may fail. Ptah: migration file form"},
	{Prefix: "BC", Origin: OriginAtlas, Summary: "changes that break code already deployed against the old schema"},
	{Prefix: "PG", Origin: OriginAtlas, Summary: "PostgreSQL-specific locking, rewrite, and transaction hazards"},
	{Prefix: "MY", Origin: OriginAtlas, Summary: "MySQL and MariaDB-specific rebuild and blocking-DDL hazards"},
	{Prefix: "LT", Origin: OriginAtlas, Summary: "SQLite-specific hazards"},
	{Prefix: "TX", Origin: OriginAtlas, Summary: "transaction shape of a migration"},
	{Prefix: "NM", Origin: OriginAtlas, Summary: "naming conventions; Atlas documents these, Ptah emits none"},
	{Prefix: "SA", Origin: OriginAtlas, Summary: "static analysis; Atlas documents these, Ptah emits none"},
	{Prefix: "OW", Origin: OriginAtlas, Summary: "ownership policy; Atlas documents these, Ptah emits none"},
	{Prefix: "SQL", Origin: OriginPtah, Summary: "the SQL linter could not read or model the statement"},
	{Prefix: "DDL", Origin: OriginPtah, Summary: "the shape of a DDL statement the SQL linter modeled"},
	{Prefix: "CAP", Origin: OriginPtah, Summary: "the target server version lacks a capability the statement needs"},
}

// Entry is one rule, joined from a registry and this package's declaration.
type Entry struct {
	// Code is the identifier the rule reports under natively.
	Code string
	// Kind says which linter registers it.
	Kind Kind
	// Title is the registry's short label for the hazard.
	Title string
	// Summary is the one-line meaning a reader looks up.
	Summary string
	// Severity is the rule's default severity.
	Severity string
	// Dialects lists the dialects the rule applies to; empty means every one.
	Dialects []string
	// Native reports whether native ptah can emit the rule.
	Native bool
	// Compat reports whether ptah-compat can emit the rule.
	Compat bool
	// AtlasCode is the Atlas analyzer check this rule reports, when it reports
	// one. It is empty for a rule of Ptah's own, and it is the single source of
	// [Entry.Origin], so the two can never say different things.
	AtlasCode string
	// CompatAnalyzer and CompatCode are what ptah-compat prints for this rule.
	CompatAnalyzer string
	CompatCode     string
}

// ruleMeta is everything about a rule that no registry can carry.
type ruleMeta struct {
	// Summary is the one-line meaning.
	Summary string
	// AtlasCode names the Atlas analyzer check the rule reports, and is empty
	// for a rule of Ptah's own. It decides the rule's origin and whether its
	// identifier follows the convention, so it is declared once here rather
	// than restated alongside a separate origin field that could contradict it.
	AtlasCode string
	// NativeOnly marks a rule the compatibility surface never reports. It is
	// pinned by a test that runs the analyzer under both profiles, so it cannot
	// become a claim about behavior that has changed.
	NativeOnly bool
}

// migrationRuleMeta declares the documentation-only facts about each migration
// lint rule. Adding a rule to migration/lint without adding it here fails
// [Entries]; removing one without removing the entry here fails it too.
var migrationRuleMeta = map[string]ruleMeta{
	"DS101": {
		Summary:   "DROP TABLE, or a rename that retires the name, destroys the table and every row in it",
		AtlasCode: "DS102",
	},
	"DS102": {
		Summary:   "DROP COLUMN destroys the column and every value stored in it",
		AtlasCode: "DS103",
	},
	"DS103": {
		Summary: "a column type change can truncate or reject existing values and rewrite the table",
	},
	"DS104": {
		Summary: "DROP NOT NULL removes a column-level data protection",
	},
	"DS105": {
		Summary: "an untyped DROP CONSTRAINT removes a data protection the SQL does not name",
	},
	"DS106": {
		Summary: "removing an enum value can invalidate rows that still hold it",
	},
	"DS107": {
		// Broader than Atlas DS101, which is the schema drop alone: this rule
		// also fires on DROP TYPE, EXTENSION, FUNCTION, ROLE, and POLICY, so
		// the rule is ours even though it covers the Atlas one.
		Summary: "dropping a schema, type, extension, function, role, or policy removes behavior",
	},
	"DS108": {
		Summary: "TRUNCATE deletes every row in the table",
	},
	"DS109": {
		Summary: "DISABLE ROW LEVEL SECURITY removes an access-control protection",
	},
	"CD101": {
		Summary:   "dropping a foreign key removes referential-integrity enforcement",
		AtlasCode: "CD101",
	},
	"CD102": {
		Summary:   "dropping a check constraint removes a value-validation guarantee",
		AtlasCode: "CD102",
	},
	"CD103": {
		Summary:   "dropping a primary key removes row identity and can break replication",
		AtlasCode: "CD103",
	},
	"DD101": {
		Summary:   "adding a NOT NULL column without a default fails or blocks on a populated table",
		AtlasCode: "MF103",
	},
	"MF101": {
		Summary: "no matching .down.sql exists, so a failed deploy cannot be rolled back mechanically",
	},
	"MF102": {
		Summary: "the migration carries no executable statements",
	},
	"MF103": {
		Summary: "the file name does not follow the migration file-name convention",
	},
	"BC101": {
		Summary:    "a rename retires a name deployed code still refers to",
		AtlasCode:  "BC101",
		NativeOnly: true,
	},
	"PG101": {
		Summary:   "CREATE INDEX without CONCURRENTLY blocks writes for the whole build",
		AtlasCode: "PG101",
	},
	"PG102": {
		Summary: "ALTER TYPE ... ADD VALUE cannot run inside a transaction block",
	},
	"PG103": {
		Summary:   "CONCURRENTLY cannot run inside the migration's transaction",
		AtlasCode: "PG103",
	},
	"PG104": {
		Summary:   "adding a primary key takes an ACCESS EXCLUSIVE lock and scans existing rows",
		AtlasCode: "PG104",
	},
	"PG105": {
		Summary:   "adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates rows",
		AtlasCode: "PG105",
	},
	"PG106": {
		Summary:   "DROP INDEX without CONCURRENTLY blocks writes while the index is removed",
		AtlasCode: "PG102",
	},
	"PG110": {
		Summary:   "the declared column order wastes tuple padding",
		AtlasCode: "PG110",
	},
	"PG302": {
		Summary:   "a volatile DEFAULT on an added column rewrites or evaluates every existing row",
		AtlasCode: "PG302",
	},
	"PG303": {
		Summary:   "SET NOT NULL scans the table to validate existing rows",
		AtlasCode: "PG303",
	},
	"PG305": {
		Summary:   "adding a CHECK constraint validates existing rows and holds locks",
		AtlasCode: "PG305",
	},
	"PG306": {
		Summary:   "adding a foreign key validates existing rows and can block writes on both tables",
		AtlasCode: "PG306",
	},
	"PG307": {
		Summary:   "changing LOGGED or UNLOGGED rewrites the table under heavyweight locks",
		AtlasCode: "PG307",
	},
	"PG308": {
		Summary:   "CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block writes",
		AtlasCode: "PG308",
	},
	"PG309": {
		Summary:   "adding a STORED generated column computes and stores a value for every row",
		AtlasCode: "PG309",
	},
	"PG310": {
		Summary:   "adding an identity column can rewrite existing rows",
		AtlasCode: "PG310",
	},
	"PG311": {
		Summary:   "changing a table's access method rewrites the table",
		AtlasCode: "PG311",
	},
	"MY101": {
		Summary: "this ALTER TABLE form rebuilds the table and blocks writes for the duration",
	},
	"MY102": {
		Summary:   "MySQL ignores an inline REFERENCES clause in ADD COLUMN",
		AtlasCode: "MY102",
	},
	"MY131": {
		Summary:   "adding a foreign key can copy or lock the table and block writes",
		AtlasCode: "MY131",
	},
	"MY132": {
		Summary:   "adding a primary key rebuilds the table and blocks DML",
		AtlasCode: "MY132",
	},
	"MY134": {
		Summary:   "adding a FULLTEXT index can rebuild the table and block writes",
		AtlasCode: "MY134",
	},
	"MY135": {
		Summary:   "adding a SPATIAL index can rebuild the table and block writes",
		AtlasCode: "MY135",
	},
	"LT101": {
		Summary:   "SQLite cannot enforce NOT NULL on existing nullable data without a rebuild",
		AtlasCode: "LT101",
	},
	"TX101": {
		Summary:   "the migration mixes statements that cannot share one transaction",
		AtlasCode: "TX101",
	},
	"TX201": {
		Summary:   "an explicit BEGIN/COMMIT block fights the migrator's transaction management",
		AtlasCode: "TX201",
	},
}

// sqlRuleMeta declares the same facts for the standalone SQL linter, which
// only native ptah reaches.
var sqlRuleMeta = map[string]ruleMeta{
	"SQL001": {
		Summary: "the SQL parser could not build an AST, so no rule could inspect the statement",
	},
	"SQL002": {
		Summary: "the statement uses a sub-language `ptah sql lint` does not model yet",
	},
	"DDL001": {
		Summary: "the created table declares no primary key",
	},
	"CAP001": {
		Summary: "the statement needs a capability the target server version does not have",
	},
}

// preConventionCodes pins the identifiers that predate the suffix convention.
//
// The convention -- an Atlas identifier unchanged for an Atlas analyzer, the
// same identifier plus a trailing P for a rule of ours inside an Atlas family,
// and no marking at all inside a family of our own -- is checkable rather than
// remembered, which means something has to hold the identifiers that were
// chosen before it existed. This list is that something, and [Validate] fails
// when the derived set of non-conforming identifiers differs from it in either
// direction.
//
// The list may shrink and must not grow: a rule added from now on that does not
// follow the convention fails the gate rather than being appended here.
// Renaming what is already on it changes what ptah-compat prints, what a
// .ptah-lint.yaml selector matches, and what a SARIF consumer keys on, so it is
// a decision with a deprecation path rather than an edit to this slice.
var preConventionCodes = []string{
	"DS101", "DS102", "DS103", "DS104", "DS105", "DS106", "DS107", "DS108", "DS109",
	"DD101",
	"MF101", "MF102", "MF103",
	"PG102", "PG106",
	"MY101",
}

// Families returns every declared identifier prefix.
func Families() []Family { return slices.Clone(families) }

// FamilyFor returns the family a code belongs to.
func FamilyFor(code string) (Family, bool) {
	prefix := prefixOf(code)
	for _, family := range families {
		if family.Prefix == prefix {
			return family, true
		}
	}
	return Family{}, false
}

// prefixOf returns the leading letters of a code, which is its family.
func prefixOf(code string) string {
	for index, value := range code {
		if value >= 'A' && value <= 'Z' {
			continue
		}
		return code[:index]
	}
	return code
}

// Origin says whether the rule reports an Atlas analyzer check or one of
// Ptah's own.
func (e Entry) Origin() Origin {
	if e.AtlasCode != "" {
		return OriginAtlas
	}
	return OriginPtah
}

// FollowsConvention reports whether an identifier is spelled the way the
// convention requires: an Atlas check keeps the Atlas identifier, a rule of
// ours inside an Atlas family adds a trailing P, and a rule inside a family of
// ours is left unmarked because the prefix already carries the provenance.
func (e Entry) FollowsConvention() bool {
	family, found := FamilyFor(e.Code)
	if !found {
		return false
	}
	if e.AtlasCode != "" {
		return e.Code == e.AtlasCode
	}
	if family.Origin == OriginPtah {
		return !strings.HasSuffix(e.Code, "P")
	}
	return strings.HasSuffix(e.Code, "P")
}

// ConventionNote explains, for a non-conforming identifier, what the convention
// would have spelled it. It returns the empty string for a conforming one.
//
// All three arms are spelled out because this text is what a failing check
// prints. A note that named the wrong remedy would send the reader to rename
// the one thing the convention did not ask them to.
func (e Entry) ConventionNote() string {
	if e.FollowsConvention() {
		return ""
	}
	if e.AtlasCode != "" {
		return fmt.Sprintf("reports Atlas `%s`, which the convention spells `%s`", e.AtlasCode, e.AtlasCode)
	}
	family, found := FamilyFor(e.Code)
	if !found {
		return fmt.Sprintf("prefix `%s` belongs to no declared family", prefixOf(e.Code))
	}
	if family.Origin == OriginPtah {
		return fmt.Sprintf("Ptah rule inside Ptah's own `%s` family, which the convention leaves unmarked", family.Prefix)
	}
	return fmt.Sprintf("Ptah rule inside the Atlas `%s` family, which the convention spells `%sP`", family.Prefix, e.Code)
}

// everyDialect is the label for a rule with no dialect restriction.
const everyDialect = "all"

// severityError is the severity spelling both linters use for an error-grade
// finding.
const severityError = "error"

// DialectsLabel renders the dialect restriction for a heading or a table cell.
func (e Entry) DialectsLabel() string {
	if len(e.Dialects) == 0 {
		return everyDialect
	}
	return strings.Join(e.Dialects, ", ")
}

// SurfaceLabel renders which binaries report the rule.
func (e Entry) SurfaceLabel() string {
	switch {
	case e.Native && e.Compat:
		return "both"
	case e.Native:
		return "native only"
	case e.Compat:
		return "compat only"
	}
	return "none"
}

// Entries returns every rule both linters register, migration lint first.
func Entries() ([]Entry, error) {
	migration, err := MigrationEntries()
	if err != nil {
		return nil, err
	}
	sql, err := SQLEntries()
	if err != nil {
		return nil, err
	}
	return append(migration, sql...), nil
}

// MigrationEntries returns every rule migration lint registers, in registry
// order.
func MigrationEntries() ([]Entry, error) {
	return migrationEntriesFrom(lint.Rules())
}

// migrationEntriesFrom is the join, separated from the registry it normally
// reads so a test can hand it a registry the catalog does not describe. Driving
// the disagreement through the real registry would mean registering a rule
// process-wide and leaving it there for every test that runs after.
func migrationEntriesFrom(rules []lint.Rule) ([]Entry, error) {
	entries := make([]Entry, 0, len(rules))
	declared := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		meta, ok := migrationRuleMeta[rule.Code]
		if !ok {
			return nil, fmt.Errorf("migration lint registers rule %s with no catalog entry: add it to migrationRuleMeta so the documented list cannot fall behind the code", rule.Code)
		}
		declared[rule.Code] = struct{}{}
		identity := atlaslint.RuleForNativeCode(rule.Code)
		entries = append(entries, Entry{
			Code:           rule.Code,
			Kind:           KindMigration,
			Title:          rule.Title,
			Summary:        meta.Summary,
			Severity:       string(rule.Severity),
			Dialects:       slices.Clone(rule.Dialects),
			Native:         true,
			Compat:         !meta.NativeOnly,
			AtlasCode:      meta.AtlasCode,
			CompatAnalyzer: identity.Analyzer,
			CompatCode:     identity.Code,
		})
	}
	if err := checkOrphans(migrationRuleMeta, declared, "migration lint"); err != nil {
		return nil, err
	}
	return entries, nil
}

// SQLEntries returns every rule the standalone SQL linter can report.
func SQLEntries() ([]Entry, error) {
	return sqlEntriesFrom(sqllint.CatalogIDs())
}

// sqlEntriesFrom is the SQL-linter half of the same join, split for the same
// reason as [migrationEntriesFrom].
func sqlEntriesFrom(ids []string) ([]Entry, error) {
	entries := make([]Entry, 0, len(ids))
	declared := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		meta, ok := sqlRuleMeta[id]
		if !ok {
			return nil, fmt.Errorf("SQL lint can report rule %s with no catalog entry: add it to sqlRuleMeta so the documented list cannot fall behind the code", id)
		}
		declared[id] = struct{}{}
		entries = append(entries, Entry{
			Code:      id,
			Kind:      KindSQL,
			Title:     sqllint.CatalogTitle(id),
			Summary:   meta.Summary,
			Severity:  string(sqllint.CatalogSeverity(id)),
			Native:    true,
			AtlasCode: meta.AtlasCode,
		})
	}
	if err := checkOrphans(sqlRuleMeta, declared, "SQL lint"); err != nil {
		return nil, err
	}
	return entries, nil
}

// checkOrphans fails when this package documents a rule no registry produces,
// which is the other half of the drift the catalog exists to prevent.
func checkOrphans(meta map[string]ruleMeta, declared map[string]struct{}, surface string) error {
	orphans := make([]string, 0, len(meta))
	for code := range meta {
		if _, ok := declared[code]; !ok {
			orphans = append(orphans, code)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	slices.Sort(orphans)
	return fmt.Errorf("the catalog documents %s rule(s) %s that no longer exist: remove the catalog entry or restore the rule", surface, strings.Join(orphans, ", "))
}

// NonConforming returns the entries whose identifiers do not follow the suffix
// convention, in catalog order.
func NonConforming(entries []Entry) []Entry {
	var out []Entry
	for _, entry := range entries {
		if !entry.FollowsConvention() {
			out = append(out, entry)
		}
	}
	return out
}

// Validate fails on every way the catalog and the code can disagree: a rule
// with no family, a rule claiming an Atlas identifier that Atlas does not
// document, an Atlas check mapped onto a rule that does not exist, and a set of
// non-conforming identifiers that differs from the pinned one.
func Validate(entries []Entry) error {
	codes := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		codes[entry.Code] = struct{}{}
	}
	var problems []string
	for _, entry := range entries {
		if _, found := FamilyFor(entry.Code); !found {
			problems = append(problems, fmt.Sprintf("rule %s has prefix %q, which no family declares", entry.Code, prefixOf(entry.Code)))
		}
		if entry.AtlasCode != "" {
			check, ok := atlasCheckFor(entry.AtlasCode)
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("rule %s reports Atlas %s, which the Atlas analyzer list does not document", entry.Code, entry.AtlasCode))
			case !slices.Contains(check.PtahRules, entry.Code):
				// Both directions of the same fact, so a row can never claim a
				// rule the rule does not claim back.
				problems = append(problems, fmt.Sprintf("rule %s reports Atlas %s, but that check's row does not name %s", entry.Code, entry.AtlasCode, entry.Code))
			}
		}
		if strings.TrimSpace(entry.Summary) == "" {
			problems = append(problems, fmt.Sprintf("rule %s has no one-line meaning", entry.Code))
		}
	}
	for _, check := range atlasChecks {
		for _, code := range check.PtahRules {
			if _, ok := codes[code]; !ok {
				problems = append(problems, fmt.Sprintf("Atlas check %s claims coverage by Ptah rule %s, which no linter registers", check.Code, code))
			}
		}
		switch check.Status {
		case StatusCovered, StatusPartial:
			if len(check.PtahRules) == 0 {
				problems = append(problems, fmt.Sprintf("Atlas check %s is marked %s and names no Ptah rule", check.Code, check.Status))
			}
		case StatusAbsent, StatusWaived:
			if len(check.PtahRules) > 0 {
				problems = append(problems, fmt.Sprintf("Atlas check %s is marked %s yet names Ptah rules", check.Code, check.Status))
			}
		default:
			problems = append(problems, fmt.Sprintf("Atlas check %s has unknown status %q", check.Code, check.Status))
		}
	}
	problems = append(problems, conventionProblems(entries)...)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("lint catalog is inconsistent with the code:\n  %s", strings.Join(problems, "\n  "))
}

// conventionProblems compares the identifiers that do not follow the convention
// against the pinned list of the ones that predate it.
func conventionProblems(entries []Entry) []string {
	pinned := make(map[string]struct{}, len(preConventionCodes))
	for _, code := range preConventionCodes {
		pinned[code] = struct{}{}
	}
	var problems []string
	seen := make(map[string]struct{}, len(preConventionCodes))
	for _, entry := range NonConforming(entries) {
		seen[entry.Code] = struct{}{}
		if _, ok := pinned[entry.Code]; ok {
			continue
		}
		problems = append(problems, fmt.Sprintf("rule %s does not follow the identifier convention (%s) and is not one of the identifiers that predate it", entry.Code, entry.ConventionNote()))
	}
	for _, code := range preConventionCodes {
		if _, ok := seen[code]; !ok {
			problems = append(problems, fmt.Sprintf("rule %s is pinned as predating the identifier convention but now follows it or no longer exists; remove it from preConventionCodes", code))
		}
	}
	return problems
}
