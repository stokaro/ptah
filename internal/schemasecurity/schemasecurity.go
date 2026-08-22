// Package schemasecurity reports security findings over a schema Ptah has
// read, without a hosted service, an account, or a second copy of the schema.
//
// The input is the schema itself rather than a migration diff. Every analyzer
// in migration/lint answers "is this change safe"; these answer "is this state
// safe" -- who holds which privilege, which tables are reachable without a
// row-level policy, which routines run as their owner. That is a different
// question over a different input, which is why it is a sibling package rather
// than a rule family inside the linter (stokaro/ptah#1035).
//
// # Severity
//
// Findings carry [risk.Severity], the same vocabulary migration lint, the
// safety report and the SARIF export already speak. A second scale would mean
// two answers to "how bad is this" in one binary, and every gate and exit code
// would have to map between them. `info` reports and never blocks, `warning`
// asks for review, `error` blocks -- which is the distinction a security
// finding needs.
//
// # What a finding is not
//
// It is not a claim about a running system. Analysis reads what the schema
// declares or what a catalog reported; it does not attempt an access, and it
// cannot see a privilege granted outside the objects Ptah models. A clean
// report means "nothing here matched a rule", never "this database is secure".
package schemasecurity

import (
	"cmp"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/risk"
)

// publicRole is the pseudo-role every engine grants to when a grant names no
// principal. It is compared case-insensitively because a catalog reports it in
// its own case and an annotation is written in the author's.
const publicRole = "PUBLIC"

// Subject is the object a finding is about, so a caller can attach the finding
// to a node of a graph rather than to a line of text.
type Subject struct {
	// Kind is the object kind: table, routine, or schema.
	Kind string `json:"kind"`
	// Name is the object's name as the schema states it.
	Name string `json:"name"`
}

// Detail is the structured half of a finding.
//
// A message alone cannot be acted on by anything but a human reader, and the
// suggestion for most of these rules names the very values here: which
// privileges, which roles. They are separate fields rather than a formatted
// string so a consumer can group, filter, or diff them.
type Detail struct {
	// Privileges are the privilege names the finding is about, upper-cased and
	// sorted.
	Privileges []string `json:"privileges,omitempty"`
	// Roles are the roles the finding is about, sorted.
	Roles []string `json:"roles,omitempty"`
	// Language is the routine's language, where the finding is about one.
	Language string `json:"language,omitempty"`
}

// Finding is one result.
type Finding struct {
	// Code is the stable identifier a policy names to select or silence the
	// rule.
	Code string `json:"code"`
	// Severity is what a gate reads. See the package comment.
	Severity risk.Severity `json:"severity"`
	// Subject is the object the finding attaches to.
	Subject Subject `json:"subject"`
	// Message states what was found.
	Message string `json:"message"`
	// Detail carries the values the message summarizes.
	Detail Detail `json:"detail,omitzero"`
	// Suggestion is what to do about it, and is never empty: a finding with no
	// remediation is a complaint.
	Suggestion string `json:"suggestion"`
}

// Summary counts findings by severity, which is what a report header shows and
// what a gate compares against a threshold.
type Summary struct {
	Info    int `json:"info"`
	Warning int `json:"warning"`
	Error   int `json:"error"`
}

// Report is the full result of an analysis.
type Report struct {
	// Findings are sorted by code, then subject, so two runs over an unchanged
	// schema produce identical output and a diff means something changed.
	Findings []Finding `json:"findings"`
	// Summary counts Findings by severity.
	Summary Summary `json:"summary"`
	// SkippedRules names rules that did not run because the target cannot
	// express what they check, with the reason.
	//
	// Printed rather than omitted: a rule that silently did not run is
	// indistinguishable from one that found nothing, and the difference is the
	// whole value of a clean report.
	SkippedRules []SkippedRule `json:"skipped_rules,omitempty"`
}

// SkippedRule is one rule that did not run, and why.
type SkippedRule struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// Options selects what the analysis can check.
type Options struct {
	// Dialect names the target the schema is read for. It decides which rules
	// can run: a rule about row-level security has nothing to say on a target
	// that does not model it, and firing it there would report every granted
	// table on MySQL.
	Dialect string
}

// Analyze runs every rule over db and returns their findings.
//
// A nil database is analyzed as an empty one: a caller that read nothing gets
// an empty report rather than an error, and an empty report is a true statement
// about an empty schema.
func Analyze(db *goschema.Database, opts Options) Report {
	if db == nil {
		db = &goschema.Database{}
	}

	report := Report{Findings: make([]Finding, 0), SkippedRules: make([]SkippedRule, 0)}
	report.Findings = append(report.Findings, findPublicGrants(db)...)
	report.Findings = append(report.Findings, findDefinerRoutines(db)...)

	if rowLevelSecurity(opts.Dialect) {
		report.Findings = append(report.Findings, findGrantedTablesWithoutRLS(db)...)
	} else {
		report.SkippedRules = append(report.SkippedRules, SkippedRule{
			Code:   "PRV01",
			Reason: "the target does not model row-level security",
		})
	}

	slices.SortFunc(report.Findings, func(a, b Finding) int {
		return cmp.Or(
			cmp.Compare(a.Code, b.Code),
			cmp.Compare(a.Subject.Kind, b.Subject.Kind),
			cmp.Compare(a.Subject.Name, b.Subject.Name),
			cmp.Compare(a.Message, b.Message),
		)
	})
	report.Summary = summarize(report.Findings)
	return report
}

// rowLevelSecurity reports whether the target models row-level security, asked
// of the capability set rather than of a list of dialect names written here: a
// list is what falls behind when a dialect gains the feature.
func rowLevelSecurity(dialect string) bool {
	return capability.ForDialect(dialect).Has(capability.RowLevelSecurity)
}

// findPublicGrants implements PRV03.
//
// A grant to PUBLIC reaches every current and future role, including ones
// created after the grant was written, so it is the one privilege statement
// whose blast radius is not visible from the statement.
func findPublicGrants(db *goschema.Database) []Finding {
	findings := make([]Finding, 0)
	for _, grant := range db.Grants {
		if !strings.EqualFold(strings.TrimSpace(grant.Role), publicRole) {
			continue
		}
		if isShippedDefaultPublicGrant(grant) {
			continue
		}
		object, kind := grantTarget(grant)
		if object == "" {
			continue
		}
		findings = append(findings, Finding{
			Code:     "PRV03",
			Severity: risk.Warning,
			Subject:  Subject{Kind: kind, Name: object},
			Message: "privileges on " + kind + " " + object + " are granted to " + publicRole +
				", which every role holds",
			Detail:     Detail{Privileges: normalizedPrivileges(grant.Privileges), Roles: []string{publicRole}},
			Suggestion: "grant to a named role and let members inherit it, so the grant names who holds it",
		})
	}
	return findings
}

// findDefinerRoutines implements PRV02.
//
// A SECURITY DEFINER routine runs with its owner's privileges, so whoever may
// execute it acts as that owner for the length of the call. That is the point
// of the setting and also its risk, which is why this is info rather than
// warning: the finding is "this exists and is worth reading", not "this is
// wrong".
func findDefinerRoutines(db *goschema.Database) []Finding {
	findings := make([]Finding, 0)
	for _, function := range db.Functions {
		if !strings.EqualFold(strings.TrimSpace(function.Security), "DEFINER") {
			continue
		}
		findings = append(findings, Finding{
			Code:     "PRV02",
			Severity: risk.Info,
			Subject:  Subject{Kind: "routine", Name: function.Name},
			Message: "routine " + function.Name + " runs as its owner, so every caller that may " +
				"execute it acts with the owner's privileges",
			Detail: Detail{Language: function.Language},
			Suggestion: "qualify the object names in its body or pin search_path, and grant EXECUTE " +
				"only to roles that should act as the owner",
		})
	}
	return findings
}

// findGrantedTablesWithoutRLS implements PRV01.
//
// A table whose privileges reach a role returns every row to that role unless a
// policy narrows it. On a target that models row-level security, that is a
// decision worth making explicitly rather than by omission -- so the finding is
// info: plenty of tables are meant to be read whole.
func findGrantedTablesWithoutRLS(db *goschema.Database) []Finding {
	protected := make(map[string]bool, len(db.RLSEnabledTables))
	for _, enabled := range db.RLSEnabledTables {
		protected[enabled.Table] = true
	}

	roles := make(map[string]map[string]bool, len(db.Grants))
	privileges := make(map[string]map[string]bool, len(db.Grants))
	for _, grant := range db.Grants {
		table := strings.TrimSpace(grant.OnTable)
		if table == "" || protected[table] {
			continue
		}
		if roles[table] == nil {
			roles[table] = make(map[string]bool)
			privileges[table] = make(map[string]bool)
		}
		roles[table][strings.TrimSpace(grant.Role)] = true
		for _, privilege := range normalizedPrivileges(grant.Privileges) {
			privileges[table][privilege] = true
		}
	}

	findings := make([]Finding, 0, len(roles))
	for _, table := range tableNames(roles) {
		findings = append(findings, Finding{
			Code:     "PRV01",
			Severity: risk.Info,
			Subject:  Subject{Kind: "table", Name: table},
			Message: "table " + table + " is granted to a role and has no row-level security enabled, " +
				"so a granted role reads every row",
			Detail: Detail{
				Privileges: sortedKeys(privileges[table]),
				Roles:      sortedKeys(roles[table]),
			},
			Suggestion: "enable row-level security and declare a policy, or record that the whole " +
				"table is meant to be readable by these roles",
		})
	}
	return findings
}

// isShippedDefaultPublicGrant reports whether a grant is one the engine itself
// creates, rather than one somebody wrote.
//
// PostgreSQL ships USAGE on schema public to PUBLIC in every database it
// creates. Reporting it would fire this rule on every PostgreSQL database that
// has ever existed, and a finding present in every report is one a reader
// learns to skip -- taking the rest of the rule with it.
//
// The exclusion is narrow on purpose: USAGE alone, on the schema named public.
// CREATE on that schema is NOT excluded, because PostgreSQL 15 revoked it from
// PUBLIC by default, so a database that still grants it is stating something
// rather than inheriting it.
func isShippedDefaultPublicGrant(grant goschema.Grant) bool {
	if !strings.EqualFold(strings.TrimSpace(grant.OnSchema), "public") {
		return false
	}
	privileges := normalizedPrivileges(grant.Privileges)
	return len(privileges) == 1 && privileges[0] == "USAGE"
}

// grantTarget names what a grant is on, and the kind of that object.
func grantTarget(grant goschema.Grant) (name, kind string) {
	switch {
	case strings.TrimSpace(grant.OnTable) != "":
		return strings.TrimSpace(grant.OnTable), "table"
	case strings.TrimSpace(grant.OnSchema) != "":
		return strings.TrimSpace(grant.OnSchema), "schema"
	case strings.TrimSpace(grant.OnSequence) != "":
		return strings.TrimSpace(grant.OnSequence), "sequence"
	default:
		return "", ""
	}
}

// normalizedPrivileges upper-cases and sorts privilege names, so two schemas
// that spell the same grant differently produce the same finding.
func normalizedPrivileges(privileges []string) []string {
	seen := make(map[string]bool, len(privileges))
	for _, privilege := range privileges {
		trimmed := strings.ToUpper(strings.TrimSpace(privilege))
		if trimmed != "" {
			seen[trimmed] = true
		}
	}
	return sortedKeys(seen)
}

// sortedKeys returns the set's members in a stable order.
func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// tableNames returns the table keys in a stable order, so the findings do not
// depend on map iteration.
func tableNames(set map[string]map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// summarize counts findings by severity.
func summarize(findings []Finding) Summary {
	var summary Summary
	for _, finding := range findings {
		switch finding.Severity {
		case risk.Error:
			summary.Error++
		case risk.Warning:
			summary.Warning++
		default:
			summary.Info++
		}
	}
	return summary
}
