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
	"strconv"
	"strings"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/risk"
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

// RoleMembership is one role-in-role edge the analysis reads: Member holds
// everything Role grants.
//
// It is passed in rather than read from the schema because Ptah models
// membership as a property of the live server rather than of a desired state:
// a caller holding a catalog read has it, and one holding a schema file does
// not (stokaro/ptah#1950).
type RoleMembership struct {
	// Role is the role whose privileges are granted.
	Role string
	// Member is the role that receives them.
	Member string
	// AdminOption reports whether the edge grants the right to grant the role
	// onward rather than evidence that somebody uses it. See
	// [heldForItsPrivileges] for why the two rules here read it.
	AdminOption bool
}

// ObjectOwner is one object and the role that owns it.
//
// Passed in for the same reason memberships are: Ptah models no ownership in a
// desired state, so a caller holding a catalog read has it and one holding a
// schema file does not (stokaro/ptah#1950).
type ObjectOwner struct {
	// Kind is the object kind: table, view, sequence, schema.
	Kind string
	// Name is the object's name.
	Name string
	// Owner is the role that owns it.
	Owner string
	// OwnerCanLogin reports whether that role can log in, which is the whole
	// question OWN01 asks. It is a fact the caller read rather than one this
	// package infers from the described roles: the owner of everything on a
	// default PostgreSQL database is a role Ptah deliberately does not
	// describe.
	OwnerCanLogin bool
}

// RoleObjectUsage is one observation that a role used an object.
//
// It is passed in for a stronger reason than ownership and membership are: a
// privilege is not use, and no catalog Ptah reads keeps this. A role holding
// SELECT on a table it has never read looks identical, in every catalog, to one
// that reads it hourly. PostgreSQL's pg_stat_user_tables counts scans without
// attributing them to a role; pg_stat_statements attributes to a userid but is
// an extension a database may not have, is reset on demand, and records
// statement text rather than the object graph; MySQL and MariaDB keep no
// equivalent; SQL Server's sys.dm_db_index_usage_stats attributes nothing to a
// principal.
//
// So the signal is the caller's to collect -- from pg_stat_statements, from an
// audit stream, from anything that observed real access -- and ROL01 reports
// itself skipped without one rather than reporting every grant as unused
// (stokaro/ptah#1961).
type RoleObjectUsage struct {
	// Role is the role observed using the object.
	Role string
	// Kind is the object kind, matching what a grant names: table, schema, or
	// sequence.
	Kind string
	// Name is the object's name.
	Name string
}

// Options selects what the analysis can check.
type Options struct {
	// ObjectOwners are the owners of the objects under analysis.
	//
	// Nil means the caller did not read them and OWN01 reports itself skipped;
	// an empty non-nil slice means it read them and there are none.
	ObjectOwners []ObjectOwner
	// RoleMemberships are the role-in-role edges of the target.
	//
	// A NIL slice means the caller did not read them, and the rules that need
	// them are reported as skipped. An EMPTY non-nil slice means the caller
	// read them and there are none, and those rules run: a server whose roles
	// have no members is exactly what ROL04 is about. "Not read" and "none"
	// are different answers, and only the second one makes a clean report mean
	// something.
	RoleMemberships []RoleMembership
	// RoleObjectUsage records which roles were observed using which objects.
	//
	// A NIL slice means the caller collected no usage data, and ROL01 is
	// reported as skipped. An EMPTY non-nil slice means the caller collected it
	// and observed no access at all, and ROL01 runs -- a window in which
	// nothing was used is a real answer, and it is the answer that makes every
	// grant in it unused.
	//
	// The distinction matters more here than for the other signals, because the
	// two states produce opposite reports: "not collected" must say nothing,
	// and "collected, nothing used" must name every grant.
	RoleObjectUsage []RoleObjectUsage
	// Capabilities is the set the target resolves, and it decides which rules
	// can run: a rule about row-level security has nothing to say where the
	// target does not model it, and firing it there would report every granted
	// table on MySQL.
	//
	// It is a capability set rather than a dialect name because the answer can
	// depend on the version -- PostgreSQL gained row-level security in 9.5 --
	// and because a live caller already holds the set its session resolved,
	// refinements included. An empty set runs no capability-gated rule and says
	// so in SkippedRules.
	Capabilities capability.Capabilities
}

// Analyze runs every rule over db and returns their findings.
//
// A nil database is analyzed as an empty one: a caller that read nothing gets
// an empty report rather than an error, and an empty report is a true statement
// about an empty schema.
func Analyze(db *schemamodel.Database, opts Options) Report {
	if db == nil {
		db = &schemamodel.Database{}
	}

	report := Report{Findings: make([]Finding, 0), SkippedRules: make([]SkippedRule, 0)}
	report.Findings = append(report.Findings, findPublicGrants(db)...)
	report.Findings = append(report.Findings, findDefinerRoutines(db)...)

	if opts.Capabilities.Has(capability.RowLevelSecurity) {
		report.Findings = append(report.Findings, findGrantedTablesWithoutRLS(db)...)
	} else {
		report.SkippedRules = append(report.SkippedRules, SkippedRule{
			Code:   "PRV01",
			Reason: "the target does not model row-level security",
		})
	}

	if opts.ObjectOwners != nil {
		report.Findings = append(report.Findings, findObjectsOwnedByALoginRole(opts.ObjectOwners)...)
	} else {
		report.SkippedRules = append(report.SkippedRules,
			SkippedRule{Code: "OWN01", Reason: "object ownership was not read for this source"})
	}

	if opts.RoleObjectUsage != nil {
		report.Findings = append(report.Findings, findGrantsOnUnusedObjects(db, opts.RoleObjectUsage)...)
	} else {
		report.SkippedRules = append(report.SkippedRules, SkippedRule{
			Code: "ROL01",
			Reason: "no role usage data was supplied; a privilege is not use, and no catalog " +
				"records which roles read which objects",
		})
	}

	if opts.RoleMemberships != nil {
		report.Findings = append(report.Findings, findRolesWithNoMembers(db, opts.RoleMemberships)...)
		report.Findings = append(report.Findings, findOverlappingRoles(db, opts.RoleMemberships)...)
	} else {
		report.SkippedRules = append(report.SkippedRules,
			SkippedRule{Code: "ROL03", Reason: "role membership was not read for this source"},
			SkippedRule{Code: "ROL04", Reason: "role membership was not read for this source"},
		)
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

// findPublicGrants implements PRV03.
//
// A grant to PUBLIC reaches every current and future role, including ones
// created after the grant was written, so it is the one privilege statement
// whose blast radius is not visible from the statement.
func findPublicGrants(db *schemamodel.Database) []Finding {
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
func findDefinerRoutines(db *schemamodel.Database) []Finding {
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
func findGrantedTablesWithoutRLS(db *schemamodel.Database) []Finding {
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
func isShippedDefaultPublicGrant(grant schemamodel.Grant) bool {
	if !strings.EqualFold(strings.TrimSpace(grant.OnSchema), "public") {
		return false
	}
	privileges := normalizedPrivileges(grant.Privileges)
	return len(privileges) == 1 && privileges[0] == "USAGE"
}

// findObjectsOwnedByALoginRole implements OWN01.
//
// An object owned by a role somebody logs in as can be dropped or altered by
// whoever holds that password, and the privilege cannot be revoked without
// changing the owner: an owner is not a grant. Owning through a NOLOGIN group
// role instead makes the right revocable, because membership is.
//
// ONE finding per owning role rather than one per object. The finding is
// common -- PostgreSQL makes the creating role the owner, and most schemas are
// created by the account the application connects as -- so a per-object rule
// would bury every other finding under a list of every table. Aggregating
// keeps it readable and keeps its severity honest: this is worth knowing, not
// worth blocking.
func findObjectsOwnedByALoginRole(owners []ObjectOwner) []Finding {
	owned := make(map[string]map[string]bool, len(owners))
	for _, owner := range owners {
		name := strings.TrimSpace(owner.Owner)
		if name == "" || !owner.OwnerCanLogin {
			continue
		}
		if owned[name] == nil {
			owned[name] = make(map[string]bool)
		}
		owned[name][strings.TrimSpace(owner.Kind)+" "+strings.TrimSpace(owner.Name)] = true
	}

	findings := make([]Finding, 0, len(owned))
	for _, role := range sortedKeys(setOfOwners(owned)) {
		objects := sortedKeys(owned[role])
		findings = append(findings, Finding{
			Code:     "OWN01",
			Severity: risk.Info,
			Subject:  Subject{Kind: "role", Name: role},
			Message: "role " + role + " can log in and owns " + strconv.Itoa(len(objects)) +
				" object(s), so whoever holds its password can drop or alter them",
			Detail:     Detail{Roles: []string{role}, Privileges: objects},
			Suggestion: "own them with a role that cannot log in and grant membership in it instead",
		})
	}
	return findings
}

// setOfOwners turns the owner map into a set, so sortedKeys can order it.
func setOfOwners(owned map[string]map[string]bool) map[string]bool {
	keys := make(map[string]bool, len(owned))
	for key := range owned {
		keys[key] = true
	}
	return keys
}

// findGrantsOnUnusedObjects implements ROL01.
//
// A role holding privileges on an object it never touches is privilege nobody
// is exercising, and the remedy is to revoke it. The rule cannot be answered
// from a schema -- see [RoleObjectUsage] -- so it runs only over a usage signal
// the caller collected, and the caller decides what window that signal covers.
//
// PUBLIC is skipped. Usage cannot be attributed to it (every role holds it, so
// an observation naming a real role says nothing about the PUBLIC grant), and a
// privilege granted to PUBLIC is already PRV03's finding with a better
// suggestion. Reporting it here as well would mean two findings and two
// remedies for one grant.
//
// Severity is Warning rather than Error: the signal is a window, and a window
// is evidence of absence only for as long as it covers. A quarterly job that
// did not run inside it holds a privilege this rule will name.
func findGrantsOnUnusedObjects(db *schemamodel.Database, usage []RoleObjectUsage) []Finding {
	used := make(map[string]bool, len(usage))
	for _, observation := range usage {
		role := strings.TrimSpace(observation.Role)
		name := strings.TrimSpace(observation.Name)
		if role == "" || name == "" {
			continue
		}
		used[usageKey(role, observation.Kind, name)] = true
	}

	findings := make([]Finding, 0)
	for _, grant := range db.Grants {
		role := strings.TrimSpace(grant.Role)
		if role == "" || strings.EqualFold(role, publicRole) {
			continue
		}
		object, kind := grantTarget(grant)
		if object == "" {
			continue
		}
		if used[usageKey(role, kind, object)] {
			continue
		}
		findings = append(findings, Finding{
			Code:     "ROL01",
			Severity: risk.Warning,
			Subject:  Subject{Kind: kind, Name: object},
			Message: "role " + role + " holds privileges on " + kind + " " + object +
				", which it was not observed using",
			Detail:     Detail{Privileges: normalizedPrivileges(grant.Privileges), Roles: []string{role}},
			Suggestion: "revoke the privileges, or widen the usage window if the access is periodic",
		})
	}
	return findings
}

// usageKey is the identity a grant and an observation are matched on.
//
// Case-insensitive on every part, for the reason the package already folds a
// role name: a catalog reports an identifier in its own case and a usage
// collector reports whatever its source recorded, and a rule that fired because
// one of them said `Orders` would be reporting a spelling.
func usageKey(role, kind, name string) string {
	return strings.ToLower(role) + "\x00" + strings.ToLower(strings.TrimSpace(kind)) + "\x00" + strings.ToLower(name)
}

// findRolesWithNoMembers implements ROL04.
//
// A role that cannot log in and that nobody holds grants its privileges to
// nobody. That is not an attack, which is why it is info: it is surface that
// looks maintained and is not, and the next `GRANT role TO someone` activates
// whatever it accumulated while nobody was reading it.
//
// A login role with no members is NOT reported. It is its own principal, and
// reporting every application account would bury the rule that matters.
func findRolesWithNoMembers(db *schemamodel.Database, memberships []RoleMembership) []Finding {
	held := make(map[string]bool, len(memberships))
	for _, membership := range memberships {
		if !heldForItsPrivileges(membership) {
			continue
		}
		held[strings.TrimSpace(membership.Role)] = true
	}

	findings := make([]Finding, 0)
	for _, role := range db.Roles {
		name := strings.TrimSpace(role.Name)
		if name == "" || role.Login || held[name] {
			continue
		}
		findings = append(findings, Finding{
			Code:     "ROL04",
			Severity: risk.Info,
			Subject:  Subject{Kind: "role", Name: name},
			Message: "role " + name + " cannot log in and has no members, so nothing it is " +
				"granted reaches anybody",
			Detail:     Detail{Privileges: privilegesOfRole(db, name)},
			Suggestion: "grant it to the roles that should hold it, or drop it with its privileges",
		})
	}
	return findings
}

// heldForItsPrivileges reports whether an edge is evidence that somebody uses
// the role, as opposed to the right to grant it onward.
//
// Measured on MariaDB 11.8: `CREATE ROLE reader` inserts
// `(root, reader, Admin_option='Y')` into mysql.roles_mapping by itself, so
// every role on every MariaDB server is "held" by whoever created it. Counting
// that edge makes ROL04 unable to fire at all and makes ROL03 name the creator
// on every server -- a rule that reports nothing and a rule that reports the
// same thing everywhere. MySQL 8.4's mysql.role_edges carries no such row: the
// two engines record the same GRANT differently.
//
// The cost is stated rather than hidden: an explicit `WITH ADMIN OPTION` grant
// to a real user is ignored here too, so ROL04 can name a role that one
// administrator could also use. That is the safe direction for an advisory
// rule -- a finding a reader can dismiss beats a finding that never appears.
func heldForItsPrivileges(membership RoleMembership) bool {
	return !membership.AdminOption
}

// findOverlappingRoles implements ROL03.
//
// Two roles held by one member that grant nearly the same privileges are two
// names for one thing: revoking one changes nothing, and a reader checking what
// a principal can do has to read both to find out that it did not matter.
//
// The threshold is written down rather than tuned: at least two shared
// privileges, and at least half of the smaller role's set. One shared privilege
// is a coincidence on any real schema, and a fraction below half describes
// roles that genuinely differ.
func findOverlappingRoles(db *schemamodel.Database, memberships []RoleMembership) []Finding {
	privileges := make(map[string]map[string]bool, len(db.Roles))
	for _, role := range db.Roles {
		privileges[strings.TrimSpace(role.Name)] = privilegeSetOfRole(db, strings.TrimSpace(role.Name))
	}

	byMember := make(map[string][]string, len(memberships))
	for _, membership := range memberships {
		if !heldForItsPrivileges(membership) {
			continue
		}
		member := strings.TrimSpace(membership.Member)
		byMember[member] = append(byMember[member], strings.TrimSpace(membership.Role))
	}

	findings := make([]Finding, 0)
	for _, member := range sortedKeys(setOf(byMember)) {
		roles := byMember[member]
		slices.Sort(roles)
		for i := range roles {
			for j := i + 1; j < len(roles); j++ {
				shared, ratio := overlap(privileges[roles[i]], privileges[roles[j]])
				if len(shared) < 2 || ratio < 0.5 {
					continue
				}
				findings = append(findings, Finding{
					Code:     "ROL03",
					Severity: risk.Info,
					Subject:  Subject{Kind: "role", Name: member},
					Message: "roles " + roles[i] + " and " + roles[j] + " are both held by " + member +
						" and grant " + strconv.Itoa(len(shared)) + " of the same privileges (" +
						strconv.Itoa(int(ratio*100)) + "% of the smaller one)",
					Detail:     Detail{Privileges: shared, Roles: []string{roles[i], roles[j]}},
					Suggestion: "consolidate them, or record what the difference between them is for",
				})
			}
		}
	}
	return findings
}

// privilegeSetOfRole is every privilege a role holds, keyed by privilege and
// object so that SELECT on one table and SELECT on another are different
// members.
func privilegeSetOfRole(db *schemamodel.Database, role string) map[string]bool {
	held := make(map[string]bool)
	for _, grant := range db.Grants {
		if strings.TrimSpace(grant.Role) != role {
			continue
		}
		object, kind := grantTarget(grant)
		if object == "" {
			continue
		}
		for _, privilege := range normalizedPrivileges(grant.Privileges) {
			held[privilege+" ON "+kind+" "+object] = true
		}
	}
	return held
}

// privilegesOfRole is the sorted privilege list for a finding's detail.
func privilegesOfRole(db *schemamodel.Database, role string) []string {
	return sortedKeys(privilegeSetOfRole(db, role))
}

// overlap returns the shared members and their share of the smaller set. Two
// empty sets overlap in nothing rather than in everything.
func overlap(left, right map[string]bool) ([]string, float64) {
	smaller, larger := left, right
	if len(right) < len(left) {
		smaller, larger = right, left
	}
	if len(smaller) == 0 {
		return make([]string, 0), 0
	}
	shared := make(map[string]bool, len(smaller))
	for member := range smaller {
		if larger[member] {
			shared[member] = true
		}
	}
	return sortedKeys(shared), float64(len(shared)) / float64(len(smaller))
}

// setOf turns a keyed map into a set, so sortedKeys can order its keys.
func setOf(byMember map[string][]string) map[string]bool {
	keys := make(map[string]bool, len(byMember))
	for key := range byMember {
		keys[key] = true
	}
	return keys
}

// grantTarget names what a grant is on, and the kind of that object.
func grantTarget(grant schemamodel.Grant) (name, kind string) {
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
