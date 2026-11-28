package schemasecurity

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/rowaccess"
	"ptah.run/migration/risk"
)

// The rules that measure a role's reach. They are named rather than numbered:
// a numbered code belongs to the Atlas compatibility surface, and Atlas has no
// rule these map to.
const (
	// ReadReachRule reports a role that can read more of the database's
	// relations than the policy allows.
	ReadReachRule = "roles/read-reach"
	// WriteReachRule reports a role that can change the rows of more of the
	// database's relations than the policy allows.
	WriteReachRule = "roles/write-reach"
)

// DefaultReachWarnPercent and DefaultReachErrorPercent are the thresholds a
// caller that states none uses: a role that reaches more than half of the
// relations asks for review, and no reach is an error until the caller lowers
// the error threshold, since no reach exceeds all of the relations. The owner
// of every table reaches all of them, and that is how most applications that
// run their own migrations are deployed, so a default that blocked would fail
// nearly every database it met rather than the one that needs attention.
const (
	DefaultReachWarnPercent  = 50
	DefaultReachErrorPercent = 100
)

// Reach is what the reach rules measure over: the relations of a database,
// every privilege on them, the roles and how they reach one another, and the
// access to every relation that no grant on an object states.
//
// It is built from a catalog read by [ReachFromCatalog], because the facts it
// needs are ones a description leaves out: the privileges an owner holds
// because it owns the object, the roles nothing in the inspected schemas
// refers to, and the access a superuser, a predefined role or a server-wide
// privilege gives.
type Reach struct {
	// Relations are the tables, views and materialized views counted.
	Relations []ReachRelation
	// Grants are the read and write privileges on relations and on whole
	// schemas, the access to every relation of one schema among them.
	Grants []ReachGrant
	// Principals are the roles whose reach is measured: every role the read
	// found, whether the description defines it or not.
	Principals []ReachPrincipal
	// Memberships are the role-in-role edges a member can use, by inheriting
	// the role's privileges or by switching to it.
	Memberships []ReachMembership
	// WideAccess is the access to every relation that no grant states. An
	// access to every relation of one schema is in Grants, which names its
	// source.
	WideAccess []catalog.WideAccess
	// ReadOnly are the counted relations whose rows no role can change, the
	// materialized views: the write rule leaves them out of its count.
	ReadOnly []ReachRelation
	// WarnPercent and ErrorPercent are the thresholds: a role that reaches
	// more than WarnPercent of the relations is a warning, and more than
	// ErrorPercent an error.
	WarnPercent  int
	ErrorPercent int
	// PublicIsRole reports a read in which PUBLIC names a role rather than
	// every role, as [catalog.Database.PublicIsRole] says: a grant to it, and
	// a membership of it, are then that role's alone.
	PublicIsRole bool
	// Unread are the roles whose privileges the read did not read, each with
	// its cause, as [catalog.Database.UnreadRoles] says. The reach of a role
	// that can use one is not measured, and the rules say which roles they
	// left out and why.
	Unread []catalog.UnreadRole
	// EveryRoleHoldsUnread reports that every role holds roles whose
	// privileges the read does not state, and why, as
	// [catalog.Database.EveryAccountHoldsUnread] says. No role's reach is
	// measured then. Nil where every role holds only what the read states.
	EveryRoleHoldsUnread *catalog.EveryAccountUnread
}

// ReachRelation names a relation the same way whichever object reports it.
type ReachRelation struct {
	// Schema is empty for the schema or database the connection resolved,
	// and the schema's name otherwise.
	Schema string
	// Name is the relation's name.
	Name string
}

// ReachGrant is one read or write privilege a role holds on a relation, or on
// every relation of a schema.
type ReachGrant struct {
	// Role holds the privilege; PUBLIC is every role.
	Role string
	// Relation is the relation the privilege is on. A grant on a schema sets
	// only its Schema.
	Relation ReachRelation
	// OnSchema reports a grant on every relation of Relation.Schema.
	OnSchema bool
	// Owned reports a privilege the role holds because it owns the relation,
	// which no revoke narrows for good: the owner can grant it back.
	Owned bool
	// Source names a grant that the schema's own grants do not state and a
	// revoke on a relation does not narrow, such as a MySQL grant on a
	// database name pattern, "SELECT ON `ab%`.*". A finding names it in
	// detail.reach.sources. It is empty for every other grant.
	Source string
	// Subject, where set, is the one role whose reach the grant counts
	// toward, as [catalog.WideAccess.Subject] says: a member of Role does not
	// gain it through Role. A grant to PUBLIC, or one whose subject is
	// PUBLIC, counts toward every role, since every role holds what PUBLIC
	// holds. Only a database-level row sets it.
	Subject string
	// Read and Write classify the privilege.
	Read, Write bool
}

// ReachMembership is one role-in-role edge a member can use.
type ReachMembership struct {
	// Role is the role whose privileges are granted.
	Role string
	// Member is the role that receives them.
	Member string
	// CannotSwitch reports an edge the member cannot SET ROLE through, which
	// PostgreSQL 16 states with SET FALSE. The member still inherits the
	// role's privileges, but not its superuser bit, which only a switch
	// confers.
	CannotSwitch bool
	// CannotInherit reports an edge that does not pass the role's privileges
	// on, which PostgreSQL 16 states with INHERIT FALSE. The member has them
	// only after it switches to the role.
	CannotInherit bool
}

// ReachPrincipal is one role whose reach is measured.
type ReachPrincipal struct {
	// Name is the role's name.
	Name string
	// Superuser reports a role that bypasses every privilege check, and so
	// reaches every relation. A member that can switch to it reaches as much.
	Superuser bool
}

// ReachDetail is the measurement behind a reach finding.
type ReachDetail struct {
	// Reached is the number of relations the role reaches.
	Reached int `json:"reached"`
	// Relations is the number of relations counted.
	Relations int `json:"relations"`
	// ThresholdPercent is the threshold the role exceeds.
	ThresholdPercent int `json:"threshold_percent"`
	// Through are the roles whose privileges the role reaches through
	// membership, sorted.
	Through []string `json:"through,omitempty"`
	// Sources name the access that a revoke on a relation does not narrow,
	// such as superuser, pg_read_all_data or the ownership of relations,
	// sorted.
	Sources []string `json:"sources,omitempty"`
}

// ReachFromCatalog builds the reach input from a catalog read, or returns nil
// when the read did not establish the access no grant states, since a reach
// measured without it reports a superuser or a member of pg_read_all_data as
// reaching nothing. defaultSchema is the schema or database the connection
// resolved: a relation the read reports with no schema belongs to it, and a
// grant that names it names the same relation.
//
// An access to every relation of one schema, such as a MySQL grant on a
// database name pattern that reaches the database, counts as a grant on that
// schema that names its source, for the subject the read decided it for.
// Where [catalog.Database.SchemaGrantsInWideAccess] holds, the read decided
// the grants on a whole schema that way too, and the ones in Grants are left
// out.
func ReachFromCatalog(live *catalog.Database, defaultSchema string, warnPercent, errorPercent int) *Reach {
	if live == nil || live.WideAccess == nil || live.RoleMemberships == nil {
		return nil
	}
	reach := &Reach{
		Grants:               reachGrants(grantsOutsideWideAccess(live), defaultSchema),
		Memberships:          reachMemberships(live.RoleMemberships),
		WideAccess:           make([]catalog.WideAccess, 0, len(live.WideAccess)),
		WarnPercent:          warnPercent,
		ErrorPercent:         errorPercent,
		PublicIsRole:         live.PublicIsRole,
		Unread:               slices.Clone(live.UnreadRoles),
		EveryRoleHoldsUnread: live.EveryAccountHoldsUnread,
	}
	for _, held := range live.WideAccess {
		if held.Schema == "" {
			reach.WideAccess = append(reach.WideAccess, held)
			continue
		}
		reach.Grants = append(reach.Grants, ReachGrant{
			Role: held.Role, Relation: ReachRelation{Schema: schemaKey(held.Schema, defaultSchema)}, OnSchema: true,
			Source: held.Source, Subject: held.Subject, Read: held.Read, Write: held.Write,
		})
	}
	for _, table := range live.Tables {
		reach.Relations = append(reach.Relations, ReachRelation{Schema: schemaKey(table.Schema, defaultSchema), Name: table.Name})
	}
	for _, view := range live.Views {
		reach.Relations = append(reach.Relations, ReachRelation{Schema: schemaKey(view.Schema, defaultSchema), Name: view.Name})
	}
	for _, view := range live.MatViews {
		relation := ReachRelation{Schema: schemaKey(view.Schema, defaultSchema), Name: view.Name}
		reach.Relations = append(reach.Relations, relation)
		reach.ReadOnly = append(reach.ReadOnly, relation)
	}
	slices.SortFunc(reach.Relations, compareRelations)
	reach.Relations = slices.Compact(reach.Relations)
	for _, roles := range [][]catalog.Role{live.Roles, live.RolesOutOfScope} {
		for _, role := range roles {
			reach.Principals = append(reach.Principals, ReachPrincipal{Name: role.Name, Superuser: role.Superuser})
		}
	}
	return reach
}

// grantsOutsideWideAccess is the grants of live that the wide access does not
// state: every grant, or where [catalog.Database.SchemaGrantsInWideAccess]
// holds, every grant but those on a whole schema.
func grantsOutsideWideAccess(live *catalog.Database) []catalog.Grant {
	if !live.SchemaGrantsInWideAccess {
		return live.Grants
	}
	return slices.DeleteFunc(slices.Clone(live.Grants), func(grant catalog.Grant) bool {
		return strings.EqualFold(strings.TrimSpace(grant.ObjectType), "SCHEMA")
	})
}

// reachGrants keeps the grants that read or change rows of a whole relation,
// or of every relation in a schema. A privilege on some columns of a table
// does not reach the table. A privilege an owner revoked from itself still
// reaches the relation, since the owner can grant it back; any other
// subtraction from a broader grant is not modeled, so it is left out rather
// than counted as access.
func reachGrants(grants []catalog.Grant, defaultSchema string) []ReachGrant {
	out := make([]ReachGrant, 0, len(grants))
	for _, grant := range grants {
		read, write := rowaccess.Of(grant.Privilege)
		owned := grant.OwnerDefault() || grant.OwnerRevoked()
		if grant.IsPartialRevoke && !owned || grant.Column != "" || !read && !write {
			continue
		}
		entry := ReachGrant{Role: grant.Role, Owned: owned, Read: read, Write: write}
		switch strings.ToUpper(strings.TrimSpace(grant.ObjectType)) {
		case "TABLE", "VIEW", "MATERIALIZED VIEW":
			entry.Relation = ReachRelation{Schema: schemaKey(grant.Schema, defaultSchema), Name: grant.ObjectName}
		case "SCHEMA":
			entry.OnSchema = true
			entry.Relation.Schema = schemaKey(grant.ObjectName, defaultSchema)
		default:
			continue
		}
		out = append(out, entry)
	}
	return out
}

// reachMemberships keeps the edges a member can use. One that neither
// inherits the privileges nor may switch to the role, which PostgreSQL 16 can
// state, reaches nothing.
func reachMemberships(memberships []catalog.RoleMembership) []ReachMembership {
	out := make([]ReachMembership, 0, len(memberships))
	for _, membership := range memberships {
		noInherit := membership.InheritOption != nil && !*membership.InheritOption
		noSwitch := membership.SetOption != nil && !*membership.SetOption
		if noInherit && noSwitch {
			continue
		}
		out = append(out, ReachMembership{
			Role: membership.Role, Member: membership.Member, CannotSwitch: noSwitch, CannotInherit: noInherit,
		})
	}
	return out
}

// compareRelations orders relations by schema, then name.
func compareRelations(a, b ReachRelation) int {
	return cmp.Or(cmp.Compare(a.Schema, b.Schema), cmp.Compare(a.Name, b.Name))
}

// schemaKey is empty for the default schema and the schema's name otherwise.
func schemaKey(schema, defaultSchema string) string {
	if schema == defaultSchema {
		return ""
	}
	return schema
}

// findBroadReach implements roles/read-reach and roles/write-reach, and
// returns the rules it ran for some roles alone, each with the reason.
//
// A role reaches a relation when a role it can use holds a read or write
// privilege on the whole relation or on its schema, owns it, or holds access
// to every relation: superuser, a predefined role that reads or writes all
// data, a server-wide privilege. A privilege on a schema includes one granted
// on a database name pattern that reaches it, which counts only for the role
// the grant names as its subject. PUBLIC's privileges are every role's. A
// role can use itself, the roles it can switch to with SET ROLE through a
// chain of memberships that each allow it, and the roles any of those
// inherits from through a chain of memberships that each pass privileges on;
// a chain that mixes the two reaches no further than its first link of the
// other kind, which is how PostgreSQL 16 reads the options. A superuser bit
// counts only on a role the subject can switch to, since PostgreSQL does not
// pass it on.
//
// Every role holds what PUBLIC holds, so a role that reaches no more than
// PUBLIC is not reported: the PUBLIC finding already names that reach, and one
// finding per role would bury it. Each rule names the sources of its own
// reach: a role that reads everything and writes through grants is told to
// take the read access away for the read rule alone. Where PUBLIC is a role
// of that name ([Reach.PublicIsRole]), it is measured as any other role, and
// no role holds what it holds through no membership.
//
// A role that can use a role whose privileges the read did not read
// ([Reach.Unread]) is not measured, since its reach would be reported short,
// and neither is any role where every role holds such roles
// ([Reach.EveryRoleHoldsUnread]).
func findBroadReach(reach *Reach) ([]Finding, []SkippedRule) {
	if unread := reach.EveryRoleHoldsUnread; unread != nil {
		held := "roles"
		if len(unread.Roles) > 0 {
			held = "the roles " + strings.Join(unread.Roles, ", ")
		}
		reason := "every role holds " + held + " whose memberships the read does not state: " + unread.Cause +
			"; so no role's reach was measured"
		return nil, []SkippedRule{{Code: ReadReachRule, Reason: reason}, {Code: WriteReachRule, Reason: reason}}
	}
	index := indexRelations(reach.Relations)
	if len(index.counted) == 0 {
		return nil, nil
	}
	readOnly := make(map[ReachRelation]bool)
	for _, relation := range reach.ReadOnly {
		if index.counted[relation] {
			readOnly[relation] = true
		}
	}
	subjects, superusers := reachSubjects(reach)
	everyRole := !reach.PublicIsRole

	// Every account holds PUBLIC and the roles granted to it: measured on
	// MariaDB 11.8.9 and 12.3.3, after `GRANT r TO PUBLIC` a login granted
	// nothing reads the tables r reads, and is refused them with Error 1142
	// once the grant is revoked.
	publicOnly := roleAccess{everyRole: everyRole}
	if everyRole {
		publicOnly = usableRoles(publicRole, reach.Memberships, everyRole)
	}
	public := reachedRelations(reach, index, publicOnly, superusers, readOnly)
	findings := make([]Finding, 0)
	unread := make(map[catalog.UnreadRole][]string)
	for _, subject := range subjects {
		isPublic := everyRole && strings.EqualFold(subject, publicRole)
		access := usableRoles(subject, reach.Memberships, everyRole)
		through := throughRoles(subject, access)
		if !isPublic {
			maps.Copy(access.usable, publicOnly.usable)
		}
		if held := slices.IndexFunc(reach.Unread, func(role catalog.UnreadRole) bool { return access.usable[role.Name] }); held >= 0 {
			unread[reach.Unread[held]] = append(unread[reach.Unread[held]], subject)
			continue
		}
		reached := reachedRelations(reach, index, access, superusers, readOnly)
		for _, measured := range []struct {
			rule, verb      string
			reached, public ruleReach
			total           int
		}{
			{rule: ReadReachRule, verb: "read", reached: reached.read, public: public.read, total: len(index.counted)},
			{rule: WriteReachRule, verb: "change the rows of", reached: reached.write, public: public.write,
				total: len(index.counted) - len(readOnly)},
		} {
			// The usable roles include PUBLIC's, so the set only grows from
			// PUBLIC's, and an equal size is an equal set.
			if !isPublic && len(measured.reached.relations) == len(measured.public.relations) {
				continue
			}
			finding, found := reachFinding(subject, measured.rule, measured.verb, len(measured.reached.relations), measured.total, reach)
			if !found {
				continue
			}
			finding.Detail.Reach.Through = through
			finding.Detail.Reach.Sources = sortedKeys(measured.reached.sources)
			switch {
			case measured.reached.sourceDominates(reached.owned) && measured.reached.pattern:
				finding.Suggestion = patternSourceSuggestion
			case measured.reached.sourceDominates(reached.owned):
				finding.Suggestion = sourceSuggestion
			}
			findings = append(findings, finding)
		}
	}
	return findings, unreadSkips(unread)
}

// reachSubjects is each principal the reach rules measure, sorted, with
// PUBLIC where it stands for every role and the read names it, and the
// superusers among them.
func reachSubjects(reach *Reach) ([]string, map[string]bool) {
	superusers := make(map[string]bool)
	subjects := make([]string, 0, len(reach.Principals)+1)
	for _, principal := range reach.Principals {
		subjects = append(subjects, principal.Name)
		if principal.Superuser {
			superusers[principal.Name] = true
		}
	}
	if !reach.PublicIsRole && namesPublic(reach) {
		subjects = append(subjects, publicRole)
	}
	slices.Sort(subjects)
	return slices.Compact(subjects), superusers
}

// throughRoles is each role other than subject whose privileges access lets
// subject use, sorted.
func throughRoles(subject string, access roleAccess) []string {
	through := make([]string, 0, len(access.usable))
	for role := range access.usable {
		if role != subject {
			through = append(through, role)
		}
	}
	slices.Sort(through)
	return through
}

// unreadSkips says, for each role whose privileges the read did not read,
// why, and which roles holding it the reach rules left unmeasured.
func unreadSkips(unread map[catalog.UnreadRole][]string) []SkippedRule {
	skipped := make([]SkippedRule, 0, 2*len(unread))
	roles := slices.SortedFunc(maps.Keys(unread), func(a, b catalog.UnreadRole) int { return strings.Compare(a.Name, b.Name) })
	for _, role := range roles {
		reason := "the read did not read every privilege of " + role.Name + ", which " + strings.Join(unread[role], ", ") +
			" can use: " + role.Cause + "; so their reach was not measured"
		skipped = append(skipped, SkippedRule{Code: ReadReachRule, Reason: reason}, SkippedRule{Code: WriteReachRule, Reason: reason})
	}
	return skipped
}

// namesPublic reports whether the reach holds anything for PUBLIC: a grant,
// access to every relation, or a role granted to it. PUBLIC is then a subject
// of its own, since it is no principal a read lists and every account holds
// what it reaches.
func namesPublic(reach *Reach) bool {
	return slices.ContainsFunc(reach.Grants, func(grant ReachGrant) bool { return strings.EqualFold(grant.Role, publicRole) }) ||
		slices.ContainsFunc(reach.WideAccess, func(held catalog.WideAccess) bool { return strings.EqualFold(held.Role, publicRole) }) ||
		slices.ContainsFunc(reach.Memberships, func(membership ReachMembership) bool {
			return strings.EqualFold(membership.Member, publicRole)
		})
}

// sourceSuggestion asks to take the sources away first.
const sourceSuggestion = "first take away what detail.reach.sources names, since a revoke on a relation does " +
	"not narrow it: remove superuser, a role that reads or writes all data and a privilege on every database, and " +
	"give the relations to an owner the application does not connect as; then grant the role only the relations " +
	"it needs"

// patternSourceSuggestion is [sourceSuggestion] for a finding whose sources
// name a grant on a database name pattern, which it asks to remove as well.
const patternSourceSuggestion = "first take away what detail.reach.sources names, since a revoke on a relation " +
	"does not narrow it: remove superuser, a role that reads or writes all data and a privilege on every database " +
	"or on a database name pattern, and give the relations to an owner the application does not connect as; then " +
	"grant the role only the relations it needs"

// roleAccess is the roles a subject can use: usable holds every role whose
// privileges the subject has, by inheriting them or by switching first, and
// switchable the roles it can become with SET ROLE, itself included.
// everyRole reports that PUBLIC stands for every role.
type roleAccess struct {
	subject            string
	usable, switchable map[string]bool
	everyRole          bool
}

// usableRoles is the access subject has through memberships. everyRole is
// [roleAccess.everyRole].
func usableRoles(subject string, memberships []ReachMembership, everyRole bool) roleAccess {
	switchable := roleClosure(subject, memberships, func(membership ReachMembership) bool { return !membership.CannotSwitch })
	usable := make(map[string]bool)
	for role := range switchable {
		maps.Copy(usable, roleClosure(role, memberships, func(membership ReachMembership) bool { return !membership.CannotInherit }))
	}
	return roleAccess{subject: subject, usable: usable, switchable: switchable, everyRole: everyRole}
}

// holds reports whether the subject has the privileges of grant: through a
// role it can use, or, for a grant that names its subject, as that subject.
// Where PUBLIC stands for every role, a grant to PUBLIC, and a grant whose
// subject is PUBLIC, are held by every role that holds PUBLIC's privileges.
func (a roleAccess) holds(grant ReachGrant) bool {
	public := a.everyRole && a.usable[publicRole] &&
		(strings.EqualFold(grant.Role, publicRole) || strings.EqualFold(grant.Subject, publicRole))
	if grant.Subject != "" {
		return grant.Subject == a.subject || public
	}
	return a.usable[grant.Role] || public
}

// roleClosure is start and every role it reaches through the memberships
// follow accepts, directly or through a chain of them.
func roleClosure(start string, memberships []ReachMembership, follow func(ReachMembership) bool) map[string]bool {
	seen := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		member := queue[0]
		queue = queue[1:]
		for _, membership := range memberships {
			if membership.Member != member || seen[membership.Role] || !follow(membership) {
				continue
			}
			seen[membership.Role] = true
			queue = append(queue, membership.Role)
		}
	}
	return seen
}

// relationReach is what a subject reaches, by rule, and the relations a
// usable role owns, which every rule counts.
type relationReach struct {
	read, write ruleReach
	owned       map[ReachRelation]bool
}

// ruleReach is what a subject reaches under one rule: the relations, the
// sources of that access a revoke on a relation does not narrow, and which
// of those sources it holds.
type ruleReach struct {
	relations map[ReachRelation]bool
	// sources name the access a revoke on a relation does not narrow.
	sources map[string]bool
	// sourced are the relations reached through a grant that names its
	// source.
	sourced map[ReachRelation]bool
	// wide reports access to every relation, which no grant states.
	wide bool
	// pattern reports a source that is a grant on a database name pattern.
	pattern bool
}

// newRuleReach is a ruleReach that reaches nothing.
func newRuleReach() ruleReach {
	return ruleReach{relations: make(map[ReachRelation]bool), sources: make(map[string]bool), sourced: make(map[ReachRelation]bool)}
}

// sourceDominates reports whether the sources explain most of the rule's
// reach: access to every relation always does, and the relations in owned
// and those reached through a grant that names a source do when they are
// more than half of it. Otherwise the other grants are the cause, and the
// suggestion names them.
func (r ruleReach) sourceDominates(owned map[ReachRelation]bool) bool {
	if r.wide {
		return true
	}
	fixed := 0
	for relation := range r.relations {
		if owned[relation] || r.sourced[relation] {
			fixed++
		}
	}
	return fixed*2 > len(r.relations)
}

// reachedRelations is what the roles access can use reach among the counted
// relations. superusers marks the roles with the superuser bit, which counts
// only on a role access can switch to. No role changes the rows of a relation
// in readOnly.
func reachedRelations(
	reach *Reach, index relationIndex, access roleAccess, superusers map[string]bool, readOnly map[ReachRelation]bool,
) relationReach {
	result := relationReach{read: newRuleReach(), write: newRuleReach(), owned: make(map[ReachRelation]bool)}
	result.addWideAccess(reach, access, superusers, readOnly)
	for owner, relations := range result.addGrants(reach, index, access, readOnly) {
		source := ownershipSource(len(relations), owner)
		result.read.sources[source] = true
		result.write.sources[source] = true
	}
	return result
}

// addWideAccess adds every relation for the superuser bits and the access to
// every relation that access can use, and names each such source under the
// rule it gives.
func (r *relationReach) addWideAccess(reach *Reach, access roleAccess, superusers map[string]bool, readOnly map[ReachRelation]bool) {
	for role := range access.switchable {
		if superusers[role] {
			r.read.addEvery(reach.Relations, nil, "superuser held by "+role)
			r.write.addEvery(reach.Relations, readOnly, "superuser held by "+role)
		}
	}
	for _, held := range reach.WideAccess {
		// A role that holds the access only by switching to the predefined
		// role passes it to a subject that can switch to it, and to no other.
		if !access.usable[held.Role] || held.SwitchOnly && !access.switchable[held.Role] {
			continue
		}
		if held.Read {
			r.read.addEvery(reach.Relations, nil, held.Source+" held by "+held.Role)
		}
		if held.Write {
			r.write.addEvery(reach.Relations, readOnly, held.Source+" held by "+held.Role)
		}
	}
}

// addEvery adds each of relations except those in skip, from the access to
// every relation that source names.
func (r *ruleReach) addEvery(relations []ReachRelation, skip map[ReachRelation]bool, source string) {
	r.wide = true
	r.sources[source] = true
	for _, relation := range relations {
		if !skip[relation] {
			r.relations[relation] = true
		}
	}
}

// addGrants adds the counted relations the grants that access holds reach,
// names each grant naming a source under the rule it gives where it reaches
// one, and returns, by owner, the relations a usable role owns.
func (r *relationReach) addGrants(
	reach *Reach, index relationIndex, access roleAccess, readOnly map[ReachRelation]bool,
) map[string]map[ReachRelation]bool {
	ownedBy := make(map[string]map[ReachRelation]bool)
	for _, grant := range reach.Grants {
		if !access.holds(grant) {
			continue
		}
		reached := index.reachedBy(grant)
		if grant.Read {
			r.read.addGrant(grant, reached, nil)
		}
		if grant.Write {
			r.write.addGrant(grant, reached, readOnly)
		}
		if !grant.Owned {
			continue
		}
		for _, relation := range reached {
			if ownedBy[grant.Role] == nil {
				ownedBy[grant.Role] = make(map[ReachRelation]bool)
			}
			ownedBy[grant.Role][relation] = true
			r.owned[relation] = true
		}
	}
	return ownedBy
}

// addGrant adds the relations of reached outside skip, and names grant's
// source where it reaches one.
func (r *ruleReach) addGrant(grant ReachGrant, reached []ReachRelation, skip map[ReachRelation]bool) {
	added := false
	for _, relation := range reached {
		if skip[relation] {
			continue
		}
		added = true
		r.relations[relation] = true
		r.sourced[relation] = r.sourced[relation] || grant.Source != ""
	}
	if grant.Source != "" && added {
		r.sources[grant.Source+" held by "+grant.Role] = true
		r.pattern = r.pattern || grant.Subject != ""
	}
}

// ownershipSource names the relations owner owns among those counted.
func ownershipSource(count int, owner string) string {
	noun := "relations"
	if count == 1 {
		noun = "relation"
	}
	return "ownership of " + strconv.Itoa(count) + " " + noun + " held by " + owner
}

// relationIndex finds the counted relations a grant reaches without a pass
// over every relation for every grant.
type relationIndex struct {
	counted  map[ReachRelation]bool
	bySchema map[string][]ReachRelation
}

// reachedBy is the counted relations grant is on: every one of its schema for
// a grant on the schema, and its relation if that is counted otherwise.
func (ix relationIndex) reachedBy(grant ReachGrant) []ReachRelation {
	if grant.OnSchema {
		return ix.bySchema[grant.Relation.Schema]
	}
	if ix.counted[grant.Relation] {
		return []ReachRelation{grant.Relation}
	}
	return nil
}

func indexRelations(relations []ReachRelation) relationIndex {
	index := relationIndex{counted: make(map[ReachRelation]bool, len(relations)), bySchema: make(map[string][]ReachRelation)}
	for _, relation := range relations {
		index.counted[relation] = true
		index.bySchema[relation.Schema] = append(index.bySchema[relation.Schema], relation)
	}
	return index
}

// reachPercent is reached as a share of total, to a tenth of a percent where
// the share is not whole, so a share just above a threshold does not read as
// equal to it.
func reachPercent(reached, total int) string {
	tenths := reached * 1000 / total
	if tenths%10 == 0 {
		return strconv.Itoa(tenths/10) + "%"
	}
	return strconv.Itoa(tenths/10) + "." + strconv.Itoa(tenths%10) + "%"
}

// reachFinding is the finding for a role that reaches reached of total
// relations, if that exceeds a threshold.
func reachFinding(subject, rule, verb string, reached, total int, reach *Reach) (Finding, bool) {
	severity, threshold := risk.Severity(""), 0
	switch {
	case reached*100 > reach.ErrorPercent*total:
		severity, threshold = risk.Error, reach.ErrorPercent
	case reached*100 > reach.WarnPercent*total:
		severity, threshold = risk.Warning, reach.WarnPercent
	default:
		return Finding{}, false
	}
	level := "warning"
	if severity == risk.Error {
		level = "error"
	}
	return Finding{
		Code:     rule,
		Severity: severity,
		Subject:  Subject{Kind: "role", Name: subject},
		Message: "role " + subject + " can " + verb + " " + strconv.Itoa(reached) + " of " + strconv.Itoa(total) +
			" relations (" + reachPercent(reached, total) + "), above the " + level + " threshold of " +
			strconv.Itoa(threshold) + "%",
		Detail: Detail{Reach: &ReachDetail{Reached: reached, Relations: total, ThresholdPercent: threshold}},
		Suggestion: "grant the role only the relations it needs, or split it into narrower roles; " +
			"detail.reach.through names the roles it reaches them through",
	}, true
}
