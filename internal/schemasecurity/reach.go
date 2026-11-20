package schemasecurity

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
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
// relations asks for review, and one that reaches more than three quarters
// blocks.
const (
	DefaultReachWarnPercent  = 50
	DefaultReachErrorPercent = 75
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
	// schemas.
	Grants []ReachGrant
	// Principals are the roles whose reach is measured: every role the read
	// found, whether the description defines it or not.
	Principals []ReachPrincipal
	// Memberships are the role-in-role edges a member can use, by inheriting
	// the role's privileges or by switching to it.
	Memberships []ReachMembership
	// WideAccess is the access to every relation that no grant states.
	WideAccess []catalog.WideAccess
	// WarnPercent and ErrorPercent are the thresholds: a role that reaches
	// more than WarnPercent of the relations is a warning, and more than
	// ErrorPercent an error.
	WarnPercent  int
	ErrorPercent int
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
	// Sources name the access to every relation that the role holds or
	// reaches, such as superuser or pg_read_all_data, sorted.
	Sources []string `json:"sources,omitempty"`
}

// ReachFromCatalog builds the reach input from a catalog read, or returns nil
// when the read did not establish the access no grant states, since a reach
// measured without it reports a superuser or a member of pg_read_all_data as
// reaching nothing. defaultSchema is the schema or database the connection
// resolved: a relation the read reports with no schema belongs to it, and a
// grant that names it names the same relation.
func ReachFromCatalog(live *catalog.Database, defaultSchema string, warnPercent, errorPercent int) *Reach {
	if live == nil || live.WideAccess == nil || live.RoleMemberships == nil {
		return nil
	}
	reach := &Reach{
		Grants:       reachGrants(live.Grants, defaultSchema),
		Memberships:  reachMemberships(live.RoleMemberships),
		WideAccess:   live.WideAccess,
		WarnPercent:  warnPercent,
		ErrorPercent: errorPercent,
	}
	for _, table := range live.Tables {
		reach.Relations = append(reach.Relations, ReachRelation{Schema: schemaKey(table.Schema, defaultSchema), Name: table.Name})
	}
	for _, view := range live.Views {
		reach.Relations = append(reach.Relations, ReachRelation{Schema: schemaKey(view.Schema, defaultSchema), Name: view.Name})
	}
	for _, view := range live.MatViews {
		reach.Relations = append(reach.Relations, ReachRelation{Schema: schemaKey(view.Schema, defaultSchema), Name: view.Name})
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

// reachGrants keeps the grants that read or change rows of a relation, or of
// every relation in a schema. A privilege an owner revoked from itself is a
// privilege it does not hold.
func reachGrants(grants []catalog.Grant, defaultSchema string) []ReachGrant {
	out := make([]ReachGrant, 0, len(grants))
	for _, grant := range grants {
		read, write := classifyPrivilege(grant.Privilege)
		if grant.IsPartialRevoke || !read && !write {
			continue
		}
		entry := ReachGrant{Role: grant.Role, Read: read, Write: write}
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
		out = append(out, ReachMembership{Role: membership.Role, Member: membership.Member, CannotSwitch: noSwitch})
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

// classifyPrivilege says whether a privilege reads rows, changes them, or
// neither. ALL is both, for an engine that reports it unexpanded.
func classifyPrivilege(privilege string) (read, write bool) {
	switch strings.ToUpper(strings.TrimSpace(privilege)) {
	case "SELECT":
		return true, false
	case "INSERT", "UPDATE", "DELETE", "TRUNCATE":
		return false, true
	case "ALL", "ALL PRIVILEGES":
		return true, true
	default:
		return false, false
	}
}

// findBroadReach implements roles/read-reach and roles/write-reach.
//
// A role reaches a relation when it, PUBLIC, or a role it can use through
// membership holds a read or write privilege on the relation or on its schema,
// owns it, or holds access to every relation: superuser, a predefined role
// that reads or writes all data, a server-wide privilege. A membership counts
// whether the member inherits the role's privileges or must switch to the role
// first, because a role that can switch can read.
//
// Every role holds what PUBLIC holds, so a role that reaches no more than
// PUBLIC is not reported: the PUBLIC finding already names that reach, and one
// finding per role would bury it.
func findBroadReach(reach *Reach) []Finding {
	index := indexRelations(reach.Relations)
	total := len(index.counted)
	if total == 0 {
		return nil
	}
	superusers := make(map[string]bool)
	subjects := make([]string, 0, len(reach.Principals)+1)
	for _, principal := range reach.Principals {
		subjects = append(subjects, principal.Name)
		if principal.Superuser {
			superusers[principal.Name] = true
		}
	}
	if slices.ContainsFunc(reach.Grants, func(grant ReachGrant) bool { return strings.EqualFold(grant.Role, publicRole) }) {
		subjects = append(subjects, publicRole)
	}
	slices.Sort(subjects)
	subjects = slices.Compact(subjects)

	publicRead, publicWrite, _ := reachedRelations(reach, index, map[string]bool{publicRole: true}, nil)
	findings := make([]Finding, 0)
	for _, subject := range subjects {
		isPublic := strings.EqualFold(subject, publicRole)
		through := reachableRoles(subject, reach.Memberships, anyEdge)
		// A superuser bit counts on the subject itself and on a role it can
		// switch to; inheriting it is not something PostgreSQL does.
		super := make(map[string]bool)
		for _, role := range append(reachableRoles(subject, reach.Memberships, switchEdge), subject) {
			super[role] = superusers[role]
		}
		holders := map[string]bool{subject: true}
		for _, role := range through {
			holders[role] = true
		}
		if !isPublic {
			holders[publicRole] = true
		}
		read, write, sources := reachedRelations(reach, index, holders, super)
		for _, measured := range []struct {
			rule     string
			verb     string
			relation map[ReachRelation]bool
			public   map[ReachRelation]bool
		}{
			{rule: ReadReachRule, verb: "read", relation: read, public: publicRead},
			{rule: WriteReachRule, verb: "change the rows of", relation: write, public: publicWrite},
		} {
			// The holders include PUBLIC, so the set only grows from PUBLIC's,
			// and an equal size is an equal set.
			if !isPublic && len(measured.relation) == len(measured.public) {
				continue
			}
			finding, found := reachFinding(subject, measured.rule, measured.verb, len(measured.relation), total, reach)
			if !found {
				continue
			}
			finding.Detail.Reach.Through = through
			finding.Detail.Reach.Sources = sources
			if len(sources) > 0 {
				finding.Suggestion = "first take away what detail.reach.sources names -- superuser, a role that " +
					"reads or writes all data, a privilege on every database -- since no revoke on a relation " +
					"narrows it; then grant the role only the relations it needs"
			}
			findings = append(findings, finding)
		}
	}
	return findings
}

// anyEdge and switchEdge choose the memberships reachableRoles follows: every
// usable edge, or only the ones a member can SET ROLE through.
func anyEdge(ReachMembership) bool { return true }

func switchEdge(membership ReachMembership) bool { return !membership.CannotSwitch }

// reachableRoles is every role subject can use through the memberships follow
// accepts, directly or through another role, sorted and without subject itself.
func reachableRoles(subject string, memberships []ReachMembership, follow func(ReachMembership) bool) []string {
	seen := map[string]bool{subject: true}
	queue := []string{subject}
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
	delete(seen, subject)
	return sortedKeys(seen)
}

// reachedRelations is the set of counted relations the holders read and the
// set they write, and the sources of access to every relation among them.
// superusers marks the holders whose superuser bit counts.
func reachedRelations(
	reach *Reach, index relationIndex, holders, superusers map[string]bool,
) (read, write map[ReachRelation]bool, sources []string) {
	read, write = make(map[ReachRelation]bool), make(map[ReachRelation]bool)
	everything := func(set map[ReachRelation]bool) {
		for _, relation := range reach.Relations {
			set[relation] = true
		}
	}
	sourceSet := make(map[string]bool)
	for holder := range holders {
		if superusers[holder] {
			everything(read)
			everything(write)
			sourceSet["superuser held by "+holder] = true
		}
	}
	for _, access := range reach.WideAccess {
		if !holders[access.Role] {
			continue
		}
		if access.Read {
			everything(read)
		}
		if access.Write {
			everything(write)
		}
		sourceSet[access.Source+" held by "+access.Role] = true
	}
	for _, grant := range reach.Grants {
		held := holders[grant.Role] || strings.EqualFold(grant.Role, publicRole) && holders[publicRole]
		if !held {
			continue
		}
		granted := index.bySchema[grant.Relation.Schema]
		if !grant.OnSchema {
			granted = nil
			if index.counted[grant.Relation] {
				granted = []ReachRelation{grant.Relation}
			}
		}
		for _, relation := range granted {
			if grant.Read {
				read[relation] = true
			}
			if grant.Write {
				write[relation] = true
			}
		}
	}
	return read, write, sortedKeys(sourceSet)
}

// relationIndex finds the counted relations a grant reaches without a pass
// over every relation for every grant.
type relationIndex struct {
	counted  map[ReachRelation]bool
	bySchema map[string][]ReachRelation
}

func indexRelations(relations []ReachRelation) relationIndex {
	index := relationIndex{counted: make(map[ReachRelation]bool, len(relations)), bySchema: make(map[string][]ReachRelation)}
	for _, relation := range relations {
		index.counted[relation] = true
		index.bySchema[relation.Schema] = append(index.bySchema[relation.Schema], relation)
	}
	return index
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
			" relations (" + strconv.Itoa(reached*100/total) + "%), above the " + level + " threshold of " +
			strconv.Itoa(threshold) + "%",
		Detail: Detail{Reach: &ReachDetail{Reached: reached, Relations: total, ThresholdPercent: threshold}},
		Suggestion: "grant the role only the relations it needs, or split it into narrower roles; " +
			"detail.reach.through names the roles it reaches them through",
	}, true
}
