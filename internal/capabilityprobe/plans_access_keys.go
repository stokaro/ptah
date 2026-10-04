package capabilityprobe

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withAccessKeys declares, on every engine but YDB, the access keys whose
// answer is a property of Ptah's planner: whether it plans a membership of one
// role in another, a group as a principal of its own kind, a privilege on the
// database itself, and a grant path resolved against a YDB database root. The
// YDB plan asks each of them of the server through ydbAccessControl.
//
// They are declared rather than asked. PostgreSQL, MySQL and ClickHouse grant
// a role to a role, and PostgreSQL grants on a database, so a server accepting
// the statement would say nothing about whether this dialect's planner plans
// it; no planner but YDB's does. A group is a role that cannot log in on every
// one of them, and a grant path is YDB's alone.
func withAccessKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return p
	}
	if p.undecided == nil {
		p.undecided = make(map[capability.Capability]string)
	}
	p.undecided[capability.RoleMembership] = "the key names whether Ptah's planner plans a declared membership of " +
		"one role in another, which only the YDB planner does; this server granting a role to a role says " +
		"nothing about what this dialect's planner plans"
	p.undecided[capability.GroupPrincipals] = "the key names a principal kind of its own, made by CREATE GROUP, " +
		"which is YDB's; a role that cannot log in is this engine's group, and no statement here could make one " +
		"Ptah would plan differently"
	p.undecided[capability.DatabaseGrants] = "the key names whether Ptah's planner plans a privilege on the " +
		"database itself, which only the YDB planner does; this server accepting such a grant says nothing about " +
		"what this dialect's planner plans"
	p.undecided[capability.RelativeGrantPaths] = "the key names how a YDB line resolves the path a GRANT names " +
		"against the database root; this engine names a grant's object by schema and name, not by a path"
	return p
}
