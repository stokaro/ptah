package mssqldiff

import (
	"fmt"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Assess computes what moving from before to after does to access, from the
// two operands alone. Either may be nil, not both. SQL Server enables at most
// one policy with a predicate on a table, so no sibling policy can widen or
// narrow what this one admits.
//
// A filter predicate only removes rows from a read and a block predicate only
// refuses a write, so a policy that starts enforcing a predicate narrows
// access and one that stops widens it. A disabled policy, or one without
// predicates, enforces nothing. Between two enforcing policies:
//
//   - a slot that loses its predicate widens access, and a slot that gains one
//     narrows it;
//   - a slot whose function or arguments may change has an unknown effect,
//     since what a function admits is not known without running it;
//   - leaving replication agents out widens access for their writes, and
//     taking them back in narrows it;
//   - schema binding decides what may alter the functions, not which rows are
//     admitted, so it changes nothing here.
//
// An unknown effect wins over any other, and a change that can widen access
// reports widens even when it also narrows it.
func Assess(semantics identifier.Semantics, before *mssqlschema.ObservedSecurityPolicy, after *mssqlschema.DesiredSecurityPolicy) schemaext.AccessEffect {
	was, will := enforces(before), declaresEnforcement(after)
	switch {
	case !was && !will:
		return schemaext.AccessEffect{Access: schemaext.AccessUnchanged,
			Reason: "neither side enforces a predicate: a disabled policy, or one without predicates, filters and blocks nothing"}
	case !was:
		return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "the policy starts filtering reads or blocking writes on its tables"}
	case !will:
		return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "the policy stops filtering reads and blocking writes on its tables"}
	}
	var unknown, widens, narrows string
	for _, old := range before.Predicates {
		replacement, found := find(semantics, after.Predicates, old)
		switch {
		case !found:
			widens = first(widens, fmt.Sprintf("the %s predicate on %s is removed", describe(old), old.Table))
		case mssqlschema.CompareInvocation(semantics, replacement, old) != mssqlschema.Agree:
			unknown = first(unknown, fmt.Sprintf("the %s predicate on %s invokes another function or other arguments, "+
				"and what a function admits is not known without running it", describe(old), old.Table))
		}
	}
	for _, added := range after.Predicates {
		if _, found := find(semantics, before.Predicates, added); !found {
			narrows = first(narrows, fmt.Sprintf("a %s predicate on %s is added", describe(added), added.Table))
		}
	}
	_, _, leavesReplication := after.Resolved()
	switch {
	case leavesReplication && !before.NotForReplication:
		widens = first(widens, "writes by replication agents stop being checked")
	case !leavesReplication && before.NotForReplication:
		narrows = first(narrows, "writes by replication agents start being checked")
	}
	switch {
	case unknown != "":
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: unknown}
	case widens != "":
		return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: widens}
	case narrows != "":
		return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: narrows}
	}
	return schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "every predicate, the state and the replication behavior stay as they are"}
}

func enforces(policy *mssqlschema.ObservedSecurityPolicy) bool {
	return policy != nil && policy.Enabled && len(policy.Predicates) > 0
}

func declaresEnforcement(policy *mssqlschema.DesiredSecurityPolicy) bool {
	if policy == nil || len(policy.Predicates) == 0 {
		return false
	}
	enabled, _, _ := policy.Resolved()
	return enabled
}

func find(semantics identifier.Semantics, predicates []mssqlschema.Predicate, want mssqlschema.Predicate) (mssqlschema.Predicate, bool) {
	for _, predicate := range predicates {
		if mssqlschema.SameSlot(semantics, predicate, want) {
			return predicate, true
		}
	}
	return mssqlschema.Predicate{}, false
}

func first(current, candidate string) string {
	if current != "" {
		return current
	}
	return candidate
}

// describe names a predicate's slot for a reason: FILTER, or BLOCK with its
// operation or for every write.
func describe(predicate mssqlschema.Predicate) string {
	switch {
	case predicate.Type == mssqlschema.Filter:
		return "FILTER"
	case predicate.Operation == "":
		return "BLOCK"
	default:
		return "BLOCK " + string(predicate.Operation)
	}
}
