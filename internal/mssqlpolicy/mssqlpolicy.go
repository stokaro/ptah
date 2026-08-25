// Package mssqlpolicy holds the single description of which FOR clauses a SQL
// Server SECURITY POLICY can carry.
//
// The planner and the renderer both need this answer and must not disagree.
// The renderer needs it to refuse a declaration it cannot express; the planner
// needs the same answer earlier, because a replacement it plans as a drop
// followed by a create would otherwise drop a policy whose create the renderer
// then refuses -- leaving the table with no row-level security at all, which is
// worse than the difference the replacement was planned to close
// (stokaro/ptah#2211).
//
// It lives outside both packages because the renderer already imports the
// planner, so the planner cannot import the renderer back.
package mssqlpolicy

import "strings"

// UnrenderableFor names why a declared FOR clause has no T-SQL form, or returns
// the empty string when the clause is expressible.
//
// The FOR clause reaches a rendered statement through one route only: the
// operation a BLOCK predicate carries. A filter predicate has no per-operation
// form -- SQL Server's grammar is `ADD FILTER PREDICATE <fn> ON <table>` with
// no slot for an operation, and the catalog reports such a predicate's
// operation as NULL. So a declaration naming an operation with no block
// predicate to attach it to could only be rendered by dropping the clause, and
// a dropped FOR clause is a policy covering every operation rather than the one
// it named.
//
// That is the same silent widening the TO clause is already refused for: SQL
// Server has no role list on a predicate, and honoring `TO app_user` would mean
// dropping it.
func UnrenderableFor(policyFor string, hasBlockPredicate bool) string {
	switch strings.ToUpper(strings.TrimSpace(policyFor)) {
	case "", "ALL":
		return ""
	case "INSERT", "UPDATE", "DELETE":
		if hasBlockPredicate {
			return ""
		}
		return "which only a block predicate can carry; this policy declares no WITH CHECK " +
			"expression, and a filter predicate has no per-operation form, so the clause could " +
			"only be dropped and the policy would cover every operation."
	default:
		return "which a security policy has no form for; a filter predicate applies to every " +
			"read and a block predicate fires on INSERT, UPDATE or DELETE."
	}
}
