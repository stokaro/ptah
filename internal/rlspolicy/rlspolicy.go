// Package rlspolicy holds the single description of how a row-level-security
// policy's FOR clause is folded before two sides of a comparison are matched.
//
// It is its own package because everything that folds a FOR clause has to reach
// one answer rather than each carrying its own. When two of them disagreed, an
// apply planned a change it had just made (stokaro/ptah#2211). One of the two
// was the canonical state model removed by ADR 0012; the rule outlived it,
// which is the argument for it living here rather than beside a caller.
package rlspolicy

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
)

// Command folds a declared or observed FOR clause onto one spelling.
//
// A policy with no FOR clause covers every command. That is PostgreSQL's
// documented default for CREATE POLICY; it is what a SQL Server filter
// predicate does, whose grammar has no per-operation form and whose operation
// the catalog reports as NULL; and it is what ClickHouse's reader already
// reports as ALL.
//
// Without the fold the two sides of one comparison disagreed with each other. A
// SQL Server filter-only policy read back with an empty command while the
// declaration said ALL, and the plan was a DROP SECURITY POLICY and a CREATE
// SECURITY POLICY on every apply, forever, leaving the table with no row-level
// security in between. The mirror image is a declaration that omits FOR against
// a catalog reporting ALL, which is the same disagreement with the operands
// swapped -- and it is the one a live round trip hits, because the declaration
// model's zero value is the empty string.
//
// The fold is for comparison only. It is deliberately not written back into
// either side: the renderers still emit what their node carries, so a
// declaration that omitted FOR keeps rendering without one rather than having
// Ptah's normalization written into the user's DDL.
func Command(policyFor string) string {
	folded := strings.ToUpper(strings.TrimSpace(policyFor))
	if folded == "" {
		return "ALL"
	}
	return folded
}

// Roles folds a declared or observed TO clause onto one spelling, for a
// comparison against the named target.
//
// On the PostgreSQL family a policy with no TO clause applies to PUBLIC, and
// the catalog reports it that way. Measured on PostgreSQL 18.6, CockroachDB
// 26.3.2 and YugabyteDB 2026.1: `CREATE POLICY p ON t USING (true)` stores
// polroles {0}, which is PUBLIC. Every role is a member of PUBLIC, so a list
// naming PUBLIC applies to everyone whatever else it names; PostgreSQL and
// YugabyteDB store `TO PUBLIC, app` as {0} and warn, and CockroachDB keeps both
// entries. The order of the list means nothing, and the reader joins it
// without spaces while a declaration usually writes them. So the fold reads the
// clause as a set: an empty clause or one naming PUBLIC in any case is PUBLIC,
// and otherwise the names are sorted and joined one way.
//
// Without the fold a policy declared without TO was dropped and created again
// on every comparison, and so was one naming two roles
// (stokaro/ptah#3572).
//
// Any other target is returned unchanged. The empty clause does not mean
// PUBLIC everywhere: a ClickHouse row policy with no TO applies to nobody, and
// its reader reports it as empty for that reason.
//
// The fold is for comparison only, like [Command]: a declaration that omitted
// TO keeps rendering without one.
func Roles(dialect, roles string) string {
	if !platform.IsPostgresFamily(dialect) {
		return roles
	}
	var names []string
	for part := range strings.SplitSeq(roles, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if strings.EqualFold(name, publicRole) {
			return publicRole
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return publicRole
	}
	slices.Sort(names)
	return strings.Join(slices.Compact(names), ", ")
}

// publicRole is the pseudo-role every role belongs to.
const publicRole = "PUBLIC"

// AsClause names the AS clause a restrictive flag stands for.
//
// The renderer and the comparison both need the word, and they must agree:
// a diff that reported one spelling while the DDL emitted another would name a
// change the plan does not make. Unlike [Command] this is not a fold -- both
// spellings are exact, and PERMISSIVE is the server's default rather than an
// omitted value that means something else (stokaro/ptah#3121).
func AsClause(restrictive bool) string {
	return asClauses[restrictive]
}

// asClauses names both spellings, keyed by the flag that selects one.
//
// The lookup carries the pair rather than a branch so that neither spelling can
// be written at a call site: a literal "RESTRICTIVE" somewhere else is what
// makes the renderer and the comparison drift apart.
var asClauses = map[bool]string{
	false: "PERMISSIVE",
	true:  "RESTRICTIVE",
}
