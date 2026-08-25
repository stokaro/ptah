// Package rlspolicy holds the single description of how a row-level-security
// policy's FOR clause is folded before two sides of a comparison are matched.
//
// It is its own package because both sides need the same answer and they live
// apart: the canonical state model in internal/schemastate and the field-level
// comparison in migration/schemadiff. When they disagreed, an apply planned a
// change it had just made (stokaro/ptah#2211).
package rlspolicy

import "strings"

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
