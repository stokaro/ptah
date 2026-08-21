// Package matviewrefresh holds Ptah's one answer to a declared
// materialized-view refresh strategy: it refuses the declaration, and says why.
//
// Ptah does not refresh materialized views as part of schema reconciliation,
// and the reason is a property of the object rather than of any target. A
// materialized view is populated when it is created, and a changed definition
// is reconciled as DROP and CREATE, which populates it again -- so nothing in
// the schema lifecycle leaves one unpopulated. It goes stale when its SOURCE
// DATA changes, and that is not a schema difference: a schema tool cannot
// observe it, and inferring it from unrelated schema changes would put a
// potentially long data operation into a migration nobody asked to run one in.
//
// CONCURRENTLY follows from that. It is an option of an explicit refresh
// operation, not persistent schema state, so there is no refresh for it to
// modify. The attribute was accepted for one value -- "manual" -- which every
// renderer then ignored, and a declaration that reaches no statement is state
// Ptah cannot reconcile (stokaro/ptah#1625).
//
// ClickHouse's REFRESH EVERY|AFTER is a different thing wearing the same word:
// it is engine-native DDL that the server itself schedules, and it belongs in a
// ClickHouse-specific declaration rather than in this shared abstraction.
//
// The package carries no strategy value and no dialect logic, which is the
// point: there is nothing left to canonicalize, compare or plan.
package matviewrefresh

import (
	"fmt"

	"go.5x5.cz/ptah/core/ptaherr"
)

// Attribute is the retired attribute's only spelling, shared by every
// declaration format that could express it.
const Attribute = "refresh_strategy"

// Reason is the whole explanation. Every surface that refuses the attribute
// quotes this string; there is deliberately no second wording, because two
// wordings drift and the one an operator meets depends on which format they
// happened to write.
const Reason = "Ptah does not refresh materialized views: one is populated when it is created, " +
	"a changed definition is reconciled as DROP and CREATE, and it goes stale only when its " +
	"source data changes, which schema reconciliation cannot observe. Remove the attribute and " +
	"refresh from your own scheduler"

// Refuse reports that a declaration named the retired attribute on object.
//
// The message names no dialect. The refusal is not a capability judgment --
// every target answers the same way, because the reason is about what a schema
// can state rather than about what a server can do.
func Refuse(object string) error {
	return fmt.Errorf("%w: materialized view %q declares %s: %s",
		ptaherr.ErrRetiredAttribute, object, Attribute, Reason)
}
