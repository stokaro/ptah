// Package unreadroles writes the note a read command prints when its read
// holds principals whose privileges it did not read in full, or found that
// every account holds roles whose memberships it does not state, such as
// MySQL mandatory roles it could not read. The read keeps both in
// [ptah.run/catalog.Database.UnreadRoles] and
// [ptah.run/catalog.Database.EveryAccountHoldsUnread], with the cause. A role
// or login that holds such a principal holds privileges no document made from
// the read states, so without the note the output reads as the whole of its
// access.
package unreadroles

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/catalog"
)

// publicGrantee is the member a read names for every account.
const publicGrantee = "PUBLIC"

// Report writes one note for the roles every account holds and the read does
// not state, and one naming each principal the read did not read in full,
// with the roles and logins that hold it directly and the cause. It writes nothing where
// the read states everything. One principal fits on the note's line; several
// follow it, one per line.
//
// w may be nil, which is how the read surfaces spell "no diagnostics
// stream". Write errors are dropped: a diagnostic that fails to print must
// not fail a read that succeeded.
func Report(w io.Writer, schema *catalog.Database) {
	if w == nil || schema == nil {
		return
	}
	if unread := schema.EveryAccountHoldsUnread; unread != nil {
		held := "roles"
		if len(unread.Roles) > 0 {
			held = "the roles " + strings.Join(unread.Roles, ", ")
		}
		_, _ = fmt.Fprintf(w, "note: every account holds %s whose memberships this read does not state: %s\n", held, unread.Cause)
	}
	if len(schema.UnreadRoles) == 0 {
		return
	}
	entries := make([]string, 0, len(schema.UnreadRoles))
	for _, role := range schema.UnreadRoles {
		entries = append(entries, role.Name+", held by "+holdersOf(schema, role.Name)+": "+role.Cause)
	}
	if len(entries) == 1 {
		_, _ = fmt.Fprintf(w, "note: 1 principal a role or login holds whose privileges this read did not read in full: %s\n", entries[0])
		return
	}
	_, _ = fmt.Fprintf(w, "note: %d principals roles or logins hold whose privileges this read did not read in full:\n  %s\n",
		len(entries), strings.Join(entries, "\n  "))
}

// holdersOf names the members schema records holding name directly, sorted,
// with PUBLIC as every account where it is not a role of that name, and every
// account for a role [catalog.EveryAccountUnread.Roles] names.
func holdersOf(schema *catalog.Database, name string) string {
	var members []string
	if every := schema.EveryAccountHoldsUnread; every != nil && slices.Contains(every.Roles, name) {
		members = append(members, "every account")
	}
	for _, membership := range schema.RoleMemberships {
		if membership.Role != name {
			continue
		}
		member := membership.Member
		if member == publicGrantee && !schema.PublicIsRole {
			member = "every account"
		}
		members = append(members, member)
	}
	slices.Sort(members)
	return strings.Join(slices.Compact(members), ", ")
}
