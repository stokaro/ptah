package goschema

// White-box testing required: deduplicateGrants is package-local and the fact
// under test is its identity key, which nothing outside the package can see. A
// test driven through Merge would only observe that two grants survived, which
// the old joined key also produced for every pair whose components happened not
// to carry the separators -- that is, for every pair anyone would think to
// write.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// Two grants on different objects whose delimiter-joined keys are the same
// string. Under `role|privs|t:table|s:schema|q:sequence|o:bool` both render as
//
//	app|SELECT|t:a|s:b|s:|q:|o:false
//
// so the second was dropped and the schema lost a grant without a word. The
// names are deliberately hostile rather than realistic: the point of
// stokaro/ptah#1345 is that identity must not rest on characters being absent
// from names nothing rejects them from. Grant.Canonicalize trims and
// upper-cases; it does not refuse a "|" or a ":".
func TestDeduplicateGrantsKeepsGrantsWhoseJoinedKeysCollide(t *testing.T) {
	c := qt.New(t)
	grants := []Grant{
		{Role: "app", Privileges: []string{"SELECT"}, OnTable: "a|s:b"},
		{Role: "app", Privileges: []string{"SELECT"}, OnTable: "a", OnSchema: "b|s:"},
	}

	deduplicated := deduplicateGrants(grants)

	c.Assert(deduplicated, qt.HasLen, 2)
	c.Assert(deduplicated[0].OnTable, qt.Equals, "a|s:b")
	c.Assert(deduplicated[1].OnSchema, qt.Equals, "b|s:")
}

// The control in the other direction: the deduplication this function exists
// for still happens, so the fix is not "stop deduplicating".
func TestDeduplicateGrantsStillCollapsesTheSameGrantTwice(t *testing.T) {
	c := qt.New(t)
	grants := []Grant{
		{Role: "app", Privileges: []string{"SELECT", "INSERT"}, OnTable: "users"},
		{Role: "app", Privileges: []string{"INSERT", "SELECT"}, OnTable: "users"},
	}

	deduplicated := deduplicateGrants(grants)

	c.Assert(deduplicated, qt.HasLen, 1)
	c.Assert(deduplicated[0].Privileges, qt.DeepEquals, []string{"SELECT", "INSERT"})
}

// The grant option is part of identity, and a struct key has to keep saying so.
func TestDeduplicateGrantsKeepsTheGrantOptionApart(t *testing.T) {
	c := qt.New(t)
	grants := []Grant{
		{Role: "app", Privileges: []string{"SELECT"}, OnTable: "users"},
		{Role: "app", Privileges: []string{"SELECT"}, OnTable: "users", WithOption: true},
	}

	deduplicated := deduplicateGrants(grants)

	c.Assert(deduplicated, qt.HasLen, 2)
	c.Assert(deduplicated[0].WithOption, qt.IsFalse)
	c.Assert(deduplicated[1].WithOption, qt.IsTrue)
}
