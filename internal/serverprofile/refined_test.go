package serverprofile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/serverprofile"
)

// TestProfileRefinedRecordsWhatTheServerDecided pins the three answers Refined
// can give.
//
// A version alone does not decide every key. MySQL 8.4 reads its foreign-key
// reference policy from restrict_fk_on_non_standard_key, and two servers on
// that release answer opposite ways about the two reference keys depending on
// how they were started. Measured on both: with the setting at its default the
// release line is right, and with it off the release line is wrong on both keys
// -- it tells the operator a reference must be unique when it need not be, and
// that it need not be indexed when it must (stokaro/ptah#1230).
//
// The unchanged row is the control, and it carries the cost: a server whose
// configuration is ordinary has to render exactly what it rendered before, or
// this would add a section to every profile ever printed.
func TestProfileRefinedRecordsWhatTheServerDecided(t *testing.T) {
	preset := serverprofile.Profile{
		Capabilities: []serverprofile.Capability{
			{Key: "foreign_keys_require_indexed_reference", Supported: false},
			{Key: "foreign_keys_require_unique_reference", Supported: true},
			{Key: "views", Supported: true},
		},
	}

	tests := []struct {
		name string
		// effective is what a pinned session resolved.
		effective capability.Capabilities
		reason    string
		// wantValues is every key's value after refinement, in order.
		wantValues []bool
		// wantRefined names the keys recorded as refined.
		wantRefined []string
	}{
		{
			name: "a server the release line describes changes nothing",
			effective: capability.Capabilities{
				"foreign_keys_require_indexed_reference": false,
				"foreign_keys_require_unique_reference":  true,
				"views":                                  true,
			},
			reason:      "read from this server's session settings",
			wantValues:  []bool{false, true, true},
			wantRefined: make([]string, 0),
		},
		{
			name: "a server configured the other way answers for itself",
			effective: capability.Capabilities{
				"foreign_keys_require_indexed_reference": true,
				"foreign_keys_require_unique_reference":  false,
				"views":                                  true,
			},
			reason:      "read from this server's session settings",
			wantValues:  []bool{true, false, true},
			wantRefined: []string{"foreign_keys_require_indexed_reference", "foreign_keys_require_unique_reference"},
		},
		{
			name:        "nothing measured leaves the release line's answer alone",
			effective:   nil,
			reason:      "read from this server's session settings",
			wantValues:  []bool{false, true, true},
			wantRefined: make([]string, 0),
		},
		{
			// A refinement a reader cannot go and check is a claim, and this
			// verb exists to replace claims with what a server said.
			name: "a refinement with no reason is not recorded",
			effective: capability.Capabilities{
				"foreign_keys_require_unique_reference": false,
			},
			reason:      "  ",
			wantValues:  []bool{false, true, true},
			wantRefined: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := preset.Refined(test.effective, test.reason)

			values := make([]bool, 0, len(got.Capabilities))
			for _, row := range got.Capabilities {
				values = append(values, row.Supported)
			}
			c.Assert(values, qt.DeepEquals, test.wantValues)

			refined := make([]string, 0, len(got.Refinements))
			for _, row := range got.Refinements {
				refined = append(refined, row.Key)
				c.Assert(row.Reason, qt.Equals, test.reason)
				c.Assert(row.Preset, qt.Not(qt.Equals), row.Effective)
			}
			c.Assert(refined, qt.DeepEquals, test.wantRefined)
		})
	}
}

// TestProfileRefined_TraitsFollowTheKeysTheyAreReadFrom pins the second half of
// a refinement: the non-boolean values derived from the keys that moved.
//
// foreign_key_reference is not stored, it is read from the three reference
// policy keys, and Refined rewrote the keys without rereading it. Measured on
// the MySQL 8.4 pair, the profile for the server with
// restrict_fk_on_non_standard_key OFF said
//
//	Behavior:
//	  foreign_key_reference  unique
//	Set by this server rather than by its release line:
//	  foreign_keys_require_indexed_reference  supported    (the release line answers unsupported)
//	  foreign_keys_require_unique_reference   unsupported  (the release line answers supported)
//
// -- one output contradicting itself, and the wrong half is the one an operator
// reads first (stokaro/ptah#1230).
func TestProfileRefined_TraitsFollowTheKeysTheyAreReadFrom(t *testing.T) {
	tests := []struct {
		name string
		// effective is what a pinned session resolved.
		effective capability.Capabilities
		// wantReference is the policy the profile must report afterwards.
		wantReference capability.ReferencePolicy
		// wantEnum is the enum trait, which no row here refines and which
		// therefore must survive unchanged.
		wantEnum capability.EnumMode
	}{
		{
			name: "a server the release line describes keeps its policy",
			effective: capability.Capabilities{
				capability.ForeignKeys:                        true,
				capability.ForeignKeysRequireUniqueReference:  true,
				capability.ForeignKeysRequireIndexedReference: false,
				capability.EnumInlineColumn:                   true,
			},
			wantReference: capability.ReferenceUnique,
			wantEnum:      capability.EnumInline,
		},
		{
			name: "a server configured the other way reports the other policy",
			effective: capability.Capabilities{
				capability.ForeignKeys:                        true,
				capability.ForeignKeysRequireUniqueReference:  false,
				capability.ForeignKeysRequireIndexedReference: true,
				capability.EnumInlineColumn:                   true,
			},
			wantReference: capability.ReferenceIndexed,
			wantEnum:      capability.EnumInline,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := mysql84Profile().Refined(test.effective, "read from this server's session settings")

			c.Assert(got.Traits.ForeignKeyReference, qt.Equals, test.wantReference)
			c.Assert(got.Traits.EnumModeling, qt.Equals, test.wantEnum)
			// The identifier limit comes from the resolved dialect and no
			// session setting moves it, so it is the control on rereading too
			// much.
			c.Assert(got.Traits.Identifiers, qt.Equals, mysql84Profile().Traits.Identifiers)
		})
	}
}

// mysql84Profile is the release line's answer for MySQL 8.4 over the keys these
// traits are read from: unique references required, indexed ones not, enums
// inline.
func mysql84Profile() serverprofile.Profile {
	return serverprofile.Profile{
		Traits: capability.Traits{
			Identifiers:         capability.Identifiers("mysql"),
			EnumModeling:        capability.EnumInline,
			ForeignKeyReference: capability.ReferenceUnique,
		},
		Capabilities: []serverprofile.Capability{
			{Key: string(capability.EnumInlineColumn), Supported: true},
			{Key: string(capability.ForeignKeys), Supported: true},
			{Key: string(capability.ForeignKeysRequireIndexedReference), Supported: false},
			{Key: string(capability.ForeignKeysRequireUniqueReference), Supported: true},
		},
	}
}
