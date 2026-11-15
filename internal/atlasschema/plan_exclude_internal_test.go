package atlasschema

// White-box testing required: excludeCurrentWithBindings is reached only
// through a saved plan checked against a live connection, and the scope it
// counts a pattern's parts in comes from that connection's URL. No exported
// entry point takes a read and a URL without connecting.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
)

// excludedPolicy is a policy named with its schema and its table, three parts.
var excludedPolicy = []string{"public.notes.p[type=policy]"}

// policyRead is a PostgreSQL read of public.notes and public.other, each
// holding a policy named p.
func policyRead() *catalog.Database {
	observed := func(table string) schemaext.Object {
		return must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("public", table, "p"), pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandSelect, Composition: pgpolicy.Permissive, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
		}))
	}
	return &catalog.Database{
		Tables:          []catalog.Table{{Schema: "public", Name: "notes"}, {Schema: "public", Name: "other"}},
		FeatureObjects:  must.Must(schemaext.NewObjects(observed("notes"), observed("other"))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
}

// TestExcludeCurrentWithBindings_CountsARealmScope pins that the reread a
// saved plan is checked against counts a pattern's parts the way the plan was
// computed. A URL that names no schema leaves every part to the pattern, so
// the policy named with its schema and its table leaves, and the policy of
// the same name on the other table stays.
func TestExcludeCurrentWithBindings_CountsARealmScope(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://localhost/app"}

	current, err := excludeCurrentWithBindings(t.Context(), policyRead(), excludedPolicy, info, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(current.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgpolicy.PolicyRef("public", "other", "p")})
}

// TestExcludeCurrentWithBindings_CountsASchemaBoundScope_FailurePath is the
// control: on a URL bound to one schema the schema is the pattern's first
// part, so the same pattern has one part too many.
func TestExcludeCurrentWithBindings_CountsASchemaBoundScope_FailurePath(t *testing.T) {
	c := qt.New(t)
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://localhost/app?search_path=public"}

	current, err := excludeCurrentWithBindings(t.Context(), policyRead(), excludedPolicy, info, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `(?s).*too many parts in pattern "public\.notes\.p\[type=policy\]".*`)
	c.Assert(current, qt.IsNil)
}
