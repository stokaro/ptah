package atlasschema

// White-box testing required: scopeApplyStates performs a pair-wide second
// projection after it learns that either comparison side selected a
// non-extension resource. The counterexample needs independently shaped
// current and desired states and does not cross a process boundary.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestScopeApplyStatesDoesNotReAddCurrentSupportExtension(t *testing.T) {
	c := qt.New(t)
	current := &types.DBSchema{Extensions: []types.DBExtension{
		{Schema: "extensions", Name: "citext"},
		{Name: "pgcrypto"},
	}}
	desired := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Extensions: []goschema.Extension{
			{Schema: "extensions", Name: "citext"},
			{Name: "pgcrypto"},
		},
	}
	scope := atlasfilter.Scope{
		Include:       []string{"app.users", "extensions.citext"},
		DefaultSchema: "public",
	}

	got := scopeApplyStates(current, desired, scope)

	c.Assert(got.currentErr, qt.IsNil)
	c.Assert(got.desiredErr, qt.IsNil)
	c.Assert(got.current.Extensions, qt.DeepEquals, current.Extensions)
	c.Assert(got.desired.Extensions, qt.DeepEquals, desired.Extensions)
	applyExtensionSupportCoverage(
		got.desired,
		got.currentReports.Selection,
		got.desiredReports.Selection,
	)
	diff := schemadiff.CompareWithDialect(got.desired, got.current, platform.Postgres)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}
