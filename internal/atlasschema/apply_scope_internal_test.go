package atlasschema

// White-box testing required: scopeApplyStates performs a pair-wide second
// projection after it learns that either comparison side selected a
// non-extension resource. The counterexample needs independently shaped
// current and desired states and does not cross a process boundary.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlasfilter"
	"ptah.run/migration/schemadiff"
)

func TestScopeApplyStatesDoesNotReAddCurrentSupportExtension(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		FeatureObjects:  installedExtensions(installedExtension("citext", "extensions", ""), installedExtension("pgcrypto", "", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		FeatureObjects:  declaredExtensions(declaredExtension("citext", "extensions"), declaredExtension("pgcrypto", "")),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	scope := atlasfilter.Scope{
		Selections:    ownerSelections(),
		Include:       []string{"app.users", "extensions.citext"},
		DefaultSchema: "public",
	}

	got := scopeApplyStates(current, desired, scope)

	c.Assert(got.currentErr, qt.IsNil)
	c.Assert(got.desiredErr, qt.IsNil)
	c.Assert(pgextension.Names(got.current.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto"})
	c.Assert(pgextension.Names(got.desired.FeatureObjects), qt.DeepEquals, []string{"citext", "pgcrypto"})
	applyExtensionSupportCoverage(
		got.desired,
		got.currentReports.Selection,
		got.desiredReports.Selection,
	)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), got.desired, got.current, platform.Postgres, must.Must(builtin.New())))
	added, removed := extensionChanges(diff)
	c.Assert(added, qt.HasLen, 0)
	c.Assert(removed, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}
