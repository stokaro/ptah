package goschema_test

import (
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
)

func TestRLSPolicyGenerationInventarioReproduction(t *testing.T) {
	c := qt.New(t)

	database, err := goschema.ParseDir("../../integration/internal/fixtures/entities/017-rls-inventario-reproduction")
	c.Assert(err, qt.IsNil)

	// One policy and one enablement in each file reach the row-security owner.
	c.Assert(slices.Sorted(maps.Keys(ownerPolicies(c, database))), qt.DeepEquals, []string{
		"public.areas.area_tenant_isolation",
		"public.commodities.commodity_tenant_isolation",
		"public.files.file_tenant_isolation",
		"public.locations.location_tenant_isolation",
		"public.users.user_tenant_isolation",
	})
	c.Assert(slices.Sorted(maps.Keys(ownerSwitches(c, database))), qt.DeepEquals, []string{"areas", "commodities", "files", "locations", "users"})
	c.Assert(database.RLSPolicies, qt.HasLen, 0)
	c.Assert(database.RLSEnabledTables, qt.HasLen, 0)
}
